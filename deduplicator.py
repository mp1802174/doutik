"""
视频去重处理引擎
借鉴 video-mover 策略，纯 ffmpeg 实现，支持：
  - 水平镜像
  - 轻微旋转
  - 边缘裁剪
  - 颜色微调（饱和度/亮度/对比度）
  - 淡入淡出
  - 边缘模糊条带（上/下/左右）
  - 水印文字叠加
  - HZH 画中画叠加
"""
import hashlib
import random
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger("deduplicator")


class VideoDeduplicator:
    def __init__(
        self,
        output_dir: str,
        enable_dedup: bool = True,
        flip_horizontal: bool = True,
        rotation_angle: float = 0.0,
        crop_percentage: float = 0.02,
        saturation: float = 1.05,
        brightness: float = 0.03,
        contrast: float = 1.05,
        fade_in_frames: int = 5,
        fade_out_frames: int = 15,
        # 边缘模糊（占原视频高/宽的百分比，整数 0-100）
        top_blur_pct: int = 0,
        bottom_blur_pct: int = 0,
        side_blur_pct: int = 0,
        # 画中画（HZH）
        include_hzh: bool = False,
        hzh_opacity: float = 0.1,
        hzh_scale: float = 0.3,
        hzh_video_file: str = "",
        # 水印文字
        include_watermark: bool = False,
        watermark_text: str = "",
        # 音频替换
        replace_audio: bool = True,
        bgm_dir: str = "./assets/bgm",
        random_bgm: bool = True,
        bgm_volume: float = 0.15,
        # 编码参数
        ffmpeg_preset: str = "ultrafast",
        ffmpeg_crf: int = 28,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.enable_dedup = enable_dedup
        self.flip_horizontal = flip_horizontal
        self.rotation_angle = rotation_angle
        self.crop_percentage = crop_percentage
        self.saturation = saturation
        self.brightness = brightness
        self.contrast = contrast
        self.fade_in_frames = fade_in_frames
        self.fade_out_frames = fade_out_frames
        self.top_blur_pct = top_blur_pct
        self.bottom_blur_pct = bottom_blur_pct
        self.side_blur_pct = side_blur_pct
        self.include_hzh = include_hzh
        self.hzh_opacity = hzh_opacity
        self.hzh_scale = hzh_scale
        self.hzh_video_file = hzh_video_file
        self.include_watermark = include_watermark
        self.watermark_text = watermark_text
        self.replace_audio = replace_audio
        self.bgm_dir = Path(bgm_dir)
        self.random_bgm = random_bgm
        self.bgm_volume = bgm_volume
        self.ffmpeg_preset = ffmpeg_preset
        self.ffmpeg_crf = ffmpeg_crf

        self._check_ffmpeg()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def deduplicate(self, input_path: str) -> Optional[str]:
        """对单个视频执行去重变换，返回处理后路径；失败返回 None。"""
        input_path = Path(input_path)
        if not input_path.exists():
            logger.error("Input file not found: %s", input_path)
            return None

        output_path = self.output_dir / f"{input_path.stem}_dedup.mp4"

        # 幂等：已处理且非空则直接复用
        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info("Reuse existing dedup: %s", output_path)
            return str(output_path)

        # 两个开关都关闭：不处理，直接返回原文件路径（下载后直接上传）
        if self._is_passthrough():
            logger.info("Dedup and audio replacement disabled, using original file directly: %s", input_path)
            return str(input_path)

        # 判断是否需要 HZH overlay（需要第二输入）。enable_dedup=false 时不做任何视觉处理。
        if self.enable_dedup and self.include_hzh and self.hzh_video_file and Path(self.hzh_video_file).exists():
            return self._run_hzh(input_path, output_path)
        else:
            return self._run_simple(input_path, output_path)

    def cleanup_input(self, input_path: str):
        p = Path(input_path)
        if p.exists():
            p.unlink()
            logger.debug("Cleaned input: %s", p)

    def cleanup_output(self, output_path: str):
        p = Path(output_path)
        if p.exists():
            p.unlink()
            logger.debug("Cleaned output: %s", p)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_passthrough(self) -> bool:
        """是否完全不处理：不开启画面去重，也不替换音频。"""
        return (not self.enable_dedup) and (not self.replace_audio)

    def _build_vf_chain(self) -> list:
        """构建 -vf 滤镜链（不含 HZH overlay）。enable_dedup=false 时不做视觉处理。"""
        if not self.enable_dedup:
            return []
        vf = []

        # 1. 水平镜像
        if self.flip_horizontal:
            vf.append("hflip")

        # 2. 轻微旋转
        if self.rotation_angle != 0:
            rad = self.rotation_angle * 3.141592653589793 / 180.0
            vf.append(f"rotate={rad:.6f}:bilinear=1")

        # 3. 边缘裁剪
        if self.crop_percentage > 0:
            p = self.crop_percentage
            vf.append(
                f"crop=iw*(1-{p*2}):ih*(1-{p*2}):iw*{p}:ih*{p}"
            )

        # 4. 颜色微调
        eq_parts = []
        if self.saturation != 1.0:
            eq_parts.append(f"saturation={self.saturation}")
        if self.brightness != 0.0:
            eq_parts.append(f"brightness={self.brightness}")
        if self.contrast != 1.0:
            eq_parts.append(f"contrast={self.contrast}")
        if eq_parts:
            vf.append(f"eq={':'.join(eq_parts)}")

        # 5. 淡入（按时间）
        if self.fade_in_frames > 0:
            vf.append("fade=t=in:st=0:d=0.2")

        # 6. 边缘模糊条带（上/下/两侧）
        if self.top_blur_pct > 0 or self.bottom_blur_pct > 0 or self.side_blur_pct > 0:
            vf.extend(self._build_blur_filters())

        # 7. 水印文字
        if self.include_watermark and self.watermark_text:
            safe = self.watermark_text.replace("'", "\\'").replace(":", "\\:")
            vf.append(
                f"drawtext=text='{safe}':fontsize=24:fontcolor=white@0.3"
                ":x=w-tw-10:y=h-th-10"
            )

        return vf

    def _build_blur_filters(self) -> list:
        """
        边缘模糊：先对整个画面模糊，再叠回原始画面，仅在边缘区域显示模糊版本。
        用 overlay + crop 实现轻量效果（避免复杂 filter_complex）。
        简化版：直接在画面边缘绘制半透明黑色矩形以降低视频清晰度，
        效果等价于"边缘遮盖"，不消耗大量 CPU。
        """
        filters = []
        opacity = 0.35  # 遮盖透明度，越高遮盖越深

        # 上边缘
        if self.top_blur_pct > 0:
            h_expr = f"ih*{self.top_blur_pct / 100.0:.4f}"
            filters.append(
                f"drawbox=x=0:y=0:w=iw:h={h_expr}:color=black@{opacity}:t=fill"
            )
        # 下边缘
        if self.bottom_blur_pct > 0:
            h_expr = f"ih*{self.bottom_blur_pct / 100.0:.4f}"
            filters.append(
                f"drawbox=x=0:y=ih-({h_expr}):w=iw:h={h_expr}:color=black@{opacity}:t=fill"
            )
        # 两侧
        if self.side_blur_pct > 0:
            w_expr = f"iw*{self.side_blur_pct / 100.0:.4f}"
            filters.append(
                f"drawbox=x=0:y=0:w={w_expr}:h=ih:color=black@{opacity}:t=fill"
            )
            filters.append(
                f"drawbox=x=iw-({w_expr}):y=0:w={w_expr}:h=ih:color=black@{opacity}:t=fill"
            )
        return filters

    def _pick_bgm(self) -> Optional[Path]:
        """从 BGM 目录中随机选择一首音频。"""
        if not self.replace_audio:
            return None
        if not self.bgm_dir.exists() or not self.bgm_dir.is_dir():
            logger.warning("BGM directory not found: %s", self.bgm_dir)
            return None
        candidates = []
        for ext in ("*.mp3", "*.wav", "*.m4a", "*.aac", "*.flac", "*.ogg"):
            candidates.extend(self.bgm_dir.glob(ext))
        candidates = [p for p in candidates if p.is_file() and p.stat().st_size > 0]
        if not candidates:
            logger.warning("No BGM files found in: %s", self.bgm_dir)
            return None
        chosen = random.choice(candidates) if self.random_bgm else sorted(candidates)[0]
        logger.info("Selected BGM: %s", chosen.name)
        return chosen

    def _get_bgm_start_offset(self, bgm_path: Path, clip_duration: float) -> float:
        """为 BGM 选择随机起始时间，保证截取长度足够覆盖视频时长。"""
        bgm_duration = self._get_duration(bgm_path)
        if not bgm_duration or bgm_duration <= clip_duration:
            return 0.0
        max_offset = max(0.0, bgm_duration - clip_duration)
        offset = random.uniform(0.0, max_offset)
        logger.info("BGM start offset: %.3fs / %.3fs", offset, bgm_duration)
        return offset

    def _get_duration(self, input_path: Path) -> Optional[float]:
        """获取媒体时长（秒）。"""
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    str(input_path),
                ],
                capture_output=True,
                text=True,
                timeout=15,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode != 0:
                return None
            return float(result.stdout.strip())
        except Exception as e:
            logger.warning("Failed to probe duration for %s: %s", input_path, e)
            return None

    def _run_simple(self, input_path: Path, output_path: Path) -> Optional[str]:
        """单输入 ffmpeg 命令（无 HZH）。

        enable_dedup 控制是否应用视频滤镜；replace_audio 控制是否替换原音频。
        """
        vf_chain = self._build_vf_chain()
        vf_str = ",".join(vf_chain) if vf_chain else None

        bgm_path = self._pick_bgm()
        duration = self._get_duration(input_path)

        cmd = ["ffmpeg", "-y", "-i", str(input_path)]

        # 替换音频：添加 BGM 第二输入，并随机起点截取
        if bgm_path and duration:
            bgm_offset = self._get_bgm_start_offset(bgm_path, duration)
            cmd.extend(["-ss", f"{bgm_offset:.3f}", "-i", str(bgm_path)])

        # 视频处理：开启去重时走滤镜重编码；关闭去重时直接复制视频流
        if vf_str:
            cmd.extend([
                "-vf", vf_str,
                "-map", "0:v:0",
                "-c:v", "libx264",
                "-preset", self.ffmpeg_preset,
                "-crf", str(self.ffmpeg_crf),
            ])
        else:
            cmd.extend([
                "-map", "0:v:0",
                "-c:v", "copy",
            ])

        # 音频处理：开启 replace_audio 使用 BGM；否则保留原音频
        if bgm_path and duration:
            cmd.extend([
                "-map", "1:a:0",
                "-c:a", "aac",
                "-af", f"volume={self.bgm_volume}",
                "-t", f"{duration:.3f}",
            ])
        else:
            cmd.extend([
                "-map", "0:a?",
                "-c:a", "copy",
            ])

        cmd.extend([
            "-movflags", "+faststart",
            "-threads", "0",
            str(output_path),
        ])
        return self._run_ffmpeg(cmd, input_path, output_path)

    def _run_hzh(self, input_path: Path, output_path: Path) -> Optional[str]:
        """画中画（HZH）overlay：将第二段视频半透明叠加到主视频角落"""
        hzh_file = Path(self.hzh_video_file)
        if not hzh_file.exists():
            logger.warning("HZH file not found: %s, falling back to simple", hzh_file)
            return self._run_simple(input_path, output_path)

        # 主视频滤镜链（先做基础变换）
        base_chain = self._build_vf_chain()
        base_str = ",".join(base_chain) if base_chain else "null"

        # HZH overlay filter_complex：
        # [0:v] 基础变换 [main]；
        # [1:v] 缩放+透明度调整 [pip]；
        # [main][pip] overlay 到右下角
        scale_expr = f"iw*{self.hzh_scale}"
        filter_complex = (
            f"[0:v]{base_str}[main];"
            f"[1:v]scale={scale_expr}:-1,format=rgba,"
            f"colorchannelmixer=aa={self.hzh_opacity}[pip];"
            f"[main][pip]overlay=W-w-10:H-h-10[out]"
        )

        cmd = [
            "ffmpeg", "-y",
            "-i", str(input_path),
            "-stream_loop", "-1",  # 循环第二段视频（以防比主视频短）
            "-i", str(hzh_file),
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-map", "0:a?",       # 保留主视频音频（如有）
            "-c:v", "libx264",
            "-preset", self.ffmpeg_preset,
            "-crf", str(self.ffmpeg_crf),
            "-c:a", "copy",
            "-movflags", "+faststart",
            "-threads", "0",
            "-shortest",          # 以主视频时长为准
            str(output_path),
        ]
        return self._run_ffmpeg(cmd, input_path, output_path)

    def _run_ffmpeg(self, cmd: list, input_path: Path, output_path: Path) -> Optional[str]:
        logger.info("ffmpeg: %s → %s", input_path.name, output_path.name)
        logger.debug("cmd: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode != 0:
                logger.error(
                    "ffmpeg failed (code=%d): %s",
                    result.returncode,
                    result.stderr[-1000:],
                )
                if output_path.exists():
                    output_path.unlink()
                return None

            if not output_path.exists() or output_path.stat().st_size == 0:
                logger.error("ffmpeg produced empty output")
                return None

            h = self._file_hash(output_path)
            logger.info(
                "Dedup OK: %s (size=%d hash=%s...)",
                output_path.name, output_path.stat().st_size, h[:8],
            )
            return str(output_path)

        except subprocess.TimeoutExpired:
            logger.error("ffmpeg timed out (>10min)")
            if output_path.exists():
                output_path.unlink()
            return None
        except Exception as e:
            logger.error("ffmpeg error: %s", e)
            if output_path.exists():
                output_path.unlink()
            return None

    def _check_ffmpeg(self):
        try:
            r = subprocess.run(
                ["ffmpeg", "-version"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0:
                ver = r.stdout.splitlines()[0]
                logger.info("ffmpeg: %s", ver)
            else:
                raise RuntimeError("ffmpeg not available")
        except FileNotFoundError:
            logger.error("ffmpeg not found. Please install: https://ffmpeg.org/download.html")
            raise RuntimeError("ffmpeg not installed")

    @staticmethod
    def _file_hash(filepath: Path) -> str:
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
