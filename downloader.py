"""
F2 下载器封装
基于 f2 (JoeanAmier/TikTokDownloader) 命令行工具

f2 douyin 参数（实测确认）：
  -p, --path TEXT         保存目录（f2 会在其下建 douyin/post/{昵称}/{id}/ 子目录）
  -M, --mode post         主页作品模式
  -o, --max-counts INT    最大下载数（0=无限制）
  -n, --naming TEXT       文件命名（{aweme_id} → 数字 ID）
  -f, --folderize BOOL    是否用子目录（由 my_apps.yaml 控制，默认 yes）

注意：f2 的实际输出路径为 <path>/douyin/post/<昵称>/<id>/<id>_video.mp4
      扫描时需递归搜索。
"""
import re
import subprocess
import time
from pathlib import Path
from typing import List, Optional

from utils.logger import setup_logger

logger = setup_logger("downloader")

# 视频文件扩展名
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".flv", ".webm"}
# f2 默认在文件名末尾加 _video 后缀
VIDEO_STEM_SUFFIX = "_video"

PLATFORM_TO_F2_MODE = {
    "douyin": "douyin",
    "tiktok": "tiktok",
}


class F2Downloader:
    """基于 f2 CLI 的下载器封装"""

    def __init__(
        self,
        f2_config_path: str,
        download_dir: str,
        timeout: int = 120,
        delay: int = 3,
    ):
        self.f2_config_path = Path(f2_config_path)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.delay = delay
        self._check_f2()

    def _check_f2(self):
        try:
            result = subprocess.run(
                ["f2", "--help"],
                capture_output=True, text=True, timeout=5,
            )
            logger.info("f2 detected (returncode=%d)", result.returncode)
        except FileNotFoundError:
            logger.error("f2 not found. Install: pip install f2")
            raise RuntimeError("f2 not installed")

    def discover_user_videos(
        self,
        platform: str,
        user_url: str,
        max_count: int = 50,
    ) -> List[dict]:
        """
        用 f2 下载用户最新作品，然后递归扫描 download_dir 返回视频列表。
        """
        f2_mode = PLATFORM_TO_F2_MODE.get(platform, platform)
        logger.info(
            "Discovering %s videos: %s (max=%d)",
            platform, user_url, max_count,
        )

        if not user_url:
            logger.error("user_url is empty for platform %s", platform)
            return self._scan_downloaded_files()

        if not self.f2_config_path.exists():
            logger.error("f2 config not found: %s", self.f2_config_path)
            return self._scan_downloaded_files()

        cmd = [
            "f2", f2_mode,
            "-c", str(self.f2_config_path),
            "-u", user_url,
            "-p", str(self.download_dir),   # 保存根目录
            "-M", "post",                   # 主页作品
            "-o", str(max_count),           # 最大下载数
            "-n", "{aweme_id}",             # 文件名 = 数字 ID（生成 {id}_video.mp4）
        ]

        logger.info("Running f2: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                encoding="utf-8",
                errors="replace",
            )
            stdout_tail = result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout
            stderr_tail = result.stderr[-500:]  if len(result.stderr) > 500  else result.stderr
            logger.debug("f2 stdout: %s", stdout_tail)
            if result.stderr:
                logger.debug("f2 stderr: %s", stderr_tail)

            if result.returncode == 0:
                logger.info("f2 completed successfully")
            elif result.returncode == 2:
                logger.warning(
                    "f2 exited with code 2（可能是 Cookie 失效或无新视频）: %s",
                    stderr_tail,
                )
            else:
                logger.error(
                    "f2 failed (code=%d): %s",
                    result.returncode, stderr_tail,
                )
        except subprocess.TimeoutExpired:
            logger.warning("f2 timed out after %d seconds", self.timeout)
        except Exception as e:
            logger.error("f2 error: %s", e)

        time.sleep(self.delay)
        return self._scan_downloaded_files()

    def _scan_downloaded_files(self) -> List[dict]:
        """
        递归扫描 download_dir 下所有视频文件（包含 f2 的子目录结构）。
        只返回 *_video.mp4（f2 的默认命名）或普通 .mp4。
        按修改时间倒序排列。
        """
        videos = []

        if not self.download_dir.exists():
            return []

        # 递归查找所有视频文件
        all_files = []
        for ext in VIDEO_EXTS:
            all_files.extend(self.download_dir.rglob(f"*{ext}"))

        for f in sorted(all_files, key=lambda p: p.stat().st_mtime, reverse=True):
            # 跳过非视频文件（如 _music、_cover 误匹配）
            if not f.is_file():
                continue
            video_id = self._extract_video_id(f.name) or f.stem.replace(VIDEO_STEM_SUFFIX, "")
            videos.append(
                {
                    "video_id": video_id,
                    "file_path": str(f),
                    "file_name": f.name,
                    "original_url": "",
                    "size": f.stat().st_size,
                }
            )

        logger.info("Scanned %d video files under %s", len(videos), self.download_dir)
        return videos

    @staticmethod
    def _extract_video_id(filename: str) -> Optional[str]:
        """从文件名提取数字视频 ID（18-20 位数字）"""
        patterns = [
            r"(\d{18,20})",   # 18-20 位数字（抖音/TikTok 标准 ID）
            r"video_(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, filename)
            if m:
                return m.group(1)
        return None

    def download_single(
        self,
        platform: str,
        video_url: str,
        video_id: str,
    ) -> Optional[Path]:
        """下载单个视频（备用方法）"""
        f2_mode = PLATFORM_TO_F2_MODE.get(platform, platform)
        logger.info("Downloading single video: %s", video_url)
        cmd = [
            "f2", f2_mode,
            "-c", str(self.f2_config_path),
            "-u", video_url,
            "-p", str(self.download_dir),
            "-M", "one",
            "-n", "{aweme_id}",
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode != 0:
                logger.error("Single download failed: %s", result.stderr[-500:])
                return None

            time.sleep(self.delay)
            # 递归找该 video_id 对应的文件
            candidates = list(self.download_dir.rglob(f"*{video_id}*_video.mp4"))
            if not candidates:
                candidates = list(self.download_dir.rglob(f"*{video_id}*.mp4"))
            if candidates:
                return max(candidates, key=lambda p: p.stat().st_mtime)
            return None
        except Exception as e:
            logger.error("Single download error: %s", e)
            return None
