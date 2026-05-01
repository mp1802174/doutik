import random
import string
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from database import VideoDatabase
from deduplicator import VideoDeduplicator
from downloader import F2Downloader
from uploader import SocialAutoUploader
from utils.logger import setup_logger

logger = setup_logger("pipeline")


@dataclass
class PipelineConfig:
    # pipeline
    direction: str
    batch_size: int
    cleanup_after_upload: bool
    max_workers: int
    schedule_interval_minutes: int

    # downloader
    douyin_user_url: str
    tiktok_user_url: str
    download_dir: str
    f2_config: str
    download_timeout: int
    download_delay: int

    # uploader
    saa_path: str
    account_name: str
    headless: bool
    upload_timeout: int
    upload_delay_min: int
    upload_delay_max: int
    tiktok_tags: list
    douyin_category: str

    # deduplicator
    dedup_dir: str
    enable_dedup: bool
    flip_horizontal: bool
    rotation_angle: float
    crop_percentage: float
    saturation: float
    brightness: float
    contrast: float
    fade_in_frames: int
    fade_out_frames: int
    top_blur_pct: int
    bottom_blur_pct: int
    side_blur_pct: int
    include_hzh: bool
    hzh_opacity: float
    hzh_scale: float
    hzh_video_file: str
    include_watermark: bool
    watermark_text: str
    replace_audio: bool
    bgm_dir: str
    random_bgm: bool
    bgm_volume: float
    ffmpeg_preset: str
    ffmpeg_crf: int

    # database
    retention_days: int


class VideoPipeline:
    """完整流水线：下载 → 去重 → 上传"""

    def __init__(self, config: PipelineConfig):
        self.cfg = config
        self.db = VideoDatabase(db_path="./state.db")
        self.downloader = F2Downloader(
            f2_config_path=config.f2_config,
            download_dir=config.download_dir,
            timeout=config.download_timeout,
            delay=config.download_delay,
        )
        self.deduplicator = VideoDeduplicator(
            output_dir=config.dedup_dir,
            enable_dedup=config.enable_dedup,
            flip_horizontal=config.flip_horizontal,
            rotation_angle=config.rotation_angle,
            crop_percentage=config.crop_percentage,
            saturation=config.saturation,
            brightness=config.brightness,
            contrast=config.contrast,
            fade_in_frames=config.fade_in_frames,
            fade_out_frames=config.fade_out_frames,
            top_blur_pct=config.top_blur_pct,
            bottom_blur_pct=config.bottom_blur_pct,
            side_blur_pct=config.side_blur_pct,
            include_hzh=config.include_hzh,
            hzh_opacity=config.hzh_opacity,
            hzh_scale=config.hzh_scale,
            hzh_video_file=config.hzh_video_file,
            include_watermark=config.include_watermark,
            watermark_text=config.watermark_text,
            replace_audio=config.replace_audio,
            bgm_dir=config.bgm_dir,
            random_bgm=config.random_bgm,
            bgm_volume=config.bgm_volume,
            ffmpeg_preset=config.ffmpeg_preset,
            ffmpeg_crf=config.ffmpeg_crf,
        )
        self.uploader = SocialAutoUploader(
            saa_path=config.saa_path,
            account_name=config.account_name,
            headless=config.headless,
            timeout=config.upload_timeout,
            delay_min=config.upload_delay_min,
            delay_max=config.upload_delay_max,
        )
        if config.direction == "douyin_to_tiktok":
            self.source_platform = "douyin"
            self.target_platform = "tiktok"
            self.user_url = config.douyin_user_url
        elif config.direction == "tiktok_to_douyin":
            self.source_platform = "tiktok"
            self.target_platform = "douyin"
            self.user_url = config.tiktok_user_url
        else:
            raise ValueError(f"Invalid direction: {config.direction}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_once(self) -> dict:
        """执行一个批次的完整流水线。"""
        logger.info("=" * 50)
        logger.info(
            "Pipeline START | direction=%s | batch_size=%d",
            self.cfg.direction, self.cfg.batch_size,
        )
        stats = {"downloaded": 0, "deduped": 0, "uploaded": 0, "failed": 0, "skipped": 0}
        run_id = self.db.start_run(self.cfg.direction, self.cfg.batch_size)
        try:
            self._batch_download(stats)
            self._batch_dedup(stats)
            self._batch_upload(stats)
        except Exception as e:
            logger.exception("Pipeline run failed: %s", e)
            stats["failed"] += 1
        finally:
            self.db.end_run(
                run_id,
                downloaded=stats["downloaded"],
                deduped=stats["deduped"],
                uploaded=stats["uploaded"],
                failed=stats["failed"],
            )
            if self.cfg.retention_days > 0:
                self.db.cleanup_old_records(self.cfg.retention_days)
            logger.info(
                "Pipeline END | dl=%d dedup=%d up=%d fail=%d skip=%d",
                stats["downloaded"], stats["deduped"],
                stats["uploaded"], stats["failed"], stats["skipped"],
            )
            logger.info("=" * 50)
        return stats

    def get_stats(self) -> dict:
        return self.db.get_stats()

    def get_recent_runs(self, limit: int = 10) -> list:
        return self.db.get_recent_runs(limit)

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def _batch_download(self, stats: dict) -> list:
        logger.info("--- Stage 1: Download (max %d) ---", self.cfg.batch_size * 3)
        videos = self.downloader.discover_user_videos(
            platform=self.source_platform,
            user_url=self.user_url,
            max_count=self.cfg.batch_size * 3,
        )
        new_videos = []
        for v in videos:
            vid = v["video_id"]
            if self.db.exists(vid):
                logger.debug("Skip existing: %s", vid)
                stats["skipped"] += 1
                continue
            inserted = self.db.insert_or_ignore(
                video_id=vid,
                source_platform=self.source_platform,
                target_platform=self.target_platform,
                original_url=v.get("original_url", ""),
            )
            if inserted:
                self.db.update_status(vid, "downloaded", local_path=v["file_path"])
                new_videos.append(v)
                logger.info("Queued: %s | %s", vid, v["file_name"])
            if len(new_videos) >= self.cfg.batch_size:
                break

        stats["downloaded"] = len(new_videos)
        logger.info("Download stage: %d new videos", len(new_videos))
        return new_videos

    def _batch_dedup(self, stats: dict):
        logger.info("--- Stage 2: Deduplication ---")
        pending = self.db.get_pending_dedups(limit=self.cfg.batch_size)
        for record in pending:
            if not record.local_path or not Path(record.local_path).exists():
                logger.warning("File missing for dedup: %s", record.video_id)
                self.db.update_status(
                    record.video_id, "failed",
                    error_message="Downloaded file missing",
                )
                stats["failed"] += 1
                continue
            try:
                dedup_path = self.deduplicator.deduplicate(record.local_path)
                if dedup_path:
                    self.db.update_status(record.video_id, "deduped", dedup_path=dedup_path)
                    stats["deduped"] += 1
                    logger.info("Deduped: %s", record.video_id)
                else:
                    # ffmpeg 处理失败，直接用原文件
                    self.db.update_status(
                        record.video_id, "deduped",
                        dedup_path=record.local_path,
                        error_message="Dedup failed, using original",
                    )
                    stats["deduped"] += 1
            except Exception as e:
                logger.exception("Dedup error %s: %s", record.video_id, e)
                self.db.update_status(
                    record.video_id, "failed", error_message=str(e)[:500],
                )
                stats["failed"] += 1
        logger.info("Dedup stage: %d processed", stats["deduped"])

    def _batch_upload(self, stats: dict):
        logger.info("--- Stage 3: Upload ---")
        pending = self.db.get_pending_uploads(limit=self.cfg.batch_size)
        for record in pending:
            video_path = record.dedup_path or record.local_path
            if not video_path or not Path(video_path).exists():
                logger.warning("File missing for upload: %s", record.video_id)
                self.db.update_status(
                    record.video_id, "failed", error_message="Upload file missing",
                )
                stats["failed"] += 1
                continue
            title = self._generate_title(record.video_id, getattr(record, "title", "") or "")
            # 取标签配置
            tags = (
                self.cfg.tiktok_tags if self.target_platform == "tiktok" else []
            )
            try:
                success = self.uploader.upload(
                    platform=self.target_platform,
                    video_path=video_path,
                    title=title,
                    tags=tags if tags else None,
                    publish_type=0,
                )
                if success:
                    self.db.update_status(record.video_id, "uploaded")
                    stats["uploaded"] += 1
                    logger.info("Uploaded: %s → %s", record.video_id, self.target_platform)
                    if self.cfg.cleanup_after_upload:
                        self.deduplicator.cleanup_output(video_path)
                        if record.local_path and record.local_path != video_path:
                            self.deduplicator.cleanup_input(record.local_path)
                else:
                    # 区分两类失败：
                    #   1) 平台永久拒绝（last_rejected=True）→ status=rejected，不再重试
                    #   2) 临时失败 → status=failed，可手动 reset 后重试
                    if getattr(self.uploader, "last_rejected", False):
                        err_msg = (
                            getattr(self.uploader, "last_error_message", "")
                            or "Rejected by platform"
                        )
                        self.db.update_status(
                            record.video_id, "rejected",
                            error_message=("REJECTED: " + err_msg)[:500],
                        )
                        stats["failed"] += 1
                        logger.warning(
                            "Marked as REJECTED (will not retry): %s",
                            record.video_id,
                        )
                    else:
                        err_msg = (
                            getattr(self.uploader, "last_error_message", "")
                            or "Upload returned False"
                        )
                        self.db.update_status(
                            record.video_id, "failed",
                            error_message=err_msg[:500],
                        )
                        stats["failed"] += 1
                delay = random.randint(self.cfg.upload_delay_min, self.cfg.upload_delay_max)
                if delay > 0:
                    logger.info("Upload delay: sleeping %d seconds before next...", delay)
                    time.sleep(delay)
            except Exception as e:
                logger.exception("Upload error %s: %s", record.video_id, e)
                self.db.update_status(
                    record.video_id, "failed", error_message=str(e)[:500],
                )
                stats["failed"] += 1
        logger.info("Upload stage: %d uploaded", stats["uploaded"])

    @staticmethod
    def _generate_title(video_id: str, original_title: str = "") -> str:
        """生成上传标题：固定为'中国街拍美女'。"""
        return "中国街拍美女"


def load_pipeline_config(config_path: str = "config.yaml") -> PipelineConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    p  = raw.get("pipeline", {})
    d  = raw.get("downloader", {})
    de = raw.get("deduplicator", {})
    u  = raw.get("uploader", {})
    db = raw.get("database", {})

    return PipelineConfig(
        # pipeline
        direction=p.get("direction", "douyin_to_tiktok"),
        batch_size=p.get("batch_size", 5),
        cleanup_after_upload=p.get("cleanup_after_upload", True),
        max_workers=p.get("max_workers", 2),
        schedule_interval_minutes=p.get("schedule_interval_minutes", 300),

        # downloader
        douyin_user_url=d.get("douyin_user_url", ""),
        tiktok_user_url=d.get("tiktok_user_url", ""),
        download_dir=d.get("download_dir", "./download"),
        f2_config=d.get("f2_config", "my_apps.yaml"),
        download_timeout=d.get("timeout", 120),
        download_delay=d.get("download_delay", 3),

        # uploader
        saa_path=u.get("social_auto_upload_path", "./social-auto-upload"),
        account_name=u.get("account_name", "default"),
        headless=u.get("headless", True),
        upload_timeout=u.get("timeout", 300),
        upload_delay_min=u.get("upload_delay", [10, 10])[0],
        upload_delay_max=u.get("upload_delay", [10, 10])[-1],
        tiktok_tags=u.get("tiktok_tags", []),
        douyin_category=u.get("douyin_category", ""),

        # deduplicator
        dedup_dir=de.get("output_dir", "./dedup"),
        enable_dedup=de.get("enable_dedup", True),
        flip_horizontal=de.get("flip_horizontal", True),
        rotation_angle=de.get("rotation_angle", 0.0),
        crop_percentage=de.get("crop_percentage", 0.02),
        saturation=de.get("saturation", 1.05),
        brightness=de.get("brightness", 0.03),
        contrast=de.get("contrast", 1.05),
        fade_in_frames=de.get("fade_in_frames", 5),
        fade_out_frames=de.get("fade_out_frames", 15),
        top_blur_pct=de.get("top_blur_percentage", 0),
        bottom_blur_pct=de.get("bottom_blur_percentage", 0),
        side_blur_pct=de.get("side_blur_percentage", 0),
        include_hzh=de.get("include_hzh", False),
        hzh_opacity=de.get("hzh_opacity", 0.1),
        hzh_scale=de.get("hzh_scale", 0.3),
        hzh_video_file=de.get("hzh_video_file", ""),
        include_watermark=de.get("include_watermark", False),
        watermark_text=de.get("watermark_text", ""),
        replace_audio=de.get("replace_audio", True),
        bgm_dir=de.get("bgm_dir", "./assets/bgm"),
        random_bgm=de.get("random_bgm", True),
        bgm_volume=de.get("bgm_volume", 0.15),
        ffmpeg_preset=de.get("ffmpeg_preset", "ultrafast"),
        ffmpeg_crf=de.get("ffmpeg_crf", 28),

        # database
        retention_days=db.get("retention_days", 0),
    )
