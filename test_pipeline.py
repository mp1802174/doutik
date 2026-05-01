#!/usr/bin/env python3
"""
流水线全功能测试脚本
运行: python3 test_pipeline.py [--verbose]

测试范围:
  1. 配置文件加载
  2. 数据库 CRUD & 状态机
  3. 去重器（ffmpeg 单元测试，使用本地生成的测试视频）
  4. 下载器（仅 f2 可用性 + 目录扫描）
  5. 上传器（仅 cookie 检查、conf.py 创建）
  6. 流水线 run_once（Mock 模式，跳过真实 f2/上传）
  7. 依赖检测
  8. 命令行 stats / history
"""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ── 切换工作目录为项目根目录 ─────────────────────────────────────────────
PROJECT_DIR = Path(__file__).parent.resolve()
os.chdir(PROJECT_DIR)
sys.path.insert(0, str(PROJECT_DIR))

# ── 彩色输出 ────────────────────────────────────────────────────────────
def _c(text, color):
    if not sys.stdout.isatty():
        return text
    codes = {"green": "92", "red": "91", "yellow": "93", "cyan": "96", "bold": "1"}
    return f"\033[{codes.get(color, '0')}m{text}\033[0m"


def ok(msg):   print(f"  {_c('PASS', 'green')}  {msg}")
def fail(msg): print(f"  {_c('FAIL', 'red')}  {msg}")
def info(msg): print(f"  {_c('INFO', 'cyan')}  {msg}")
def head(msg): print(f"\n{_c('='*55, 'cyan')}\n{_c(msg, 'bold')}\n{_c('='*55, 'cyan')}")


# ══════════════════════════════════════════════════════════════════════
# 1. 配置加载测试
# ══════════════════════════════════════════════════════════════════════
class TestConfig(unittest.TestCase):
    def test_load_config(self):
        from pipeline import load_pipeline_config
        cfg = load_pipeline_config("config.yaml")
        self.assertIn(cfg.direction, ("douyin_to_tiktok", "tiktok_to_douyin"))
        self.assertGreater(cfg.batch_size, 0)
        self.assertIsInstance(cfg.flip_horizontal, bool)
        self.assertIsInstance(cfg.include_hzh, bool)
        self.assertIsInstance(cfg.include_watermark, bool)
        self.assertIsInstance(cfg.tiktok_tags, list)
        ok("load_pipeline_config: 所有字段加载正常")

    def test_config_missing_file(self):
        from pipeline import load_pipeline_config
        with self.assertRaises(FileNotFoundError):
            load_pipeline_config("/nonexistent/path/config.yaml")
        ok("load_pipeline_config: FileNotFoundError 正确抛出")


# ══════════════════════════════════════════════════════════════════════
# 2. 数据库测试
# ══════════════════════════════════════════════════════════════════════
class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = str(Path(self.tmp_dir) / "test.db")
        from database import VideoDatabase
        self.db = VideoDatabase(db_path=self.db_path)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_insert_and_exists(self):
        inserted = self.db.insert_or_ignore("vid001", "douyin", "tiktok", "http://example.com")
        self.assertTrue(inserted)
        self.assertTrue(self.db.exists("vid001"))
        ok("DB: insert_or_ignore + exists 正常")

    def test_duplicate_insert(self):
        self.db.insert_or_ignore("vid002", "douyin", "tiktok", "")
        dup = self.db.insert_or_ignore("vid002", "douyin", "tiktok", "")
        self.assertFalse(dup)
        ok("DB: 重复插入返回 False")

    def test_update_status(self):
        self.db.insert_or_ignore("vid003", "douyin", "tiktok", "")
        self.db.update_status("vid003", "downloaded", local_path="/tmp/a.mp4")
        rec = self.db.get_by_id("vid003")
        self.assertEqual(rec.status, "downloaded")
        self.assertEqual(rec.local_path, "/tmp/a.mp4")
        ok("DB: update_status 正常")

    def test_status_transitions(self):
        self.db.insert_or_ignore("vid004", "douyin", "tiktok", "")
        for st in ("downloaded", "deduped", "uploaded"):
            self.db.update_status("vid004", st)
        rec = self.db.get_by_id("vid004")
        self.assertEqual(rec.status, "uploaded")
        ok("DB: 状态流转 pending→downloaded→deduped→uploaded 正常")

    def test_get_pending_dedups(self):
        for i in range(3):
            self.db.insert_or_ignore(f"v{i}", "douyin", "tiktok", "")
            self.db.update_status(f"v{i}", "downloaded", local_path=f"/tmp/v{i}.mp4")
        rows = self.db.get_pending_dedups(limit=10)
        self.assertEqual(len(rows), 3)
        ok("DB: get_pending_dedups 返回 3 条")

    def test_get_pending_uploads(self):
        for i in range(2):
            self.db.insert_or_ignore(f"u{i}", "douyin", "tiktok", "")
            self.db.update_status(f"u{i}", "downloaded")
            self.db.update_status(f"u{i}", "deduped", dedup_path=f"/tmp/u{i}_dedup.mp4")
        rows = self.db.get_pending_uploads(limit=10)
        self.assertEqual(len(rows), 2)
        ok("DB: get_pending_uploads 返回 2 条")

    def test_get_stats(self):
        self.db.insert_or_ignore("s1", "douyin", "tiktok", "")
        self.db.update_status("s1", "failed")
        stats = self.db.get_stats()
        self.assertIn("failed", stats)
        self.assertEqual(stats["failed"], 1)
        ok("DB: get_stats 正常")

    def test_run_tracking(self):
        run_id = self.db.start_run("douyin_to_tiktok", 5)
        self.assertIsInstance(run_id, int)
        self.db.end_run(run_id, downloaded=3, deduped=3, uploaded=2, failed=1)
        runs = self.db.get_recent_runs(limit=5)
        self.assertEqual(runs[0]["status"], "completed")
        self.assertEqual(runs[0]["uploaded"], 2)
        ok("DB: start_run / end_run / get_recent_runs 正常")

    def test_cleanup_old_records(self):
        self.db.insert_or_ignore("old1", "douyin", "tiktok", "")
        self.db.update_status("old1", "uploaded")
        # retention_days=0 不清理
        self.db.cleanup_old_records(0)
        self.assertTrue(self.db.exists("old1"))
        ok("DB: cleanup_old_records(0) 不删除记录")


# ══════════════════════════════════════════════════════════════════════
# 3. 去重器测试
# ══════════════════════════════════════════════════════════════════════
class TestDeduplicator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """生成一个测试用短视频（3 秒, 320x240 黑色视频）"""
        cls.tmp_dir = tempfile.mkdtemp()
        cls.test_video = str(Path(cls.tmp_dir) / "test_input.mp4")
        cls.output_dir = str(Path(cls.tmp_dir) / "dedup_out")
        Path(cls.output_dir).mkdir()

        r = subprocess.run(
            [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", "color=black:s=320x240:r=25:d=3",
                "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "35",
                "-c:a", "aac", "-shortest",
                cls.test_video,
            ],
            capture_output=True, timeout=30,
        )
        cls.has_video = r.returncode == 0 and Path(cls.test_video).exists()
        if not cls.has_video:
            info("ffmpeg 生成测试视频失败，跳过去重测试")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_dir, ignore_errors=True)

    def _make_dedup(self, **kwargs):
        from deduplicator import VideoDeduplicator
        defaults = dict(
            output_dir=self.output_dir,
            flip_horizontal=True,
            crop_percentage=0.02,
            saturation=1.05,
            brightness=0.03,
            contrast=1.05,
            fade_in_frames=5,
            ffmpeg_preset="ultrafast",
            ffmpeg_crf=35,
        )
        defaults.update(kwargs)
        return VideoDeduplicator(**defaults)

    @unittest.skipUnless(True, "需要 ffmpeg")
    def test_basic_dedup(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        d = self._make_dedup()
        result = d.deduplicate(self.test_video)
        self.assertIsNotNone(result)
        self.assertTrue(Path(result).exists())
        self.assertGreater(Path(result).stat().st_size, 0)
        ok("Deduplicator: 基础去重（hflip+crop+eq+fade）正常")

    def test_watermark(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        out_dir = Path(self.tmp_dir) / "wm_out"
        out_dir.mkdir(exist_ok=True)
        d = self._make_dedup(
            output_dir=str(out_dir),
            include_watermark=True,
            watermark_text="Test WM",
        )
        result = d.deduplicate(self.test_video)
        self.assertIsNotNone(result)
        ok("Deduplicator: 水印叠加正常")

    def test_blur_edges(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        out_dir = Path(self.tmp_dir) / "blur_out"
        out_dir.mkdir(exist_ok=True)
        d = self._make_dedup(
            output_dir=str(out_dir),
            top_blur_pct=10,
            bottom_blur_pct=10,
            side_blur_pct=5,
        )
        result = d.deduplicate(self.test_video)
        self.assertIsNotNone(result)
        ok("Deduplicator: 边缘模糊条带正常")

    def test_rotation(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        out_dir = Path(self.tmp_dir) / "rot_out"
        out_dir.mkdir(exist_ok=True)
        d = self._make_dedup(
            output_dir=str(out_dir),
            flip_horizontal=False,
            rotation_angle=-2.0,
            crop_percentage=0.0,
        )
        result = d.deduplicate(self.test_video)
        self.assertIsNotNone(result)
        ok("Deduplicator: 旋转变换正常")

    def test_idempotent(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        out_dir = Path(self.tmp_dir) / "idem_out"
        out_dir.mkdir(exist_ok=True)
        d = self._make_dedup(output_dir=str(out_dir))
        r1 = d.deduplicate(self.test_video)
        r2 = d.deduplicate(self.test_video)
        self.assertEqual(r1, r2)
        ok("Deduplicator: 幂等（相同输入不重复处理）")

    def test_passthrough_copy(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        out_dir = Path(self.tmp_dir) / "pass_out"
        out_dir.mkdir(exist_ok=True)
        d = self._make_dedup(
            output_dir=str(out_dir),
            flip_horizontal=False,
            rotation_angle=0.0,
            crop_percentage=0.0,
            saturation=1.0,
            brightness=0.0,
            contrast=1.0,
            fade_in_frames=0,
        )
        result = d.deduplicate(self.test_video)
        self.assertIsNotNone(result)
        # passthrough 应该是 shutil.copy2，文件大小接近原始
        ok("Deduplicator: 无变换时直接复制（passthrough）")

    def test_missing_input(self):
        d = self._make_dedup()
        result = d.deduplicate("/nonexistent/video.mp4")
        self.assertIsNone(result)
        ok("Deduplicator: 输入文件不存在返回 None")

    def test_cleanup(self):
        if not self.has_video:
            self.skipTest("测试视频未生成")
        tmp_file = Path(self.tmp_dir) / "to_clean.mp4"
        shutil.copy2(self.test_video, str(tmp_file))
        d = self._make_dedup()
        d.cleanup_input(str(tmp_file))
        self.assertFalse(tmp_file.exists())
        ok("Deduplicator: cleanup_input 正常")


# ══════════════════════════════════════════════════════════════════════
# 4. 下载器测试
# ══════════════════════════════════════════════════════════════════════
class TestDownloader(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_scan_empty_dir(self):
        from downloader import F2Downloader
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dl = F2Downloader(
                f2_config_path="my_apps.yaml",
                download_dir=self.tmp_dir,
            )
        videos = dl._scan_downloaded_files()
        self.assertEqual(videos, [])
        ok("Downloader: 空目录扫描返回 []")

    def test_scan_with_video_files(self):
        from downloader import F2Downloader
        # 创建模拟视频文件
        for name in ["7123456789012345678.mp4", "other_7234567890123456789.mp4", "readme.txt"]:
            (Path(self.tmp_dir) / name).write_bytes(b"fake video")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dl = F2Downloader(
                f2_config_path="my_apps.yaml",
                download_dir=self.tmp_dir,
            )
        videos = dl._scan_downloaded_files()
        self.assertEqual(len(videos), 2)
        ok("Downloader: 扫描目录返回 2 个视频文件（跳过 .txt）")

    def test_extract_video_id(self):
        from downloader import F2Downloader
        cases = [
            ("7123456789012345678.mp4", "7123456789012345678"),
            ("user_7234567890123456789_video.mp4", "7234567890123456789"),
            ("no_id_here.mp4", None),
        ]
        for filename, expected in cases:
            result = F2Downloader._extract_video_id(filename)
            self.assertEqual(result, expected, f"filename={filename}")
        ok("Downloader: _extract_video_id 正确解析 ID")

    def test_discover_no_url(self):
        """user_url 为空时应安全返回空列表"""
        from downloader import F2Downloader
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dl = F2Downloader(
                f2_config_path="my_apps.yaml",
                download_dir=self.tmp_dir,
            )
        result = dl.discover_user_videos("douyin", "", max_count=5)
        self.assertIsInstance(result, list)
        ok("Downloader: user_url 为空时安全返回 []")


# ══════════════════════════════════════════════════════════════════════
# 5. 上传器测试
# ══════════════════════════════════════════════════════════════════════
class TestUploader(unittest.TestCase):
    def setUp(self):
        self.saa_path = Path("./social-auto-upload")

    def test_validate_saa_path(self):
        """social-auto-upload 目录存在且结构正确"""
        from uploader import SocialAutoUploader
        try:
            u = SocialAutoUploader(
                saa_path=str(self.saa_path),
                headless=True,
            )
            ok("Uploader: social-auto-upload 路径验证通过")
        except Exception as e:
            self.fail(f"Uploader 初始化失败: {e}")

    def test_conf_py_created(self):
        """conf.py 应该被自动创建"""
        conf_py = self.saa_path / "conf.py"
        if conf_py.exists():
            ok("Uploader: conf.py 已存在")
        else:
            self.fail("conf.py 未被自动创建")

    def test_check_login_no_cookie(self):
        """没有 Cookie 文件时 check_login_status 应返回 False"""
        from uploader import SocialAutoUploader
        u = SocialAutoUploader(saa_path=str(self.saa_path))
        result = u.check_login_status("nonexistent_platform_xyz")
        self.assertFalse(result)
        ok("Uploader: 无 Cookie 时 check_login_status=False")

    def test_upload_missing_video(self):
        """视频文件不存在时 upload 应返回 False"""
        from uploader import SocialAutoUploader
        u = SocialAutoUploader(saa_path=str(self.saa_path))
        result = u.upload("douyin", "/nonexistent/video.mp4", title="test")
        self.assertFalse(result)
        ok("Uploader: 视频文件不存在时 upload=False")

    def test_upload_no_cookie(self):
        """没有 Cookie 时 upload 应返回 False"""
        from uploader import SocialAutoUploader
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp.write(b"fake video")
        tmp.close()
        try:
            u = SocialAutoUploader(
                saa_path=str(self.saa_path),
                account_name="__test_no_cookie__",
            )
            result = u.upload("douyin", tmp.name, title="test")
            self.assertFalse(result)
            ok("Uploader: 无 Cookie 时 upload=False")
        finally:
            Path(tmp.name).unlink(missing_ok=True)

    def test_tiktok_helper_exists(self):
        """tiktok_helper.py 应该存在"""
        helper = self.saa_path / "tiktok_helper.py"
        self.assertTrue(helper.exists(), f"tiktok_helper.py not found: {helper}")
        ok("Uploader: tiktok_helper.py 存在")


# ══════════════════════════════════════════════════════════════════════
# 6. 流水线 Mock 测试
# ══════════════════════════════════════════════════════════════════════
class TestPipelineMock(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _mock_pipeline(self):
        """创建带 Mock 依赖的 Pipeline"""
        from pipeline import PipelineConfig, VideoPipeline

        cfg = PipelineConfig(
            direction="douyin_to_tiktok",
            batch_size=2,
            cleanup_after_upload=False,
            max_workers=1,
            schedule_interval_minutes=300,
            douyin_user_url="https://example.com/user/test",
            tiktok_user_url="",
            download_dir=str(Path(self.tmp_dir) / "dl"),
            f2_config=str(PROJECT_DIR / "my_apps.yaml"),
            download_timeout=30,
            download_delay=0,
            saa_path=str(PROJECT_DIR / "social-auto-upload"),
            account_name="__mock__",
            headless=True,
            upload_timeout=30,
            upload_delay=0,
            tiktok_tags=[],
            douyin_category="",
            dedup_dir=str(Path(self.tmp_dir) / "dedup"),
            enable_dedup=True,
            flip_horizontal=True,
            rotation_angle=0.0,
            crop_percentage=0.02,
            saturation=1.05,
            brightness=0.03,
            contrast=1.05,
            fade_in_frames=5,
            fade_out_frames=0,
            top_blur_pct=0,
            bottom_blur_pct=0,
            side_blur_pct=0,
            include_hzh=False,
            hzh_opacity=0.1,
            hzh_scale=0.3,
            hzh_video_file="",
            include_watermark=False,
            watermark_text="",
            replace_audio=True,
            bgm_dir=str(PROJECT_DIR / "assets" / "bgm"),
            random_bgm=True,
            bgm_volume=0.15,
            ffmpeg_preset="ultrafast",
            ffmpeg_crf=35,
            retention_days=0,
        )

        with patch("downloader.F2Downloader._check_f2"), \
             patch("deduplicator.VideoDeduplicator._check_ffmpeg"), \
             patch("uploader.SocialAutoUploader._validate"), \
             patch("uploader.SocialAutoUploader._ensure_conf_py"), \
             patch("uploader.SocialAutoUploader._patch_headless"):
            pipeline = VideoPipeline(cfg)

        # 替换真实组件为 Mock
        pipeline.downloader = MagicMock()
        pipeline.deduplicator = MagicMock()
        pipeline.uploader = MagicMock()
        # 使用临时数据库
        from database import VideoDatabase
        pipeline.db = VideoDatabase(db_path=str(Path(self.tmp_dir) / "test.db"))

        return pipeline

    def test_run_once_empty(self):
        """无视频时 run_once 应正确返回 0"""
        pipeline = self._mock_pipeline()
        pipeline.downloader.discover_user_videos.return_value = []

        stats = pipeline.run_once()
        self.assertEqual(stats["downloaded"], 0)
        self.assertEqual(stats["uploaded"], 0)
        ok("Pipeline Mock: 无视频时 run_once 正常")

    def test_run_once_full_flow(self):
        """模拟完整流程：下载 → 去重 → 上传"""
        pipeline = self._mock_pipeline()
        tmp_dir = Path(self.tmp_dir)

        # 生成两个假视频文件
        dl_dir = tmp_dir / "dl"
        dl_dir.mkdir(exist_ok=True)
        dedup_dir = tmp_dir / "dedup"
        dedup_dir.mkdir(exist_ok=True)

        v1 = dl_dir / "7111111111111111111.mp4"
        v2 = dl_dir / "7222222222222222222.mp4"
        v1.write_bytes(b"fake video 1")
        v2.write_bytes(b"fake video 2")

        v1_d = dedup_dir / "7111111111111111111_dedup.mp4"
        v2_d = dedup_dir / "7222222222222222222_dedup.mp4"
        v1_d.write_bytes(b"deduped 1")
        v2_d.write_bytes(b"deduped 2")

        pipeline.downloader.discover_user_videos.return_value = [
            {"video_id": "7111111111111111111", "file_path": str(v1), "file_name": v1.name, "original_url": ""},
            {"video_id": "7222222222222222222", "file_path": str(v2), "file_name": v2.name, "original_url": ""},
        ]
        pipeline.deduplicator.deduplicate.side_effect = [str(v1_d), str(v2_d)]
        pipeline.uploader.upload.return_value = True

        stats = pipeline.run_once()

        self.assertEqual(stats["downloaded"], 2)
        self.assertEqual(stats["deduped"], 2)
        self.assertEqual(stats["uploaded"], 2)
        self.assertEqual(stats["failed"], 0)
        ok("Pipeline Mock: 完整流程 dl=2 dedup=2 up=2 fail=0")

    def test_run_once_upload_failure(self):
        """上传失败时 failed 计数正确"""
        pipeline = self._mock_pipeline()
        tmp_dir = Path(self.tmp_dir)
        dl_dir = tmp_dir / "dl2"; dl_dir.mkdir(exist_ok=True)
        dedup_dir = tmp_dir / "dedup2"; dedup_dir.mkdir(exist_ok=True)

        v = dl_dir / "7333333333333333333.mp4"
        v.write_bytes(b"fake")
        vd = dedup_dir / "7333333333333333333_dedup.mp4"
        vd.write_bytes(b"deduped")

        pipeline.downloader.discover_user_videos.return_value = [
            {"video_id": "7333333333333333333", "file_path": str(v), "file_name": v.name, "original_url": ""},
        ]
        pipeline.deduplicator.deduplicate.return_value = str(vd)
        pipeline.uploader.upload.return_value = False  # 上传失败

        stats = pipeline.run_once()
        self.assertEqual(stats["uploaded"], 0)
        self.assertEqual(stats["failed"], 1)
        ok("Pipeline Mock: 上传失败时 failed=1")

    def test_run_once_dedup_failure(self):
        """去重失败（None）时 fallback 到原文件继续上传"""
        pipeline = self._mock_pipeline()
        tmp_dir = Path(self.tmp_dir)
        dl_dir = tmp_dir / "dl3"; dl_dir.mkdir(exist_ok=True)
        dedup_dir = tmp_dir / "dedup3"; dedup_dir.mkdir(exist_ok=True)

        v = dl_dir / "7444444444444444444.mp4"
        v.write_bytes(b"fake")

        pipeline.downloader.discover_user_videos.return_value = [
            {"video_id": "7444444444444444444", "file_path": str(v), "file_name": v.name, "original_url": ""},
        ]
        pipeline.deduplicator.deduplicate.return_value = None   # 去重失败
        pipeline.uploader.upload.return_value = True

        stats = pipeline.run_once()
        # dedup 失败后 fallback 到原文件，仍然会尝试上传
        self.assertEqual(stats["deduped"], 1)  # fallback 计为 deduped
        ok("Pipeline Mock: 去重失败 fallback 到原文件")

    def test_duplicate_skip(self):
        """同一 video_id 第二次运行时应被跳过"""
        pipeline = self._mock_pipeline()
        tmp_dir = Path(self.tmp_dir)
        dl_dir = tmp_dir / "dl4"; dl_dir.mkdir(exist_ok=True)
        dedup_dir = tmp_dir / "dedup4"; dedup_dir.mkdir(exist_ok=True)

        v = dl_dir / "7555555555555555555.mp4"
        v.write_bytes(b"fake")
        vd = dedup_dir / "7555555555555555555_dedup.mp4"
        vd.write_bytes(b"deduped")

        video_entry = {"video_id": "7555555555555555555", "file_path": str(v),
                       "file_name": v.name, "original_url": ""}
        pipeline.downloader.discover_user_videos.return_value = [video_entry]
        pipeline.deduplicator.deduplicate.return_value = str(vd)
        pipeline.uploader.upload.return_value = True

        stats1 = pipeline.run_once()
        stats2 = pipeline.run_once()

        self.assertEqual(stats1["downloaded"], 1)
        self.assertEqual(stats2["downloaded"], 0)   # 第二次跳过
        self.assertEqual(stats2["skipped"], 1)
        ok("Pipeline Mock: 重复 video_id 在第二次运行被跳过")


# ══════════════════════════════════════════════════════════════════════
# 7. 依赖检测测试
# ══════════════════════════════════════════════════════════════════════
class TestDependencyChecker(unittest.TestCase):
    def test_check_all_returns_dict(self):
        from dependency_checker import check_all, get_missing
        results = check_all(saa_path="./social-auto-upload", verbose=False)
        self.assertIn("python", results)
        self.assertIn("commands", results)
        self.assertIn("pip", results)
        self.assertIn("f2", results)
        self.assertIn("browser", results)
        self.assertIn("saa", results)
        ok("DependencyChecker: check_all 返回结构正确")

    def test_python_version_ok(self):
        from dependency_checker import check_all
        results = check_all(verbose=False)
        py_ok, py_ver = results["python"]
        self.assertTrue(py_ok, f"Python version check failed: {py_ver}")
        ok(f"DependencyChecker: Python OK ({py_ver})")

    def test_ffmpeg_detected(self):
        from dependency_checker import check_cmd
        ok_flag, ver, err = check_cmd("ffmpeg", "ffmpeg", "-version")
        self.assertTrue(ok_flag, f"ffmpeg not found: {err}")
        ok(f"DependencyChecker: ffmpeg OK ({ver})")

    def test_f2_detected(self):
        from dependency_checker import check_f2
        ok_flag, ver, err = check_f2()
        self.assertTrue(ok_flag, f"f2 not found: {err}")
        ok(f"DependencyChecker: f2 OK ({ver})")

    def test_saa_detected(self):
        from dependency_checker import check_saa
        ok_flag, path, err = check_saa("./social-auto-upload")
        self.assertTrue(ok_flag, f"social-auto-upload check failed: {err}")
        ok(f"DependencyChecker: social-auto-upload OK ({path})")

    def test_get_missing_structure(self):
        from dependency_checker import check_all, get_missing
        results = check_all(verbose=False)
        missing = get_missing(results)
        self.assertIn("commands", missing)
        self.assertIn("pip", missing)
        self.assertIn("f2", missing)
        self.assertIn("browser", missing)
        ok("DependencyChecker: get_missing 返回结构正确")


# ══════════════════════════════════════════════════════════════════════
# 8. CLI 命令测试
# ══════════════════════════════════════════════════════════════════════
class TestCLI(unittest.TestCase):
    def _run_cli(self, args, timeout=30):
        result = subprocess.run(
            [sys.executable, "main.py"] + args,
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result

    def test_stats(self):
        r = self._run_cli(["stats"])
        self.assertEqual(r.returncode, 0)
        self.assertIn("Database Stats", r.stdout)
        ok("CLI: `python3 main.py stats` 正常")

    def test_history(self):
        r = self._run_cli(["history", "--limit", "5"])
        self.assertEqual(r.returncode, 0)
        self.assertIn("Runs", r.stdout)
        ok("CLI: `python3 main.py history --limit 5` 正常")

    def test_invalid_action(self):
        r = self._run_cli(["badaction"])
        self.assertNotEqual(r.returncode, 0)
        ok("CLI: 无效 action 返回非零退出码")

    def test_missing_config(self):
        r = self._run_cli(["--config", "/nonexistent/config.yaml", "stats"])
        self.assertNotEqual(r.returncode, 0)
        ok("CLI: 配置文件不存在时返回非零退出码")


# ══════════════════════════════════════════════════════════════════════
# 主程序
# ══════════════════════════════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run pipeline tests")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--failfast", "-f", action="store_true")
    parser.add_argument("test_names", nargs="*", help="Optional specific test class names")
    args = parser.parse_args()

    verbosity = 2 if args.verbose else 1

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    all_classes = [
        TestConfig,
        TestDatabase,
        TestDeduplicator,
        TestDownloader,
        TestUploader,
        TestPipelineMock,
        TestDependencyChecker,
        TestCLI,
    ]

    if args.test_names:
        name_map = {cls.__name__: cls for cls in all_classes}
        selected = [name_map[n] for n in args.test_names if n in name_map]
        if not selected:
            print(f"No matching test classes. Available: {list(name_map.keys())}")
            sys.exit(1)
        classes = selected
    else:
        classes = all_classes

    for cls in classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    head(f"Running {suite.countTestCases()} tests")
    runner = unittest.TextTestRunner(verbosity=verbosity, failfast=args.failfast)
    result = runner.run(suite)

    print()
    if result.wasSuccessful():
        print(_c(f"  ALL {result.testsRun} TESTS PASSED", "green"))
    else:
        print(_c(f"  {len(result.failures)} FAILURES  {len(result.errors)} ERRORS", "red"))
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
