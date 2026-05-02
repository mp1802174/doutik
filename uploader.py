import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger("uploader")


class SocialAutoUploader:
    """封装 social-auto-upload 项目，支持 TikTok / 抖音 上传"""

    def __init__(
        self,
        saa_path: str,
        account_name: str = "default",
        headless: bool = True,
        timeout: int = 300,
        delay: int = 10,
        delay_min: int = 10,
        delay_max: int = 10,
    ):
        self.saa_path = Path(saa_path).resolve()
        self.account_name = account_name
        self.headless = headless
        self.timeout = timeout
        self.delay = delay
        self.delay_min = delay_min
        self.delay_max = delay_max
        # 上次上传是否被平台永久拒绝（视频指纹/水印/账号风控等）
        # pipeline 会读取此标志，将记录置为 rejected 状态，不再重试
        self.last_rejected: bool = False
        self.last_error_message: str = ""

        # 兼容新版 (sau_cli.py) 和旧版 (cli_main.py)
        self.cli_main = next(
            (f for f in [self.saa_path / "sau_cli.py", self.saa_path / "cli_main.py"] if f.exists()),
            self.saa_path / "sau_cli.py",
        )
        self.cookies_dir = self.saa_path / "cookies"
        self.conf_file = self.saa_path / "conf.py"

        self._validate()
        self._ensure_conf_py()
        self._patch_headless(self.headless)

    def _validate(self):
        if not self.saa_path.exists():
            raise FileNotFoundError(
                f"social-auto-upload path not found: {self.saa_path}\n"
                "Please clone: git clone https://github.com/dreammis/social-auto-upload.git"
            )
        cli_files = [self.saa_path / "sau_cli.py", self.saa_path / "cli_main.py"]
        if not any(f.exists() for f in cli_files):
            raise FileNotFoundError(
                f"Neither sau_cli.py nor cli_main.py found in {self.saa_path}."
            )
        self.cookies_dir.mkdir(parents=True, exist_ok=True)
        logger.info("social-auto-upload validated: %s", self.saa_path)

    def _ensure_conf_py(self):
        """若 conf.py 不存在则从 conf.example.py 复制创建"""
        if self.conf_file.exists():
            return
        example = self.saa_path / "conf.example.py"
        if example.exists():
            shutil.copy2(str(example), str(self.conf_file))
            logger.info("Created conf.py from conf.example.py")
        else:
            # 写一个最小 conf.py
            self.conf_file.write_text(
                "from pathlib import Path\n"
                "BASE_DIR = Path(__file__).parent.resolve()\n"
                "XHS_SERVER = 'http://127.0.0.1:11901'\n"
                "LOCAL_CHROME_PATH = ''\n"
                "LOCAL_CHROME_HEADLESS = True\n"
                "DEBUG_MODE = True\n",
                encoding="utf-8",
            )
            logger.info("Created minimal conf.py")

    def _patch_headless(self, value: bool):
        """修改 conf.py 中的 LOCAL_CHROME_HEADLESS 值"""
        if not self.conf_file.exists():
            return
        try:
            import re
            content = self.conf_file.read_text(encoding="utf-8")
            new_content = re.sub(
                r"LOCAL_CHROME_HEADLESS\s*=\s*(True|False)",
                f"LOCAL_CHROME_HEADLESS = {value}",
                content,
            )
            if new_content != content:
                self.conf_file.write_text(new_content, encoding="utf-8")
                logger.info("Patched conf.py: LOCAL_CHROME_HEADLESS = %s", value)
        except Exception as e:
            logger.warning("Failed to patch conf.py: %s", e)

    def _get_account_file(self, platform: str) -> Path:
        """获取平台 Cookie 文件路径"""
        return self.cookies_dir / f"{platform}_{self.account_name}.json"

    def check_login_status(self, platform: str) -> bool:
        """检查指定平台是否已登录（Cookie 文件存在且非空）"""
        cookie_file = self._get_account_file(platform)
        return cookie_file.exists() and cookie_file.stat().st_size > 100

    # ------------------------------------------------------------------
    # Login
    # ------------------------------------------------------------------

    def login(self, platform: str) -> bool:
        """登录指定平台并保存 Cookie（弹出浏览器交互）"""
        logger.info("Login: platform=%s account=%s", platform, self.account_name)
        self._patch_headless(False)  # 登录必须有界面
        try:
            if platform == "tiktok":
                return self._login_tiktok()
            elif platform in ("douyin", "kuaishou", "xiaohongshu", "bilibili"):
                return self._login_via_sau_cli(platform)
            else:
                logger.error("Unsupported platform for login: %s", platform)
                return False
        finally:
            self._patch_headless(self.headless)

    def _login_via_sau_cli(self, platform: str) -> bool:
        """通过 sau_cli.py 登录 Douyin/Kuaishou/Xiaohongshu/Bilibili"""
        cmd = [
            sys.executable,
            str(self.cli_main),
            platform,
            "login",
            "--account", self.account_name,
            "--headed",  # 登录必须有界面
        ]
        logger.info("Running: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.saa_path),
                timeout=300,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode == 0:
                logger.info("Login OK: %s", platform)
                return True
            logger.error("Login failed (code=%d)", result.returncode)
            return False
        except subprocess.TimeoutExpired:
            logger.error("Login timed out (5min)")
            return False
        except Exception as e:
            logger.error("Login error: %s", e)
            return False

    def _login_tiktok(self) -> bool:
        """通过 TikTok 帮助脚本登录（直接调用 tk_uploader Python API）"""
        helper = self.saa_path / "tiktok_helper.py"
        if not helper.exists():
            logger.error("tiktok_helper.py not found: %s", helper)
            return False
        cookie_file = self._get_account_file("tiktok")
        cmd = [
            sys.executable, str(helper),
            "login",
            "--account-file", str(cookie_file),
        ]
        logger.info("TikTok login: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.saa_path),
                timeout=300,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode == 0:
                logger.info("TikTok login OK, cookie: %s", cookie_file)
                return True
            logger.error("TikTok login failed (code=%d)", result.returncode)
            return False
        except subprocess.TimeoutExpired:
            logger.error("TikTok login timed out")
            return False
        except Exception as e:
            logger.error("TikTok login error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload(
        self,
        platform: str,
        video_path: str,
        title: str = "",
        tags: Optional[list] = None,
        publish_type: int = 0,
        schedule: Optional[str] = None,
    ) -> bool:
        """上传视频到指定平台"""
        # 每次调用前重置标志
        self.last_rejected = False
        self.last_error_message = ""

        video_path = Path(video_path).resolve()
        if not video_path.exists():
            logger.error("Video file not found: %s", video_path)
            self.last_error_message = f"Video file not found: {video_path}"
            return False

        cookie_file = self._get_account_file(platform)
        if not cookie_file.exists():
            logger.error(
                "Cookie not found: %s  →  Run: python main.py login %s",
                cookie_file, platform,
            )
            return False

        logger.info("Uploading to %s: %s (title=%s)", platform, video_path.name,
                    title[:30] if title else "(auto)")

        if platform == "tiktok":
            return self._upload_tiktok(video_path, title, tags, publish_type, schedule)
        elif platform in ("douyin", "kuaishou", "xiaohongshu"):
            return self._upload_via_sau_cli(platform, video_path, title, tags, publish_type, schedule)
        else:
            logger.error("Unsupported platform: %s", platform)
            return False

    def _upload_via_sau_cli(
        self,
        platform: str,
        video_path: Path,
        title: str,
        tags: Optional[list],
        publish_type: int,
        schedule: Optional[str],
    ) -> bool:
        """通过 sau_cli.py 上传（Douyin / Kuaishou / Xiaohongshu）"""
        tag_str = ",".join(tags) if tags else ""
        cmd = [
            sys.executable,
            str(self.cli_main),
            platform,
            "upload-video",
            "--account", self.account_name,
            "--file", str(video_path),
            "--title", title or video_path.stem,
        ]
        if tag_str:
            cmd += ["--tags", tag_str]
        if publish_type == 1 and schedule:
            cmd += ["--schedule", schedule]
        if self.headless:
            cmd.append("--headless")
        else:
            cmd.append("--headed")

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.saa_path),
                capture_output=True,
                text=True,
                timeout=self.timeout,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode == 0:
                logger.info("Upload OK: %s → %s", video_path.name, platform)
                return True
            logger.error(
                "Upload failed (code=%d): %s",
                result.returncode,
                (result.stderr or result.stdout or "")[-500:],
            )
            return False
        except subprocess.TimeoutExpired:
            logger.error("Upload timed out: %s", video_path.name)
            return False
        except Exception as e:
            logger.error("Upload error: %s", e)
            return False

    def _upload_tiktok(
        self,
        video_path: Path,
        title: str,
        tags: Optional[list],
        publish_type: int,
        schedule: Optional[str],
    ) -> bool:
        """通过 tiktok_helper.py 上传 TikTok"""
        helper = self.saa_path / "tiktok_helper.py"
        if not helper.exists():
            logger.error("tiktok_helper.py not found: %s", helper)
            return False

        cookie_file = self._get_account_file("tiktok")
        tag_str = ",".join(tags) if tags else ""
        cmd = [
            sys.executable, str(helper),
            "upload",
            "--account-file", str(cookie_file),
            "--file", str(video_path),
            "--title", title or video_path.stem,
        ]
        if tag_str:
            cmd += ["--tags", tag_str]

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.saa_path),
                capture_output=True,
                text=True,
                timeout=900,  # TikTok 实际上传超过 5 分钟，固定 15 分钟上限
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode == 0:
                logger.info("TikTok upload OK: %s", video_path.name)
                return True
            err_tail = (result.stderr or result.stdout or "")[-500:]
            self.last_error_message = err_tail
            # 退出码 2 = TikTok 平台拒绝（视频指纹/水印/账号风控），永久标记
            if result.returncode == 2:
                self.last_rejected = True
                logger.warning(
                    "TikTok REJECTED %s (will not retry): %s",
                    video_path.name, err_tail,
                )
            else:
                logger.error(
                    "TikTok upload failed (code=%d): %s",
                    result.returncode, err_tail,
                )
            return False
        except subprocess.TimeoutExpired:
            self.last_error_message = "Upload timed out"
            logger.error("TikTok upload timed out: %s", video_path.name)
            return False
        except Exception as e:
            self.last_error_message = str(e)
            logger.error("TikTok upload error: %s", e)
            return False
