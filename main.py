#!/usr/bin/env python3
"""
Douyin <-> TikTok 自动搬运流水线
基于 video-mover 的 ffmpeg 去重策略 + social-auto-upload 的上传能力 + f2 的下载能力

Usage:
    python main.py run            # 立即执行一次
    python main.py schedule       # 启动定时调度（后台循环）
    python main.py login <platform>   # 登录平台获取 Cookie
    python main.py stats          # 查看数据库统计
    python main.py history        # 查看最近运行记录
"""

import argparse
import signal
import sys
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from pipeline import VideoPipeline, load_pipeline_config
from uploader import SocialAutoUploader
from utils.logger import setup_logger

logger = setup_logger("main")

# 全局配置路径
CONFIG_PATH = Path("config.yaml")

# 用于优雅关闭调度器的标志
_shutdown_event = False


def handle_sigint(signum, frame):
    global _shutdown_event
    logger.info("Received SIGINT, shutting down gracefully...")
    _shutdown_event = True
    sys.exit(0)


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


def cmd_run(config_path: str):
    """立即执行一个批次的流水线"""
    logger.info("Running single pipeline batch...")
    try:
        cfg = load_pipeline_config(config_path)
        pipeline = VideoPipeline(cfg)
        stats = pipeline.run_once()
        logger.info(
            "Run complete: downloaded=%d, deduped=%d, uploaded=%d, failed=%d",
            stats["downloaded"],
            stats["deduped"],
            stats["uploaded"],
            stats["failed"],
        )
    except Exception as e:
        logger.exception("Run failed: %s", e)
        raise


def cmd_schedule(config_path: str):
    """启动定时调度器，按配置间隔循环执行"""
    try:
        cfg = load_pipeline_config(config_path)
    except Exception as e:
        logger.error("Failed to load config: %s", e)
        sys.exit(1)

    interval = cfg.schedule_interval_minutes

    logger.info(
        "Starting scheduler | interval=%d min | direction=%s | batch_size=%d",
        interval,
        cfg.direction,
        cfg.batch_size,
    )

    scheduler = BlockingScheduler()
    scheduler.add_job(
        func=lambda: cmd_run(config_path),
        trigger=IntervalTrigger(minutes=interval),
        id="pipeline_job",
        replace_existing=True,
        max_instances=1,  # 防止重叠执行
        misfire_grace_time=60,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
        scheduler.shutdown(wait=False)


def cmd_login(platform: str, config_path: str):
    """登录指定平台并保存 Cookie"""
    import yaml
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    u = raw.get("uploader", {})
    saa_path = u.get("social_auto_upload_path", "./social-auto-upload")
    account_name = u.get("account_name", "default")

    uploader = SocialAutoUploader(
        saa_path=saa_path,
        account_name=account_name,
        headless=False,  # 登录必须有界面
        timeout=300,
    )
    success = uploader.login(platform)
    if success:
        print(f"Login successful for {platform}! Cookie saved.")
    else:
        print(f"Login failed for {platform}.")
        sys.exit(1)


def cmd_stats(config_path: str):
    """查看数据库统计"""
    cfg = load_pipeline_config(config_path)
    pipeline = VideoPipeline(cfg)
    stats = pipeline.get_stats()
    print("\n--- Database Stats ---")
    for status, count in stats.items():
        print(f"  {status:12s}: {count}")
    print("-" * 25)
    total = sum(stats.values())
    print(f"  {'total':12s}: {total}")
    print()


def cmd_history(config_path: str, limit: int = 10):
    """查看最近运行记录"""
    cfg = load_pipeline_config(config_path)
    pipeline = VideoPipeline(cfg)
    runs = pipeline.get_recent_runs(limit)
    print(f"\n--- Recent {len(runs)} Runs ---")
    print(
        f"{'Run ID':<8} {'Start':<20} {'End':<20} {'Dir':<18} {'Batch':<6} {'DL':<5} {'DEDUP':<6} {'UP':<5} {'FAIL':<5} {'Status':<10}"
    )
    print("-" * 105)
    for r in runs:
        start = r.get("run_start", "") or ""
        end = r.get("run_end", "") or ""
        print(
            f"{r.get('id',0):<8} {str(start)[:19]:<20} {str(end)[:19]:<20} "
            f"{r.get('direction',''):<18} {r.get('batch_size',0):<6} "
            f"{r.get('downloaded',0):<5} {r.get('deduped',0):<6} "
            f"{r.get('uploaded',0):<5} {r.get('failed',0):<5} {r.get('status',''):<10}"
        )
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Douyin <-> TikTok Video Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py run                    # Run one batch immediately
  python main.py schedule               # Start background scheduler
  python main.py login tiktok           # Login TikTok (interactive)
  python main.py login douyin           # Login Douyin (interactive)
  python main.py stats                  # Show database statistics
  python main.py history --limit 20     # Show last 20 runs
        """,
    )
    parser.add_argument(
        "action",
        choices=["run", "schedule", "login", "stats", "history"],
        help="Action to perform",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config YAML (default: config.yaml)",
    )
    parser.add_argument(
        "platform",
        nargs="?",
        choices=["tiktok", "douyin"],
        help="Platform for login (required with 'login' action)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of history entries to show (default: 10)",
    )

    args = parser.parse_args()

    config_path = args.config
    if not Path(config_path).exists():
        print(f"ERROR: Config file not found: {config_path}")
        print("Please run: python setup_wizard.py")
        sys.exit(1)

    # Run dependency check before commands that need external tools
    if args.action in ("run", "schedule", "login"):
        try:
            from dependency_checker import check_all, get_missing, has_critical_missing, install_all
            results = check_all(saa_path="./social-auto-upload", verbose=False)
            missing = get_missing(results)
            if has_critical_missing(missing):
                print("\n" + "=" * 50)
                print("  ⚠ 检测到缺失依赖")
                print("=" * 50)
                if missing["commands"]:
                    print(f"  命令: {', '.join(missing['commands'])}")
                if missing["pip"]:
                    print(f"  pip包: {', '.join(missing['pip'])}")
                if missing["f2"]:
                    print("  工具: f2")
                if missing["browser"]:
                    print("  浏览器: playwright chromium")
                print()
                answer = input("  是否自动安装缺失依赖？ [Y/n]: ").strip().lower()
                if answer in ("", "y", "yes", "是", "1"):
                    success, still = install_all(missing, saa_path="./social-auto-upload")
                    if success:
                        print("  依赖安装完成，继续运行...\n")
                    else:
                        print("\n  部分依赖安装失败，请运行: python setup_wizard.py")
                        print("  或手动安装后再试。\n")
                        sys.exit(1)
                else:
                    print("\n  请运行: python setup_wizard.py")
                    print("  或手动安装缺失项后再运行本命令。\n")
                    sys.exit(1)
        except ImportError:
            pass  # dependency_checker not available, proceed anyway

    if args.action == "run":
        cmd_run(config_path)
    elif args.action == "schedule":
        cmd_schedule(config_path)
    elif args.action == "login":
        if not args.platform:
            print("ERROR: 'login' requires a platform argument (tiktok or douyin)")
            sys.exit(1)
        cmd_login(args.platform, config_path)
    elif args.action == "stats":
        cmd_stats(config_path)
    elif args.action == "history":
        cmd_history(config_path, args.limit)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
