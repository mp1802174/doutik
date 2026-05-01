#!/usr/bin/env python3
"""交互式配置向导，自动检测环境、引导填写参数、写入所有配置文件"""
import os
import shutil
import subprocess
import sys
from pathlib import Path
import yaml


def c(text, color="white"):
    colors = {"red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
              "blue": "\033[94m", "cyan": "\033[96m", "bold": "\033[1m", "white": "\033[0m"}
    return f"{colors.get(color, '')}{text}\033[0m"


def header(text):
    print(f"\n{c('='*60, 'cyan')}\n{c('  '+text, 'bold')}\n{c('='*60, 'cyan')}\n")


def step(n, total, text):
    print(f"{c(f'[{n}/{total}] ', 'yellow')}{c(text, 'bold')}")


def ok(text): print(f"  {c('✓ '+text, 'green')}")


def warn(text): print(f"  {c('⚠ '+text, 'yellow')}")


def err(text): print(f"  {c('✗ '+text, 'red')}")


def info(text): print(f"  {c('ℹ '+text, 'blue')}")


def ask(prompt, default=""):
    if default:
        v = input(c(f"  {prompt}", "cyan") + c(f" [默认: {default}]: ", "yellow")).strip()
    else:
        v = input(c(f"  {prompt}: ", "cyan")).strip()
    return v if v else default


def confirm(prompt, default=True):
    h = "[Y/n]" if default else "[y/N]"
    v = input(c(f"  {prompt} {h}: ", "cyan")).strip().lower()
    return default if not v else v in ("y", "yes", "是", "1")


def run_cmd(cmd, timeout=10):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, r.stdout.strip(), r.stderr.strip()
    except Exception as e:
        return False, "", str(e)


def check_env():
    header("环境检测")
    try:
        from dependency_checker import check_all, get_missing, prompt_install
        results = check_all(saa_path="./social-auto-upload", verbose=True)
        missing = get_missing(results)
        if any(missing.values()):
            if not prompt_install(missing, saa_path="./social-auto-upload"):
                if not confirm("是否继续配置？"):
                    sys.exit(0)
        else:
            ok("环境检查全部通过")
    except ImportError:
        # Fallback: basic manual check if dependency_checker not available
        issues = []
        for name, cmd in [("Python", f'"{sys.executable}" --version'),
                          ("ffmpeg", "ffmpeg -version"), ("git", "git --version")]:
            ok_flag, out, _ = run_cmd(cmd, 5)
            if ok_flag:
                ok(f"{name}: {out.splitlines()[0][:50] if out else '已安装'}")
            else:
                err(f"{name}: 未安装")
                issues.append(name)
        for pkg in ["yaml", "apscheduler", "playwright"]:
            try:
                __import__("yaml" if pkg == "yaml" else pkg)
                ok(f"Python 包 {pkg}: 已安装")
            except ImportError:
                warn(f"Python 包 {pkg}: 未安装")
                issues.append(f"pip:{pkg}")
        if shutil.which("f2"):
            ok("f2: 已安装")
        else:
            err("f2: 未安装 (pip install f2)")
            issues.append("f2")
        if issues:
            warn(f"检测到 {len(issues)} 个问题")
            if not confirm("是否继续配置？"):
                sys.exit(0)
        else:
            ok("环境检查全部通过")
    print()


def step1_paths():
    step(1, 6, "项目路径配置")
    base = Path(__file__).parent.resolve()
    info(f"当前项目根目录: {base}")
    saa_default = str(base / "social-auto-upload")
    saa = ask("social-auto-upload 目录路径", saa_default)
    saa_p = Path(saa).expanduser()
    if not saa_p.exists():
        warn(f"目录不存在: {saa_p}")
        if confirm("是否自动 clone？"):
            ok_flag, _, e = run_cmd(f'git clone https://github.com/dreammis/social-auto-upload.git "{saa_p}"', 60)
            if ok_flag:
                ok("clone 成功")
            else:
                err(f"clone 失败: {e}")
    f2_default = str(base / "my_apps.yaml")
    f2 = ask("f2 配置文件路径", f2_default)
    return {"saa_path": str(saa_p), "f2_config": f2, "base_dir": str(base)}


def step2_direction():
    step(2, 6, "搬运方向与批量设置")
    info("选择视频搬运方向")
    print("  1. 抖音 → TikTok\n  2. TikTok → 抖音\n")
    while True:
        ch = ask("请选择", "1")
        if ch in ("1", "2"):
            break
        err("请输入 1 或 2")
    direction = "douyin_to_tiktok" if ch == "1" else "tiktok_to_douyin"
    src, dst = ("抖音", "TikTok") if ch == "1" else ("TikTok", "抖音")
    ok(f"方向: {src} → {dst}")
    batch = ask("每批处理个数", "5")
    try:
        batch_size = max(1, min(50, int(batch)))
    except ValueError:
        batch_size = 5
        warn(f"使用默认值: {batch_size}")
    interval = ask("定时运行间隔（分钟）", "300")
    try:
        interval_min = max(1, int(interval))
    except ValueError:
        interval_min = 300
    cleanup = confirm("上传成功后删除本地视频？", True)
    return {"direction": direction, "batch_size": batch_size,
            "schedule_interval_minutes": interval_min, "cleanup_after_upload": cleanup}


def step3_url(direction):
    step(3, 6, "源账号配置")
    src = "抖音" if direction == "douyin_to_tiktok" else "TikTok"
    info(f"请输入要下载的 {src} 用户主页 URL")
    if direction == "douyin_to_tiktok":
        info("格式: https://www.douyin.com/user/MS4wLjABAAAAxxxxx")
        prompt = "抖音用户主页 URL"
    else:
        info("格式: https://www.tiktok.com/@username")
        prompt = "TikTok 用户主页 URL"
    url = ask(prompt, "")
    ok(f"源账号 URL: {url}")
    return {"source_url": url}


def step4_account(paths):
    step(4, 6, "上传账号配置")
    account = ask("账号标识名（用于保存 Cookie）", "default")
    ok(f"账号名: {account}")
    saa_p = Path(paths["saa_path"])
    cookies_dir = saa_p / "cookies"
    if cookies_dir.exists():
        cookies = list(cookies_dir.glob("*.json"))
        if cookies:
            ok(f"发现 {len(cookies)} 个已保存 Cookie")
            for c in cookies:
                print(f"    - {c.name}")
        else:
            warn("未找到 Cookie，需要首次登录")
    else:
        warn("目录结构不完整，需要首次登录")
    print("\n  1. 现在立即登录（弹出浏览器）\n  2. 稍后手动登录\n")
    login_now = ask("请选择", "2") == "1"
    return {"account_name": account, "login_now": login_now}


def step5_dedup():
    step(5, 6, "去重处理策略")
    info("选择去重强度")
    print("  1. 轻量模式（推荐）- 镜像+裁剪+颜色+淡入淡出")
    print("  2. 标准模式 - 增加轻微旋转")
    print("  3. 强力模式 - 增加画中画叠加\n")
    while True:
        ch = ask("请选择", "1")
        if ch in ("1", "2", "3"):
            break
        err("请输入 1/2/3")
    presets = {
        "1": {"name": "轻量", "flip_horizontal": True, "rotation_angle": 0.0,
              "crop_percentage": 0.02, "saturation": 1.05, "brightness": 0.03,
              "contrast": 1.05, "fade_in_frames": 5, "fade_out_frames": 15,
              "include_hzh": False, "ffmpeg_preset": "ultrafast"},
        "2": {"name": "标准", "flip_horizontal": True, "rotation_angle": -2.0,
              "crop_percentage": 0.03, "saturation": 1.08, "brightness": 0.05,
              "contrast": 1.08, "fade_in_frames": 8, "fade_out_frames": 20,
              "include_hzh": False, "ffmpeg_preset": "fast"},
        "3": {"name": "强力", "flip_horizontal": True, "rotation_angle": -3.0,
              "crop_percentage": 0.05, "saturation": 1.10, "brightness": 0.05,
              "contrast": 1.10, "fade_in_frames": 10, "fade_out_frames": 25,
              "include_hzh": True, "ffmpeg_preset": "medium"},
    }
    preset = presets[ch]
    ok(f"已选择: {preset['name']}模式")
    if confirm("是否微调参数？", False):
        v = ask("旋转角度（-5~5）", str(preset["rotation_angle"]))
        try:
            preset["rotation_angle"] = float(v)
        except:
            pass
        v = ask("裁剪比例（0~0.2）", str(preset["crop_percentage"]))
        try:
            preset["crop_percentage"] = max(0, min(0.2, float(v)))
        except:
            pass
        v = ask("ffmpeg 预设", preset["ffmpeg_preset"])
        if v in ("ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow"):
            preset["ffmpeg_preset"] = v
    print()
    return preset


def step6_cookie(direction):
    step(6, 6, "下载 Cookie 配置")
    src = "抖音" if direction == "douyin_to_tiktok" else "TikTok"
    info(f"f2 下载 {src} 视频需要 Cookie")
    info("获取方式: 浏览器登录 → F12 → Network → 复制 Cookie")
    print("\n  1. 现在输入 Cookie\n  2. 跳过，稍后手动编辑 my_apps.yaml\n")
    cookie_choice = ask("请选择", "1")
    cookie = ""
    if cookie_choice == "1":
        info("粘贴 Cookie 字符串（输入空行结束）:")
        lines = []
        while True:
            line = input()
            if not line.strip():
                break
            lines.append(line)
        cookie = " ".join(lines).strip()
        if cookie:
            ok(f"已接收 Cookie ({len(cookie)} 字符)")
        else:
            warn("未输入 Cookie")
    return {"cookie": cookie, "skip": cookie_choice != "1"}


def generate(all_cfg):
    header("生成配置文件")
    base = Path(all_cfg["paths"]["base_dir"])
    direction = all_cfg["direction"]["direction"]

    config_yaml = {
        "pipeline": {
            "direction": direction,
            "batch_size": all_cfg["direction"]["batch_size"],
            "schedule_interval_minutes": all_cfg["direction"]["schedule_interval_minutes"],
            "cleanup_after_upload": all_cfg["direction"]["cleanup_after_upload"],
            "timezone": "Asia/Shanghai", "max_workers": 2,
        },
        "downloader": {
            "f2_config": all_cfg["paths"]["f2_config"],
            "douyin_user_url": all_cfg["source"]["source_url"] if direction == "douyin_to_tiktok" else "",
            "tiktok_user_url": all_cfg["source"]["source_url"] if direction == "tiktok_to_douyin" else "",
            "download_dir": "./download", "timeout": 120, "download_delay": 3,
        },
        "deduplicator": {
            "output_dir": "./dedup",
            "flip_horizontal": all_cfg["dedup"]["flip_horizontal"],
            "rotation_angle": all_cfg["dedup"]["rotation_angle"],
            "crop_percentage": all_cfg["dedup"]["crop_percentage"],
            "saturation": all_cfg["dedup"]["saturation"],
            "brightness": all_cfg["dedup"]["brightness"],
            "contrast": all_cfg["dedup"]["contrast"],
            "fade_in_frames": all_cfg["dedup"]["fade_in_frames"],
            "fade_out_frames": all_cfg["dedup"]["fade_out_frames"],
            "top_blur_percentage": 0, "bottom_blur_percentage": 0, "side_blur_percentage": 0,
            "include_hzh": all_cfg["dedup"]["include_hzh"],
            "hzh_opacity": 0.1, "hzh_scale": 0.3, "hzh_video_file": "",
            "include_watermark": False, "watermark_text": "",
            "ffmpeg_preset": all_cfg["dedup"]["ffmpeg_preset"], "ffmpeg_crf": 28,
        },
        "uploader": {
            "social_auto_upload_path": all_cfg["paths"]["saa_path"],
            "account_name": all_cfg["account"]["account_name"],
            "headless": True, "timeout": 300, "upload_delay": 10,
            "tiktok_tags": [], "douyin_category": "",
        },
        "database": {"db_path": "./state.db", "retention_days": 0},
        "logging": {"level": "INFO", "log_dir": "./logs"},
    }

    with open(base / "config.yaml", "w", encoding="utf-8") as f:
        yaml.dump(config_yaml, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    ok("config.yaml 已生成")

    f2_path = Path(all_cfg["paths"]["f2_config"])
    if not f2_path.is_absolute():
        f2_path = base / f2_path

    cookie = all_cfg["cookie"]["cookie"]
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0"

    if direction == "douyin_to_tiktok":
        my_apps = {
            "proxies": {"http://": "", "https://": ""},
            "douyin": {
                "headers": {"User-Agent": ua, "Referer": "https://www.douyin.com/"},
                "cookie": cookie, "proxies": {"http://": "", "https://": ""},
                "music": "no", "cover": "no", "desc": "yes", "folderize": "yes", "mode": "post", "naming": "{create}",
            },
            "tiktok": {
                "headers": {"User-Agent": ua, "Referer": "https://www.tiktok.com/"},
                "cookie": "", "proxies": {"http://": "", "https://": ""},
                "music": "no", "cover": "no", "desc": "yes", "folderize": "yes", "mode": "post", "naming": "{create}",
            },
        }
    else:
        my_apps = {
            "proxies": {"http://": "", "https://": ""},
            "tiktok": {
                "headers": {"User-Agent": ua, "Referer": "https://www.tiktok.com/"},
                "cookie": cookie, "proxies": {"http://": "", "https://": ""},
                "music": "no", "cover": "no", "desc": "yes", "folderize": "yes", "mode": "post", "naming": "{create}",
            },
            "douyin": {
                "headers": {"User-Agent": ua, "Referer": "https://www.douyin.com/"},
                "cookie": "", "proxies": {"http://": "", "https://": ""},
                "music": "no", "cover": "no", "desc": "yes", "folderize": "yes", "mode": "post", "naming": "{create}",
            },
        }

    with open(f2_path, "w", encoding="utf-8") as f:
        yaml.dump(my_apps, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    ok("my_apps.yaml 已生成")

    for d in [base / "download", base / "dedup", base / "logs", base / "cookies"]:
        d.mkdir(exist_ok=True)
        ok(f"目录: {d}")
    print()
    return base / "config.yaml", f2_path


def show_summary(all_cfg, config_path, f2_path):
    header("配置完成！")
    direction = all_cfg["direction"]["direction"]
    src = "抖音" if direction == "douyin_to_tiktok" else "TikTok"
    dst = "TikTok" if direction == "douyin_to_tiktok" else "抖音"

    print(f"  搬运方向: {c(src+' → '+dst, 'green')}")
    print(f"  每批个数: {c(str(all_cfg['direction']['batch_size']), 'green')}")
    print(f"  定时间隔: {c(str(all_cfg['direction']['schedule_interval_minutes'])+' 分钟', 'green')}")
    print(f"  源账号: {c(all_cfg['source']['source_url'], 'green')}")
    print(f"  去重模式: {c(all_cfg['dedup']['ffmpeg_preset'], 'green')}")
    print(f"  config.yaml: {c(str(config_path), 'cyan')}")
    print(f"  my_apps.yaml: {c(str(f2_path), 'cyan')}")
    print()

    info("下一步操作:")
    if all_cfg["account"]["login_now"]:
        print(f"  1. {c('登录目标平台', 'yellow')}: python main.py login {'tiktok' if direction == 'douyin_to_tiktok' else 'douyin'}")
    else:
        print(f"  1. {c('登录目标平台（必需）', 'red')}: python main.py login {'tiktok' if direction == 'douyin_to_tiktok' else 'douyin'}")
    print(f"  2. {c('测试运行一次', 'yellow')}: python main.py run")
    print(f"  3. {c('启动定时调度', 'yellow')}: python main.py schedule")
    print(f"  4. {c('查看统计', 'yellow')}: python main.py stats")
    print()

    if all_cfg["cookie"]["skip"]:
        warn("您跳过了 Cookie 配置，请手动编辑 my_apps.yaml 填入 Cookie")
    print()


def main():
    print(c("\n" + "=" * 60, "cyan"))
    print(c("  Douyin-TikTok 自动搬运流水线 - 配置向导", "bold"))
    print(c("=" * 60, "cyan"))
    print()

    check_env()

    paths = step1_paths()
    direction = step2_direction()
    source = step3_url(direction["direction"])
    account = step4_account(paths)
    dedup = step5_dedup()
    cookie = step6_cookie(direction["direction"])

    all_cfg = {
        "paths": paths,
        "direction": direction,
        "source": source,
        "account": account,
        "dedup": dedup,
        "cookie": cookie,
    }

    config_path, f2_path = generate(all_cfg)
    show_summary(all_cfg, config_path, f2_path)


if __name__ == "__main__":
    main()
