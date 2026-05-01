#!/usr/bin/env python3
"""依赖检测与自动安装模块"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

def C(t, color=""):
    if sys.stdout.isatty():
        colors = {"red":"91","green":"92","yellow":"93","blue":"94","cyan":"96","bold":"1"}
        return f"\033[{colors.get(color,'0')}m{t}\033[0m"
    return t

def ok(t):  print(f"  {C('✓ '+t, 'green')}")
def warn(t): print(f"  {C('⚠ '+t, 'yellow')}")
def err(t):  print(f"  {C('✗ '+t, 'red')}")
def info(t): print(f"  {C('ℹ '+t, 'blue')}")

PROXY_URL = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:10808"

def _raw_run(cmd, timeout=120, capture=True, env=None):
    """底层运行命令，不设置代理"""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=capture, text=True, timeout=timeout, check=False, env=env)
        return r.returncode == 0, (r.stdout or "").strip(), (r.stderr or "").strip()
    except Exception as e:
        return False, "", str(e)

def _test_proxy(proxy_url):
    """测试代理是否可用"""
    test_cmd = f'curl -s -o /dev/null -w "%{{http_code}}" --connect-timeout 3 --proxy {proxy_url} https://github.com'
    ok, out, _ = _raw_run(test_cmd, timeout=5)
    return ok and out.strip() == "200"

def run(cmd, timeout=120, capture=True, use_proxy=True):
    """运行命令，自动通过代理"""
    env = None
    if use_proxy and PROXY_URL:
        env = os.environ.copy()
        env["HTTP_PROXY"] = PROXY_URL
        env["HTTPS_PROXY"] = PROXY_URL
        env["http_proxy"] = PROXY_URL
        env["https_proxy"] = PROXY_URL
    try:
        r = subprocess.run(cmd, shell=True, capture_output=capture, text=True, timeout=timeout, check=False, env=env)
        return r.returncode == 0, (r.stdout or "").strip(), (r.stderr or "").strip()
    except Exception as e:
        return False, "", str(e)

def check_cmd(name, cmd=None, arg="--version"):
    if cmd is None: cmd = name
    p = shutil.which(cmd)
    if not p: return False, None, f"未找到 {name}"
    ok_flag, out, _ = run(f'"{p}" {arg}', 5)
    return ok_flag, (out.splitlines()[0] if out else "已安装")[:60], None

def check_pip(pkg, import_name=None):
    if import_name is None: import_name = pkg
    try:
        mod = __import__(import_name)
        return True, getattr(mod, "__version__", "已安装"), None
    except ImportError:
        return False, None, f"未安装 {pkg}"

def check_f2(): return check_cmd("f2", "f2", "--help")

def check_browser():
    for base in [Path.home()/".cache"/("ms-playwright"), Path.home()/"AppData"/("Local")/("ms-playwright")]:
        if base.exists():
            # 兼容旧版 chromium-* 和新版 chromium_headless_shell-*
            if list(base.glob("chromium-*")) or list(base.glob("chromium_headless_shell-*")):
                return True, f"chromium found", None
    return False, None, "未下载"

def check_saa(path="./social-auto-upload"):
    p = Path(path).expanduser().resolve()
    if not (p.exists() and p.is_dir()): return False, None, f"目录不存在: {p}"
    # 兼容新版 (sau_cli.py) 和旧版 (cli_main.py / uploader)
    key_files = ["sau_cli.py", "sau_backend.py", "uploader", "cli_main.py"]
    return (True, str(p), None) if any((p/f).exists() for f in key_files) else (False, str(p), "结构不完整")

PIP_PKGS = [("pyyaml","yaml"),("apscheduler","apscheduler"),("playwright","playwright"),("videohash","videohash"),("httpx","httpx"),("Pillow","PIL"),("imagehash","imagehash")]
CMDS = [("ffmpeg","ffmpeg","-version"),("git","git","--version")]

def check_all(saa_path="./social-auto-upload", verbose=True):
    results = {"python": (sys.version_info >= (3,9), f"Python {sys.version_info.major}.{sys.version_info.minor}"), "commands": {}, "pip": {}, "f2": check_f2(), "browser": check_browser(), "saa": check_saa(saa_path)}
    for name, cmd, arg in CMDS: results["commands"][name] = check_cmd(name, cmd, arg)
    for pip_name, imp in PIP_PKGS: results["pip"][pip_name] = check_pip(pip_name, imp)
    if verbose:
        print(f"\n{C('='*50,'cyan')}\n{C('  依赖检测结果','bold')}\n{C('='*50,'cyan')}")
        py_ok, py_info = results["python"]; (ok if py_ok else err)(f"Python: {py_info}")
        for name, (cok, ver, e) in results["commands"].items(): (ok if cok else err)(f"{name}: {ver or e}")
        for name, (pok, ver, e) in results["pip"].items(): (ok if pok else err)(f"pip {name}: {ver or e}")
        f2_ok, f2_ver, f2_err = results["f2"]; (ok if f2_ok else err)(f"f2: {f2_ver or f2_err}")
        br_ok, br_ver, br_err = results["browser"]; (ok if br_ok else err)(f"playwright browser: {br_ver or br_err}")
        sa_ok, sa_path, sa_err = results["saa"]; (ok if sa_ok else warn)(f"social-auto-upload: {sa_path or sa_err}")
        print(C("="*50, "cyan"))
    return results

def get_missing(results):
    miss = {"commands": [], "pip": [], "f2": False, "browser": False, "saa": False}
    for name, (flag, _, _) in results["commands"].items():
        if not flag: miss["commands"].append(name)
    for name, (flag, _, _) in results["pip"].items():
        if not flag: miss["pip"].append(name)
    if not results["f2"][0]: miss["f2"] = True
    if not results["browser"][0]: miss["browser"] = True
    if not results["saa"][0]: miss["saa"] = True
    return miss

def install_pip(packages):
    if not packages: return True, []
    names = [p[0] for p in packages]
    print(f"\n  {C('>>> 安装 pip 包: '+', '.join(names), 'cyan')}")
    # Debian/Ubuntu PEP 668 需要 --break-system-packages
    extra = " --break-system-packages" if Path("/etc/debian_version").exists() else ""
    proxy = f" --proxy {PROXY_URL}" if PROXY_URL else ""
    run(f"{sys.executable} -m pip install{proxy}{extra} {' '.join(names)}", 300, False)
    still = [p for p in packages if not check_pip(*p)[0]]
    return not still, still

def install_f2():
    print(f"\n  {C('>>> 安装 f2', 'cyan')}")
    extra = " --break-system-packages" if Path("/etc/debian_version").exists() else ""
    proxy = f" --proxy {PROXY_URL}" if PROXY_URL else ""
    run(f"{sys.executable} -m pip install{proxy}{extra} f2", 300, False)
    return check_f2()[0]

def install_browser():
    print(f"\n  {C('>>> 安装 playwright chromium', 'cyan')}")
    run(f"{sys.executable} -m playwright install chromium", 300, False)
    return check_browser()[0]

def install_ffmpeg():
    print(f"\n  {C('>>> 安装 ffmpeg', 'cyan')}")
    if Path("/etc/debian_version").exists():
        run("sudo apt-get update && sudo apt-get install -y ffmpeg", 300, False)
        return check_cmd("ffmpeg")[0]
    warn("当前系统不支持自动安装 ffmpeg，请手动安装")
    return False

def clone_saa(target="./social-auto-upload"):
    p = Path(target).expanduser().resolve()
    if p.exists(): return True
    print(f"\n  {C('>>> clone social-auto-upload', 'cyan')}")
    proxy_cfg = ""
    if PROXY_URL:
        proxy_cfg = f' -c http.proxy={PROXY_URL} -c https.proxy={PROXY_URL}'
    run(f'git{proxy_cfg} clone https://github.com/dreammis/social-auto-upload.git "{p}"', 120, False)
    return p.exists()

def install_all(missing, saa_path="./social-auto-upload"):
    """自动安装所有缺失项，返回 (是否全部成功, 仍缺失)"""
    still = {"commands": [], "pip": [], "f2": False, "browser": False, "saa": False}
    # pip 包
    pip_missing = [(p, dict(PIP_PKGS)[p]) for p in missing["pip"]]
    if pip_missing:
        success, failed = install_pip(pip_missing)
        if failed: still["pip"] = [p[0] for p in failed]
    # f2
    if missing["f2"] and not install_f2(): still["f2"] = True
    # browser
    if missing["browser"] and not install_browser(): still["browser"] = True
    # ffmpeg
    if "ffmpeg" in missing["commands"]:
        if install_ffmpeg(): ok("ffmpeg 安装成功")
        else: still["commands"].append("ffmpeg")
    # saa
    if missing["saa"] and not clone_saa(saa_path): still["saa"] = True
    return not any(still.values()), still

def prompt_install(missing, saa_path="./social-auto-upload"):
    """交互式：检测 -> 提示 -> 安装 -> 再次检测"""
    if not any(missing.values()):
        ok("所有依赖已就绪！")
        return True
    print(f"\n{C('>>> 发现缺失依赖:', 'yellow')}")
    for name in missing["commands"]: warn(f"命令: {name}")
    for name in missing["pip"]: warn(f"pip 包: {name}")
    if missing["f2"]: warn("工具: f2")
    if missing["browser"]: warn("浏览器: playwright chromium")
    if missing["saa"]: warn("项目: social-auto-upload")
    print()
    answer = input(C("  是否自动安装缺失项？ [Y/n]: ", "cyan")).strip().lower()
    if answer and answer not in ("y", "yes", "是", "1"):
        warn("跳过自动安装，部分功能可能不可用")
        return False
    success, still = install_all(missing, saa_path)
    if success:
        ok("所有依赖安装完成！")
        return True
    print(f"\n{C('>>> 安装后仍缺失:', 'red')}")
    for name in still["commands"]: err(f"命令: {name}")
    for name in still["pip"]: err(f"pip 包: {name}")
    if still["f2"]: err("工具: f2")
    if still["browser"]: err("浏览器: playwright chromium")
    if still["saa"]: err("项目: social-auto-upload")
    return False

if __name__ == "__main__":
    results = check_all()
    missing = get_missing(results)
    prompt_install(missing)
