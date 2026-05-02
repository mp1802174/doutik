#!/usr/bin/env python3
"""
TikTok 登录 & 上传帮助脚本（增强版）
- 自动关闭版权通知弹窗 / TUXModal / joyride 引导层
- 支持多次重试点击编辑器（处理浮层遮挡）
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from conf import LOCAL_CHROME_HEADLESS, LOCAL_CHROME_PATH  # noqa
from uploader.tk_uploader.main_chrome import TiktokVideo, cookie_auth  # noqa
from uploader.tk_uploader.tk_config import Tk_Locator  # noqa
from utils.base_social_media import set_init_script  # noqa
from playwright.async_api import Playwright, async_playwright  # noqa


# ─────────────────────────────────────────────────────────────────────
# 弹窗关闭辅助
# ─────────────────────────────────────────────────────────────────────

async def dismiss_overlays(page, retries: int = 3):
    """
    关闭 TikTok 上传页面可能出现的各类遮罩/弹窗。
    核心策略：优先真正点击弹窗按钮关闭（如 "Turn on" / "Cancel"），
    而非暴力移除 DOM 节点——后者会导致 TikTok 前端状态异常，阻止 Publish。
    """
    for _ in range(retries):
        dismissed = False

        # ── 策略 1: Playwright locator 点击弹窗按钮 ────────────────────
        # 不用 :visible 伪类——它在这类动态弹窗上极不可靠
        button_selectors = [
            # ★ 版权自动检查弹窗（最高优先级）
            ('[class*="TUXModal"] button >> text=Turn on', 'turn_on'),
            ('[class*="TUXModal"] button >> text=Cancel',  'cancel'),
            # 常见关闭/确认按钮
            ('button >> text=Got it',     'got_it'),
            ('button >> text=Allow',      'allow'),
            ('button >> text=Accept',     'accept'),
            ('button >> text=OK',         'ok'),
            ('button >> text=Continue',   'continue'),
            # 不加 Confirm/Discard：这两个是"取消上传"确认框上的危险按钮
            ('button[aria-label="Close"]', 'close'),
            ('button[data-action="skip"]','skip'),
            ('button[data-action="close"]','close_action'),
        ]
        for sel, label in button_selectors:
            try:
                loc = page.locator(sel)
                n = await loc.count()
                if n > 0:
                    # 直接 click()，若被 overlay 拦截会抛异常，下面再 fallback
                    await loc.first.click(timeout=3000)
                    print(f"[dismiss] clicked '{label}' via locator")
                    dismissed = True
                    await page.wait_for_timeout(600)
                    break
            except Exception:
                pass
        if dismissed:
            continue  # 重新扫描一轮，可能还有别的弹窗

        # ── 策略 2: JS 直接扫描并点击按钮（绕过 Playwright overlay 检测） ──
        try:
            clicked_label = await page.evaluate("""
                () => {
                    const btns = Array.from(document.querySelectorAll('button, [role="button"]'));
                    // 注意：不要包含 'Cancel' / 'Confirm'，避免误点上传进度条的 Cancel 按钮导致取消上传
                    const texts = ['Turn on','Got it','Allow','Accept','OK','Continue'];
                    for (const t of texts) {
                        const btn = btns.find(b => {
                            const txt = (b.innerText || b.textContent || '').trim();
                            return txt === t || txt.startsWith(t + '\\n');
                        });
                        if (btn) { btn.click(); return t; }
                    }
                    return null;
                }
            """)
            if clicked_label:
                print(f"[dismiss] JS clicked '{clicked_label}' button")
                dismissed = True
                await page.wait_for_timeout(600)
                continue
        except Exception:
            pass

        # ── 策略 3: 按 Escape ─────────────────────────────────────────
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)

        # ── 策略 4: 最后手段——JS 移除残余 overlay 节点 ────────────────
        # 仅用于 joyride/floating-ui 等非关键弹窗；TUXModal 应优先策略 1/2 关闭
        try:
            removed = await page.evaluate("""
                () => {
                    let count = 0;
                    document.querySelectorAll('[class*="react-joyride__overlay"]').forEach(el => {
                        el.remove(); count++;
                    });
                    document.querySelectorAll('[data-floating-ui-portal]').forEach(el => {
                        el.remove(); count++;
                    });
                    // 仅当没有检测到 TUXModal 内容时才移除 TUXModal overlay
                    const hasModalContent = document.querySelector('[class*="TUXModal"] button');
                    if (!hasModalContent) {
                        document.querySelectorAll('[class*="TUXModal-overlay"]').forEach(el => {
                            el.remove(); count++;
                        });
                    }
                    return count;
                }
            """)
            if removed > 0:
                print(f"[dismiss] removed {removed} non-critical overlay nodes")
                dismissed = True
                await page.wait_for_timeout(300)
        except Exception:
            pass

        if not dismissed:
            break  # 本轮没有任何操作，退出
        await page.wait_for_timeout(400)


# ─────────────────────────────────────────────────────────────────────
# 增强版 TiktokVideo（自动处理弹窗）
# ─────────────────────────────────────────────────────────────────────

class RobustTiktokVideo(TiktokVideo):
    """覆盖 upload 方法，在 add_title_tags 前自动关闭弹窗"""

    async def upload(self, playwright: Playwright) -> None:
        # ★ 使用持久化 Chrome Profile，让 WASM/JS 缓存跨次保留，加快上传速度
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=r"D:\XZ\doutik\chrome_profile",
            headless=False,
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            args=["--disable-blink-features=AutomationControlled"],
            proxy={"server": "http://127.0.0.1:7897"},
        )
        # 从 account_file 注入 cookies（launch_persistent_context 不支持 storage_state 参数）
        _af = Path(self.account_file)
        if _af.exists():
            import json as _json
            state = _json.loads(_af.read_text(encoding="utf-8"))
            cookies = state.get("cookies", [])
            if cookies:
                await context.add_cookies(cookies)
        # ★ 加载 stealth 反检测脚本
        context = await set_init_script(context)
        context.set_default_navigation_timeout(120000)
        context.set_default_timeout(60000)
        page = await context.new_page()

        # 切换语言（来自原始实现）
        await self.change_language(page)

        await page.goto("https://www.tiktok.com/tiktokstudio/upload")
        print(f"[tiktok_helper] 上传页面加载: {self.title}")

        await page.wait_for_url(
            "https://www.tiktok.com/tiktokstudio/upload", timeout=60000
        )

        # 等待页面充分渲染
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            print("[tiktok_helper] WARNING: networkidle 超时，继续尝试")

        try:
            await page.wait_for_selector(
                'iframe[data-tt="Upload_index_iframe"], div.upload-container',
                timeout=12000,
            )
        except Exception:
            print("[tiktok_helper] WARNING: iframe/container 未出现，继续尝试")

        await self.choose_base_locator(page)

        # 选择视频文件（增加超时，VPS 网络可能较慢）
        # 注意：TikTok 上传按钮可能需要 10-15s 才完全渲染
        upload_button = self.locator_base.locator(
            'button:has-text("Select video")'
        )
        try:
            await upload_button.wait_for(state="visible", timeout=120000)
        except Exception:
            # 备用：直接在 page 级别查找
            print("[tiktok_helper] locator_base 未找到 Select video，尝试 page 级别")
            upload_button = page.locator('button:has-text("Select video")')
            await upload_button.wait_for(state="visible", timeout=60000)

        async with page.expect_file_chooser() as fc_info:
            await upload_button.click()
        file_chooser = await fc_info.value
        await file_chooser.set_files(str(self.file_path))
        print(f"[tiktok_helper] 视频已选择，等待上传界面就绪...")

        # ★ 等待并关闭所有弹窗
        await page.wait_for_timeout(3000)
        await dismiss_overlays(page, retries=4)

        # ★ 填写标题+标签（带重试）
        await self._robust_add_title_tags(page)

        # 等待视频上传完成
        await self.detect_upload_status(page)

        # ★ 检测 TikTok 拒绝错误（视频指纹/水印命中、账号风控等）
        # 错误以 toast 形式弹出，会自动消失再循环弹出，所以必须轮询检测
        # 文案: "Something went wrong. You can try again or replace it with a different video"
        rejected = await self._poll_for_rejection(page, total_seconds=12)
        if rejected:
            print(
                "[tiktok_helper] REJECTED by TikTok: "
                "Something went wrong (video rejected, will not retry)",
                file=sys.stderr,
            )
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass
            sys.exit(2)  # 退出码 2 = 平台拒绝（永久）

        if self.thumbnail_path:
            await self.upload_thumbnails(page)

        if self.publish_date != 0:
            await self.set_schedule_time(page, self.publish_date)

        # ★ 点击发布前先关闭可能遮挡的弹窗（TUXModal 等）
        await dismiss_overlays(page, retries=3)
        await self._robust_click_publish(page)

        # 获取发布后的视频 ID
        try:
            vid = await self.get_last_video_id(page)
            print(f"[tiktok_helper] ✅ 发布成功! video_id={vid}")
        except Exception:
            print("[tiktok_helper] ✅ 发布成功!")

        await context.storage_state(path=str(self.account_file))
        print("[tiktok_helper] Cookie 已更新")
        await asyncio.sleep(2)
        await context.close()

    async def detect_upload_status(self, page):
        """等待 TikTok 显示 '已上传' / 'Uploaded' 文字，这是上传真正完成的标志。最多等 300 秒。"""
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 600  # 最多等 10 分钟（首次需下载 WASM 模块）
        while loop.time() < deadline:
            try:
                txt = await self.locator_base.evaluate(
                    "el => el.innerText", timeout=3000
                )
                # 中文：已上传   英文：Uploaded
                if "已上传" in txt or "Uploaded" in txt:
                    print("[tiktok_helper] ✅ video uploaded (detected)")
                    return
                remaining = int(deadline - loop.time())
                if remaining % 30 == 0:
                    print(f"[tiktok_helper] 等待上传完成... 剩余 {remaining}s | iframe: {txt[:120]!r}")
                if await self.locator_base.locator('button[aria-label="Select file"]').count():
                    print("[tiktok_helper] 检测到上传错误，尝试重试...")
                    await self.handle_upload_error(page)
            except Exception as e:
                print(f"[tiktok_helper] 检测上传状态: {e!s:.60}")
            await asyncio.sleep(2)
        print("[tiktok_helper] WARNING: 视频上传检测超时（300s），强制继续...")

    async def _poll_for_rejection(self, page, total_seconds: int = 12) -> bool:
        """
        TikTok 拒绝提示是 toast 形式：弹出 → 消失 → 再次弹出，循环数秒。
        单次 is_visible() 检查极易错过，需在一段时间窗口内高频轮询。
        同时通过 JS 直接搜整页 textContent，绕过 toast 出现/消失的时机问题。
        """
        deadline = total_seconds * 10  # 100ms 一次，共 total_seconds 秒
        for i in range(deadline):
            try:
                # 方法 1: locator 实时 visible 检查
                err_loc = page.locator(
                    'text=/Something went wrong.*replace it with/i'
                ).first
                if await err_loc.count():
                    try:
                        if await err_loc.is_visible():
                            return True
                    except Exception:
                        pass

                # 方法 2: 直接 JS 搜索整页文本（即使 toast 消失，也可能在 DOM 中残留毫秒）
                found = await page.evaluate(
                    """() => {
                        const txt = document.body && document.body.innerText || '';
                        return /Something went wrong[\\s\\S]*replace it with/i.test(txt);
                    }"""
                )
                if found:
                    return True

                # 方法 3: 检查 TikTok 常见 toast 容器
                toast_selectors = [
                    '[class*="Toast"]',
                    '[class*="toast"]',
                    '[role="alert"]',
                    '[class*="Notice"]',
                ]
                for sel in toast_selectors:
                    try:
                        n = await page.locator(sel).count()
                        for j in range(n):
                            text = await page.locator(sel).nth(j).inner_text(timeout=200)
                            if text and re.search(
                                r'something went wrong.*replace it with', text, re.I,
                            ):
                                return True
                    except Exception:
                        pass
            except Exception:
                pass

            await page.wait_for_timeout(100)
        return False

    async def _robust_click_publish(self, page):
        """带弹窗处理的发布点击：TUXModal 等弹窗会遮挡 Publish 按钮"""
        # 先关闭遮挡弹窗
        await dismiss_overlays(page, retries=3)
        await page.wait_for_timeout(500)

        # 诊断：打印 button-group 里所有按钮文字
        try:
            all_btns = self.locator_base.locator('div.button-group button')
            cnt = await all_btns.count()
            print(f"[tiktok_helper] button-group 按钮数: {cnt}")
            for i in range(cnt):
                txt = await all_btns.nth(i).inner_text(timeout=2000)
                dis = await all_btns.nth(i).get_attribute("disabled", timeout=2000)
                print(f"[tiktok_helper]   btn[{i}] text={txt!r} disabled={dis!r}")
        except Exception as e:
            print(f"[tiktok_helper] 诊断异常: {e!s:.80}")

        # 按文本匹配 Post 按钮，避免选到 "Save draft"
        publish_button = self.locator_base.locator('div.button-group button:has-text("Post")')

        # 尝试 1：force click
        try:
            btn_cnt = await publish_button.count()
            print(f"[tiktok_helper] Post 按钮匹配数: {btn_cnt}")
            if btn_cnt:
                await publish_button.click(force=True, timeout=5000)
                print("[tiktok_helper] Publish 按钮 force click")
        except Exception as e:
            print(f"[tiktok_helper] 发布点击异常: {e!s:.80}")

        # 点击后等 3 秒，读取页面文本诊断
        await asyncio.sleep(3)
        try:
            page_text = await page.evaluate("() => document.body ? document.body.innerText.slice(0, 500) : ''")
            print(f"[tiktok_helper] 点击后页面文本(前500): {page_text!r}")
        except Exception as e:
            print(f"[tiktok_helper] 读取页面文本失败: {e!s:.60}")
        try:
            iframe_text = await self.locator_base.evaluate("el => el.innerText.slice(0, 500)")
            print(f"[tiktok_helper] 点击后iframe文本(前500): {iframe_text!r}")
        except Exception as e:
            print(f"[tiktok_helper] 读取iframe文本失败: {e!s:.60}")

        # 等待页面离开上传页，或出现发布成功提示（最多 5 分钟）
        publish_ok = await self._wait_for_publish_success(page, timeout=300)
        if publish_ok:
            return

        # 尝试 2：JS 直接点击后再等
        await dismiss_overlays(page, retries=2)
        try:
            await page.evaluate("""
                const btn = document.querySelector('div.button-group button[data-e2e="post_video_button"]');
                if (btn) btn.click();
            """)
            print("[tiktok_helper] JS 直接点击 Publish")
        except Exception as e:
            print(f"[tiktok_helper] JS 点击异常: {e!s:.80}", file=sys.stderr)

        publish_ok = await self._wait_for_publish_success(page, timeout=300)
        if publish_ok:
            return

        # ★ 所有发布尝试均失败，退出码 1
        print(
            "[tiktok_helper] ERROR: 所有 Publish 尝试均失败，视频未发布到 TikTok",
            file=sys.stderr,
        )
        sys.exit(1)

    async def _wait_for_publish_success(self, page, timeout: int = 300) -> bool:
        """等待发布成功：离开上传页 OR 出现成功 toast。返回 True=成功"""
        start = asyncio.get_event_loop().time()
        last_log = start
        while asyncio.get_event_loop().time() - start < timeout:
            url = page.url
            now = asyncio.get_event_loop().time()
            # 每 15 秒打印一次当前 URL
            if now - last_log >= 15:
                print(f"[tiktok_helper] 等待发布... URL={url}")
                last_log = now
            # 已离开上传页，说明发布成功
            if "tiktokstudio/upload" not in url:
                print(f"[tiktok_helper] ✅ 视频发布成功，当前页: {url}")
                return True
            # 检测成功 toast / 提示文字
            try:
                found = await page.evaluate("""
                    () => {
                        const txt = document.body ? document.body.innerText : '';
                        return /successfully posted|video posted|post scheduled|Your video has been/i.test(txt);
                    }
                """)
                if found:
                    print("[tiktok_helper] ✅ 检测到发布成功提示")
                    return True
            except Exception:
                pass
            await asyncio.sleep(2)
        print(f"[tiktok_helper] 等待发布成功超时 ({timeout}s)，最终 URL={page.url}")
        return False

    async def _robust_add_title_tags(self, page):
        """带弹窗重试的标题/标签填写"""
        editor_sel = "div.public-DraftEditor-content"
        editor = self.locator_base.locator(editor_sel)

        # 最多尝试 4 次（每次先关弹窗）
        for attempt in range(4):
            try:
                await editor.wait_for(state="visible", timeout=5000)
                await editor.click(timeout=5000)
                print(f"[tiktok_helper] 编辑器点击成功（第{attempt+1}次）")
                break
            except Exception as e:
                print(f"[tiktok_helper] 编辑器点击失败（第{attempt+1}次）: {e!s:.80}")
                await dismiss_overlays(page, retries=3)
        else:
            # 终极方案：JS 点击
            print("[tiktok_helper] 使用 JS 强制点击编辑器")
            await page.evaluate(
                f"document.querySelector('{editor_sel}')?.click()"
            )
            await page.wait_for_timeout(500)

        # 填写标题
        await page.keyboard.press("End")
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Delete")
        await page.keyboard.press("End")
        await page.wait_for_timeout(800)
        await page.keyboard.insert_text(self.title)
        await page.wait_for_timeout(800)
        await page.keyboard.press("End")
        await page.keyboard.press("Enter")

        # 填写 Tags
        for idx, tag in enumerate(self.tags, 1):
            print(f"[tiktok_helper] 设置标签 {idx}: #{tag}")
            await page.keyboard.press("End")
            await page.wait_for_timeout(800)
            await page.keyboard.insert_text(f"#{tag} ")
            await page.keyboard.press("Space")
            await page.wait_for_timeout(800)
            await page.keyboard.press("Backspace")
            await page.keyboard.press("End")

    async def main(self):
        async with async_playwright() as playwright:
            await self.upload(playwright)


# ─────────────────────────────────────────────────────────────────────
# 登录
# ─────────────────────────────────────────────────────────────────────

async def do_login(account_file: str) -> None:
    account_path = Path(account_file)
    account_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[tiktok_helper] 打开浏览器进行 TikTok 登录...")
    print(f"[tiktok_helper] Cookie 将保存到: {account_file}")
    print("[tiktok_helper] 使用代理: http://127.0.0.1:7897 (美国 IP)")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context(
            proxy={"server": "http://127.0.0.1:7897"},
        )
        context = await set_init_script(context)
        context.set_default_navigation_timeout(120000)
        context.set_default_timeout(60000)
        page = await context.new_page()
        await page.goto("https://www.tiktok.com/login?lang=en")
        print("[tiktok_helper] 请在浏览器中登录，完成后关闭浏览器或按 Ctrl+C")
        await page.pause()
        await context.storage_state(path=account_file)
        await context.close()
        await browser.close()

    print(f"[tiktok_helper] 登录完成，Cookie 已保存: {account_file}")


async def do_upload(
    account_file: str,
    video_path: str,
    title: str,
    tags: list[str],
) -> None:
    # ★ 固定标题和标签，忽略 CLI 传入的值
    FIXED_TITLE = "中国街拍美女"
    FIXED_TAGS = ["dance", "beautiful girl"]
    print(f"[tiktok_helper] 检查 Cookie 有效性...")
    if not Path(account_file).exists():
        print(f"[tiktok_helper] ERROR: Cookie 文件不存在: {account_file}", file=sys.stderr)
        sys.exit(1)

    # cookie_auth 已单独验证，此处跳过（networkidle 超时问题）
    if False and not await cookie_auth(account_file):
        print(f"[tiktok_helper] ERROR: Cookie 已过期: {account_file}", file=sys.stderr)
        sys.exit(1)

    print(f"[tiktok_helper] 开始上传: {video_path}")
    print(f"[tiktok_helper] 标题(固定): {FIXED_TITLE}")
    print(f"[tiktok_helper] 标签(固定): {FIXED_TAGS}")

    app = RobustTiktokVideo(
        title=FIXED_TITLE,
        file_path=video_path,
        tags=FIXED_TAGS,
        publish_date=0,
        account_file=account_file,
    )
    await app.main()
    print(f"[tiktok_helper] 上传流程完成: {video_path}")


# ─────────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(prog="tiktok_helper")
    sub = parser.add_subparsers(dest="action", required=True)

    login_p = sub.add_parser("login")
    login_p.add_argument("--account-file", required=True)

    check_p = sub.add_parser("check")
    check_p.add_argument("--account-file", required=True)

    up_p = sub.add_parser("upload")
    up_p.add_argument("--account-file", required=True)
    up_p.add_argument("--file", required=True)
    up_p.add_argument("--title", required=True)
    up_p.add_argument("--tags", default="")

    args = parser.parse_args()

    if args.action == "login":
        asyncio.run(do_login(args.account_file))

    elif args.action == "check":
        if not Path(args.account_file).exists():
            print("invalid"); sys.exit(1)
        valid = asyncio.run(cookie_auth(args.account_file))
        print("valid" if valid else "invalid")
        sys.exit(0 if valid else 1)

    elif args.action == "upload":
        tags = [t.strip() for t in args.tags.split(",") if t.strip()]
        asyncio.run(do_upload(args.account_file, args.file, args.title, tags))


if __name__ == "__main__":
    main()
