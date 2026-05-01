import asyncio
import random
import time
from pathlib import Path

from conf import BASE_DIR
from tiktok_helper import do_upload


if __name__ == '__main__':
    filepath = Path(BASE_DIR) / "videos"
    account_file = Path(BASE_DIR / "cookies" / "tiktok_default.json")
    folder_path = Path(filepath)

    # 每次运行最多上传10个视频
    MAX_UPLOAD_PER_RUN = 10
    files = sorted(list(folder_path.glob("*.mp4")))[:MAX_UPLOAD_PER_RUN]
    print(f"[main] 本次将上传 {len(files)} 个视频 (最多 {MAX_UPLOAD_PER_RUN} 个)")

    for index, file in enumerate(files):
        print(f"\n[main] ===== 开始上传第 {index + 1}/{len(files)} 个视频 =====")
        print(f"[main] video_file_name：{file}")

        try:
            asyncio.run(do_upload(str(account_file), str(file), "", []))
            print(f"[main] ✅ 第 {index + 1} 个视频上传完成")
        except Exception as e:
            print(f"[main] ❌ 第 {index + 1} 个视频上传失败: {e}")

        # 上传间隔 5-10 分钟随机
        if index < len(files) - 1:
            wait_seconds = random.randint(300, 600)
            minutes = wait_seconds // 60
            print(f"[main] ⏳ 等待 {minutes} 分钟后上传下一个视频...")
            time.sleep(wait_seconds)

    print(f"\n[main] ===== 本次上传流程结束，共处理 {len(files)} 个视频 =====")
