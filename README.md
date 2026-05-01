# Douyin ↔ TikTok 自动搬运流水线

基于 `video-mover` 的 ffmpeg 去重策略 + `social-auto-upload` 的上传能力 + `f2` 的下载能力，实现全自动、双向、批量视频搬运。

---

## 特性

- **双向搬运**：支持 抖音→TikTok，也可配置为 TikTok→抖音
- **批量处理**：定时下载一批 → 批量去重 → 批量上传
- **防重复**：SQLite 数据库记录所有视频生命周期，已处理视频永不重复下载/上传
- **视频去重**：水平镜像、轻微旋转、边缘裁剪、颜色微调、淡入淡出（ffmpeg 轻量实现）
- **定时调度**：apscheduler 后台循环，间隔可配置
- **资源适配**：以 2C4G Windows 为主目标环境编写，同时兼容低配置服务器（ffmpeg `ultrafast` 预设）
- **完善日志**：终端 + 文件双输出，轮转保留 5 份备份

---

## 环境要求

| 依赖 | 版本 | 说明 |
|------|------|------|
| Python | 3.9+ | |
| ffmpeg | 任意 | 去重处理必需 |
| f2 | 最新 | JoeanAmier/TikTokDownloader，下载必需 |
| social-auto-upload | 最新 | dreammis/social-auto-upload，上传必需 |

### 安装步骤（Windows 2C4G）

```powershell
# 1. 克隆本项目
cd C:\your\path
git clone <本项目仓库>
cd douyin-tiktok-pipeline

# 2. 安装 Python 依赖
pip install -r requirements.txt

# 3. 启动配置向导（自动检测并安装 ffmpeg / f2 / playwright 等依赖）
python setup_wizard.py
```

> **Linux/VPS 环境**：步骤相同，`setup_wizard.py` 会自动适配系统（如 `sudo apt install ffmpeg`）。
> 如果自动安装失败，可手动安装后重试：
> ```bash
> # Debian/Ubuntu 手动安装 ffmpeg
> sudo apt install ffmpeg
> # 手动安装 pip 包
> pip install -r requirements.txt
> # 手动安装 playwright 浏览器
> playwright install chromium
> # 手动 clone social-auto-upload
> git clone https://github.com/dreammis/social-auto-upload.git
> ```

---

## 快速开始

### 推荐方式：交互式配置向导（一键完成所有配置）

```bash
python setup_wizard.py
```

向导会自动：
- 检测环境（ffmpeg、f2、Python 包等）
- 引导填写所有参数（方向、URL、去重强度、Cookie）
- 自动生成 `config.yaml` 和 `my_apps.yaml`
- 创建必要的目录
- 提示下一步操作

---

### 手动方式：编辑配置文件

如需手动配置，复制 `config.yaml` 并根据你的环境修改：

```yaml
pipeline:
  direction: "douyin_to_tiktok"      # 方向
  batch_size: 5                       # 每批处理个数
  schedule_interval_minutes: 300      # 定时间隔（分钟）
  cleanup_after_upload: true          # 上传后删除本地文件

downloader:
  douyin_user_url: "https://www.douyin.com/user/SEC_USER_ID"  # 替换为真实用户主页
  tiktok_user_url: "https://www.tiktok.com/@xxx"               # 反向搬运时填
  f2_config: "my_apps.yaml"                                    # f2 配置文件

uploader:
  social_auto_upload_path: "./social-auto-upload"  # social-auto-upload 目录
  account_name: "default"
  headless: true                                    # true=无头模式后台运行，false=弹窗浏览器（登录时必须）

deduplicator:
  flip_horizontal: true
  rotation_angle: -2
  crop_percentage: 0.02
  ffmpeg_preset: "ultrafast"                        # 低配置用这个
  ffmpeg_crf: 28
```

### 2. 获取 Cookie（首次必需）

#### 抖音下载 Cookie（f2）

在浏览器登录抖音 → F12 → Network → 任意请求 → 复制 Cookie → 粘贴到 `my_apps.yaml`：

```yaml
douyin:
  headers:
    User-Agent: "Mozilla/5.0 ..."
    Referer: "https://www.douyin.com/"
  cookie: "sessionid=xxx; ttwid=xxx; ..."
```

#### TikTok / 抖音 上传 Cookie（social-auto-upload）

```bash
# 登录 TikTok（会弹窗浏览器，扫码或密码登录）
python main.py login tiktok

# 登录抖音
python main.py login douyin
```

登录后的 Cookie 会自动保存到 `social-auto-upload/cookies/` 目录。

### 3. 运行

```bash
# 立即执行一个批次
python main.py run

# 启动后台定时调度（每 N 分钟自动执行）
python main.py schedule

# 查看已处理视频统计
python main.py stats

# 查看最近运行记录
python main.py history --limit 20
```

---

## 命令行参考

| 命令 | 说明 |
|------|------|
| `python main.py run` | 立即执行一次完整流水线 |
| `python main.py schedule` | 启动后台定时循环 |
| `python main.py login tiktok` | 登录 TikTok 获取上传 Cookie |
| `python main.py login douyin` | 登录抖音获取上传 Cookie |
| `python main.py stats` | 查看数据库状态统计 |
| `python main.py history --limit N` | 查看最近 N 次运行记录 |

---

## 数据库

使用 SQLite `state.db`，记录每条视频的完整生命周期：

- `video_id` — 来源平台视频 ID
- `source_platform` / `target_platform` — 流向
- `local_path` / `dedup_path` — 本地文件路径
- `status` — pending / downloaded / deduped / uploaded / failed
- `created_at` / `updated_at` / `uploaded_at` — 时间戳

**防重复机制**：下载前查询 `video_id`，已存在则跳过。

---

## 去重策略说明

| 变换 | 效果 | CPU 开销 |
|------|------|----------|
| 水平镜像 (`hflip`) | 极高（重构画面方向） | 低 |
| 边缘裁剪 2% (`crop`) | 高（改变画面尺寸和构图） | 中 |
| 颜色微调 (`eq`) | 中（改变像素值） | 低 |
| 轻微旋转 -2° | 高（改变画面边界） | 中 |
| 淡入淡出 (`fade`) | 低（开头结尾微调） | 低 |
| `ultrafast` 预设 | — | 编码极快，画质稍降 |
| CRF 28 | — | 平衡画质与大小 |

**推荐配置（2C4G Windows）**：默认开启 `hflip + crop + eq + fade`，单个 30s 视频处理时间约 10-20 秒。

如需更强去重（帧交换、画中画、背景模糊），可在 `config.yaml` 开启，处理时间会增加但 2C4G 仍可流畅运行。

---

## 常见问题

**Q: 为什么上传需要 headless=false 登录？**
A: TikTok/抖音的反爬检测需要真实浏览器环境完成登录流程。登录一次后 Cookie 长期有效，后续上传可用 headless=true。

**Q: f2 下载失败或 Cookie 失效怎么办？**
A: 重新从浏览器复制最新 Cookie 更新 `my_apps.yaml`。f2 不支持自动 renewal，这是已知限制。

**Q: 上传后视频被平台判重/限流？**
A: 调高去重强度：`rotation_angle` 加大、`crop_percentage` 加大，或开启 `include_hzh` 画中画叠加。

**Q: 代码能在低配置环境运行吗？**
A: 可以。本代码以 2C4G Windows 为主目标编写，但去重模块使用 ffmpeg `ultrafast` 预设，在 1C1G VPS 上处理单个 30s 视频也能在 30 秒内完成。上传模块的 `headless` 选项可控制浏览器内存占用（约 200-400MB），资源受限时可降低 `batch_size`。

**Q: 如何改成 TikTok→抖音？**
A: 改 `config.yaml`：`direction: "tiktok_to_douyin"`，并配置 `tiktok_user_url`。

---

## License

MIT
