import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from utils.logger import setup_logger

logger = setup_logger("database")


@dataclass
class VideoRecord:
    """视频记录数据类"""
    video_id: str
    source_platform: str
    target_platform: str
    original_url: str
    local_path: Optional[str] = None
    dedup_path: Optional[str] = None
    phash: Optional[str] = None
    status: str = "pending"  # pending/downloaded/deduped/uploaded/failed/rejected
    # rejected = 平台永久拒绝（视频指纹/水印/账号风控），不再进入上传队列
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    uploaded_at: Optional[datetime] = None


class VideoDatabase:
    """SQLite 状态数据库，记录视频全生命周期"""

    def __init__(self, db_path: str = "./state.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """获取线程本地连接"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    @contextmanager
    def _cursor(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    def _init_db(self):
        """初始化数据库表结构"""
        with self._cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS videos (
                    video_id TEXT PRIMARY KEY,
                    source_platform TEXT NOT NULL,
                    target_platform TEXT NOT NULL,
                    original_url TEXT,
                    local_path TEXT,
                    dedup_path TEXT,
                    phash TEXT,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    uploaded_at TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_status ON videos(status)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_source ON videos(source_platform)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    run_end TIMESTAMP,
                    direction TEXT,
                    batch_size INTEGER,
                    downloaded INTEGER DEFAULT 0,
                    deduped INTEGER DEFAULT 0,
                    uploaded INTEGER DEFAULT 0,
                    failed INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running'
                )
                """
            )
        logger.info("Database initialized: %s", self.db_path)

    # ------------------------------------------------------------------
    # Video CRUD
    # ------------------------------------------------------------------

    def insert_or_ignore(
        self,
        video_id: str,
        source_platform: str,
        target_platform: str,
        original_url: str = "",
    ) -> bool:
        """插入新视频记录，如果已存在则跳过。返回 True 表示新插入。"""
        with self._cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO videos (
                        video_id, source_platform, target_platform, original_url, status
                    ) VALUES (?, ?, ?, ?, 'pending')
                    """,
                    (video_id, source_platform, target_platform, original_url),
                )
                logger.debug("Inserted new video record: %s", video_id)
                return True
            except sqlite3.IntegrityError:
                logger.debug("Video already exists, skipping: %s", video_id)
                return False

    def exists(self, video_id: str) -> bool:
        """检查视频是否已存在于数据库"""
        with self._cursor() as cur:
            cur.execute("SELECT 1 FROM videos WHERE video_id = ?", (video_id,))
            return cur.fetchone() is not None

    def get_by_id(self, video_id: str) -> Optional[VideoRecord]:
        """根据 ID 获取视频记录"""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            row = cur.fetchone()
            return self._row_to_record(row) if row else None

    def update_status(
        self,
        video_id: str,
        status: str,
        local_path: Optional[str] = None,
        dedup_path: Optional[str] = None,
        phash: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        """更新视频状态和相关字段"""
        with self._cursor() as cur:
            fields = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
            values = [status]

            if local_path is not None:
                fields.append("local_path = ?")
                values.append(local_path)
            if dedup_path is not None:
                fields.append("dedup_path = ?")
                values.append(dedup_path)
            if phash is not None:
                fields.append("phash = ?")
                values.append(phash)
            if error_message is not None:
                fields.append("error_message = ?")
                values.append(error_message)
            if status == "uploaded":
                fields.append("uploaded_at = CURRENT_TIMESTAMP")

            values.append(video_id)
            sql = f"UPDATE videos SET {', '.join(fields)} WHERE video_id = ?"
            cur.execute(sql, tuple(values))
            logger.debug("Updated video %s status to %s", video_id, status)

    def get_pending_downloads(self, limit: int = 100) -> List[VideoRecord]:
        """获取待下载的视频列表"""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM videos WHERE status = 'pending' ORDER BY created_at LIMIT ?",
                (limit,),
            )
            return [self._row_to_record(row) for row in cur.fetchall()]

    def get_pending_dedups(self, limit: int = 100) -> List[VideoRecord]:
        """获取待去重的视频列表"""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM videos WHERE status = 'downloaded' ORDER BY updated_at LIMIT ?",
                (limit,),
            )
            return [self._row_to_record(row) for row in cur.fetchall()]

    def get_pending_uploads(self, limit: int = 100) -> List[VideoRecord]:
        """获取待上传的视频列表"""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM videos WHERE status = 'deduped' ORDER BY updated_at LIMIT ?",
                (limit,),
            )
            return [self._row_to_record(row) for row in cur.fetchall()]

    def get_failed_count(self, since_hours: int = 24) -> int:
        """获取最近 N 小时内的失败数量"""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM videos
                WHERE status = 'failed' AND updated_at > datetime('now', '-{} hours')
                """.format(
                    since_hours
                )
            )
            row = cur.fetchone()
            return row[0] if row else 0

    def get_stats(self) -> dict:
        """获取各状态视频数量统计"""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*) FROM videos GROUP BY status
                """
            )
            return {row[0]: row[1] for row in cur.fetchall()}

    def cleanup_old_records(self, retention_days: int):
        """清理超过保留期的已上传记录"""
        if retention_days <= 0:
            return
        cutoff = datetime.now() - timedelta(days=retention_days)
        with self._cursor() as cur:
            cur.execute(
                "DELETE FROM videos WHERE status = 'uploaded' AND uploaded_at < ?",
                (cutoff,),
            )
            deleted = cur.rowcount
            logger.info("Cleaned up %d old uploaded records", deleted)

    # ------------------------------------------------------------------
    # Pipeline run tracking
    # ------------------------------------------------------------------

    def start_run(self, direction: str, batch_size: int) -> int:
        """记录流水线运行开始，返回 run_id"""
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_runs (direction, batch_size, status) VALUES (?, ?, 'running')",
                (direction, batch_size),
            )
            return cur.lastrowid

    def end_run(self, run_id: int, downloaded: int, deduped: int, uploaded: int, failed: int):
        """记录流水线运行结束"""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET run_end = CURRENT_TIMESTAMP,
                    downloaded = ?, deduped = ?, uploaded = ?, failed = ?, status = 'completed'
                WHERE id = ?
                """,
                (downloaded, deduped, uploaded, failed, run_id),
            )

    def get_recent_runs(self, limit: int = 10) -> List[dict]:
        """获取最近运行记录"""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT * FROM pipeline_runs
                ORDER BY run_start DESC LIMIT ?
                """,
                (limit,),
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> VideoRecord:
        return VideoRecord(
            video_id=row["video_id"],
            source_platform=row["source_platform"],
            target_platform=row["target_platform"],
            original_url=row["original_url"] or "",
            local_path=row["local_path"],
            dedup_path=row["dedup_path"],
            phash=row["phash"],
            status=row["status"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            uploaded_at=row["uploaded_at"],
        )
