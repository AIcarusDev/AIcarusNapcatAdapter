import asyncio
import hashlib
import os
import sqlite3
from pathlib import Path

from .config import PROJECT_ROOT
from .logger import logger


class MediaCacheManager:
    """负责管理Adapter本地的媒体文件缓存和哈希数据库."""

    def __init__(self) -> None:
        self.cache_dir = Path(PROJECT_ROOT) / "cache"
        self.images_dir = self.cache_dir / "images"
        self.db_path = self.cache_dir / "media_hashes.sqlite"
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """初始化数据库和目录，应在Adapter启动时调用."""
        async with self._lock:
            self.cache_dir.mkdir(exist_ok=True)
            self.images_dir.mkdir(exist_ok=True)

            # 使用 to_thread 避免阻塞事件循环
            await asyncio.to_thread(self._init_db)
            logger.info(f"媒体缓存管理器已初始化。数据库: {self.db_path}")

    def _init_db(self) -> None:
        """同步方法：初始化SQLite数据库表."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS media_cache (
                    hash TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    mime_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    async def check_hash_exists(self, content_hash: str) -> bool:
        """检查一个哈希是否存在于数据库中."""
        async with self._lock:
            return await asyncio.to_thread(self._check_hash_sync, content_hash)

    def _check_hash_sync(self, content_hash: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM media_cache WHERE hash = ?", (content_hash,))
            return cursor.fetchone() is not None

    async def get_file_path_by_hash(self, content_hash: str) -> str | None:
        """通过哈希获取文件路径."""
        async with self._lock:
            return await asyncio.to_thread(self._get_file_path_sync, content_hash)

    def _get_file_path_sync(self, content_hash: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT file_path FROM media_cache WHERE hash = ?", (content_hash,))
            result = cursor.fetchone()
            return result[0] if result else None

    async def save_media(self, content_bytes: bytes, mime_type: str) -> tuple[str, str]:
        """保存媒体文件，计算哈希，并存入数据库。返回 (哈希, 文件路径)."""
        content_hash = hashlib.sha256(content_bytes).hexdigest()

        # 使用哈希的前两位作为子目录，避免单个文件夹内文件过多
        sub_dir = self.images_dir / content_hash[:2]
        sub_dir.mkdir(exist_ok=True)

        # 使用哈希作为文件名
        file_path = sub_dir / content_hash

        async with self._lock:
            await asyncio.to_thread(
                self._save_media_sync, content_hash, file_path, content_bytes, mime_type
            )

        return content_hash, str(file_path.resolve())

    def _save_media_sync(
        self, content_hash: str, file_path: str, content_bytes: bytes, mime_type: str
    ) -> None:
        if not os.path.exists(file_path):
            with open(file_path, "wb") as f:
                f.write(content_bytes)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO media_cache (hash, file_path, mime_type) VALUES (?, ?, ?)",
                (content_hash, str(file_path.resolve()), mime_type),
            )
            conn.commit()


# 创建一个全局实例
media_cache_manager = MediaCacheManager()
