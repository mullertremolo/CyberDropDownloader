from __future__ import annotations

import pathlib
from sqlite3 import Row, IntegrityError
from typing import TYPE_CHECKING, Iterable, Any

import aiosqlite
from yarl import URL

from cyberdrop_dl.utils.database.table_definitions import create_history, create_fixed_history
from cyberdrop_dl.utils.utilities import log

if TYPE_CHECKING:
    from cyberdrop_dl.utils.dataclasses.url_objects import MediaItem


async def get_db_path(url: URL, referer: str = "") -> str:
    """Gets the URL path to be put into the DB and checked from the DB"""
    url_path = url.path

    if referer and "e-hentai" in referer:
        url_path = url_path.split('keystamp')[0][:-1]

    if referer and "mediafire" in referer:
        url_path = url.name

    return url_path


async def get_db_domain(domain: str) -> str:
    """Gets the domain to be put into the DB and checked from the DB"""
    if domain in ("img.kiwi", "jpg.church", "jpg.homes", "jpg.fish", "jpg.fishing", "jpg.pet", "jpeg.pet", "jpg1.su",
                "jpg2.su", "jpg3.su"):
        domain = "sharex"
    return domain


class HistoryTable:
    def __init__(self, db_conn: aiosqlite.Connection):
        self.db_conn: aiosqlite.Connection = db_conn
        self.ignore_history: bool = False

    async def startup(self) -> None:
        """Startup process for the HistoryTable"""
        await self.db_conn.execute(create_history)
        await self.db_conn.commit()
        await self.fix_primary_keys()
        await self.add_columns_media()
        await self.fix_bunkr_v4_entries()

    async def check_complete(self, domain: str, url: URL, referer: URL) -> bool:
        """Checks whether an individual file has completed given its domain and url path"""
        if self.ignore_history:
            return False

        domain = await get_db_domain(domain)

        url_path = await get_db_path(url, domain)
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT referer, completed FROM media WHERE domain = ? and url_path = ?""",
                                    (domain, url_path))
        sql_file_check = await result.fetchone()
        if sql_file_check and sql_file_check[1] != 0:
            # Update the referer if it has changed so that check_complete_by_referer can work
            if str(referer) != sql_file_check[0]:
                await cursor.execute("""UPDATE media SET referer = ? WHERE domain = ? and url_path = ?""",
                                    (str(referer), domain, url_path))
                await self.db_conn.commit()
            return True
        return False

    async def check_album(self, domain: str, album_id: str) -> bool | dict[Any, Any]:
        """Checks whether an album has completed given its domain and album id"""
        if self.ignore_history:
            return False

        domain = await get_db_domain(domain)
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT url_path, completed FROM media WHERE domain = ? and album_id = ?""",
                                    (domain, album_id))
        result = await result.fetchall()
        return {row[0]: row[1] for row in result}

    async def set_album_id(self, domain: str, media_item: MediaItem) -> None:
        """Sets an album_id in the database"""
        domain = await get_db_domain(domain)
        url_path = await get_db_path(media_item.url, str(media_item.referer))
        await self.db_conn.execute("""UPDATE media SET album_id = ? WHERE domain = ? and url_path = ?""",
                                (media_item.album_id, domain, url_path))
        await self.db_conn.commit()

    async def check_complete_by_referer(self, domain: str, referer: URL) -> bool:
        """Checks whether an individual file has completed given its domain and url path"""
        if self.ignore_history:
            return False

        domain = await get_db_domain(domain)
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT completed FROM media WHERE domain = ? and referer = ?""",
                                    (domain, str(referer)))
        sql_file_check = await result.fetchone()
        return sql_file_check and sql_file_check[0] != 0

    async def insert_incompleted(self, domain: str, media_item: MediaItem) -> None:
        """Inserts an uncompleted file into the database"""
        domain = await get_db_domain(domain)
        url_path = await get_db_path(media_item.url, str(media_item.referer))
        download_filename = media_item.download_filename if isinstance(media_item.download_filename, str) else ""
        try:
            await self.db_conn.execute(
                """UPDATE media SET domain = ?, album_id = ? WHERE domain = 'no_crawler' and url_path = ? and referer = ?""",
                (domain, media_item.album_id, url_path, str(media_item.referer)))
        except IntegrityError:
            await self.db_conn.execute("""DELETE FROM media WHERE domain = 'no_crawler' and url_path = ?""",
                                    (url_path,))
        await self.db_conn.execute(
            """INSERT OR IGNORE INTO media (domain, url_path, referer, album_id, download_path, download_filename, original_filename, completed, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (domain, url_path, str(media_item.referer), media_item.album_id, str(media_item.download_folder),
            download_filename, media_item.original_filename, 0))
        await self.db_conn.execute("""UPDATE media SET download_filename = ? WHERE domain = ? and url_path = ?""",
                                (download_filename, domain, url_path))
        await self.db_conn.commit()

    async def mark_complete(self, domain: str, media_item: MediaItem) -> None:
        """Mark a download as completed in the database"""
        domain = await get_db_domain(domain)
        url_path = await get_db_path(media_item.url, str(media_item.referer))
        await self.db_conn.execute(
            """UPDATE media SET completed = 1, completed_at = CURRENT_TIMESTAMP WHERE domain = ? and url_path = ?""",
            (domain, url_path))
        await self.db_conn.commit()

    async def add_filesize(self, domain: str, media_item: MediaItem) -> None:
        """add the file size to the db"""
        domain = await get_db_domain(domain)
        url_path = await get_db_path(media_item.url, str(media_item.referer))
        file_size = pathlib.Path(media_item.complete_file).stat().st_size
        await self.db_conn.execute("""UPDATE media SET file_size=? WHERE domain = ? and url_path = ?""",
                                (file_size, domain, url_path))
        await self.db_conn.commit()

    async def check_filename_exists(self, filename: str) -> bool:
        """Checks whether a downloaded filename exists in the database"""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT EXISTS(SELECT 1 FROM media WHERE download_filename = ?)""", (filename,))
        sql_file_check = await result.fetchone()
        return sql_file_check == 1

    async def get_downloaded_filename(self, domain: str, media_item: MediaItem) -> str:
        """Returns the downloaded filename from the database"""
        domain = await get_db_domain(domain)
        url_path = await get_db_path(media_item.url, str(media_item.referer))
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT download_filename FROM media WHERE domain = ? and url_path = ?""",
                                    (domain, url_path))
        sql_file_check = await result.fetchone()
        return sql_file_check[0] if sql_file_check else None

    async def get_failed_items(self) -> Iterable[Row]:
        """Returns a list of failed items"""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute(
            """SELECT referer, download_path,completed_at,created_at FROM media WHERE completed = 0""")
        failed_files = await result.fetchall()
        return failed_files

    async def get_all_items(self, after, before) -> Iterable[Row]:
        """Returns a list of all items"""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""
        SELECT referer, download_path,completed_at,created_at
        FROM media
        WHERE COALESCE(completed_at, '1970-01-01') BETWEEN ? AND ?
        ORDER BY completed_at DESC;""", (after.format("YYYY-MM-DD"), before.format("YYYY-MM-DD")))
        all_files = await result.fetchall()
        return all_files

    async def get_unique_download_paths(self) -> Iterable[Row]:
        """Returns a list of unique download paths"""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT DISTINCT download_path FROM media""")
        all_files = await result.fetchall()
        return all_files

    async def get_all_bunkr_failed(self):
        hash_list = await self.get_all_bunkr_failed_via_hash()
        size_list = await self.get_all_bunkr_failed_via_size()
        return hash_list + size_list

    async def get_all_bunkr_failed_via_size(self) -> Iterable[Row]:
        try:
            """Returns a list of all items"""
            cursor = await self.db_conn.cursor()
            result = await cursor.execute("""
            SELECT referer,download_path,completed_at,created_at
            from media
            where file_size=322509
    ;
            """)
            all_files = await result.fetchall()
            return all_files
        except Exception as e:
            log(f"Error getting bunkr failed via size: {e}", 20)
            return []

    async def get_all_bunkr_failed_via_hash(self) -> Iterable[Row]:
        try:
            """Returns a list of all items"""
            cursor = await self.db_conn.cursor()
            result = await cursor.execute("""
    SELECT m.referer,download_path,completed_at,created_at
    FROM hash h
    INNER JOIN media m ON h.download_filename= m.download_filename
    WHERE h.hash IN (
        '848248acc7a1b72ea1e90430848badcc354aa0988c01da58642cd857cbb44dc4cbf790991434ffbc6ec04e37061ef07d2f56166fc93205efea9c7333742b5e33',
        '4bb7e09f649a22ba7992780deec38ca5963c2528ea9a2f6aa5429e853df7d691a3f694d65ef303e46e8b125bd88a397a8ec2d127fac47b6f92af6c9088b0b95f',
        '5dbbc6c65608369dcb5bc550ac8d72aff84e934304fa3ed5727d87c379fff2dd81c618221a7ad1d3417a36e8a202f1b32f23a3cf334a22b8d253232646697474',
        '6429f8cc13842d2a340aac32f2071580000085de3ca7d1ca8ba5ef0c4f694ea5849044db4ac79770ac36cbdd10ec7c4407fc4d22ea6d413712c23ea54d478000',
        'adc0ee3bd1a7e8466a699e895ed9b8345715c45489142f351fcb0bdd0a5d2962253c001d78c21302af158521a832ab334d385f7d942351d618f59b9872179d45',
        'e14e0c594596fcdb83cb8e64a8403f09a52bc1d1e2c0cdf4eb0317ca9fe5c012bf8328ee02ce033b01a802c83b61b945fbea2af8be615b3bf713b9b6080a102b',
        '6f3c5fb74a9e3a60b8f7150e2139341af06019a28f4415d55e19a2d9b2c7706375f880b676b7f5b1cddda673231c795245ccb2eb588dd09aaa0112bafe3627a4',
        'cde6f84226773d68d4e4f95d6faa99845a5088ea0ec0e41e5ff58f3262bce1bb727098a58b52a3bb67355e12c9012fcc671ba28c0a1773c0e455a57e9a1b2854',
        '1fa8256928d10dec6bc3f94e9c337c3d723b47a7e658ab492e3a27622bd7acb26a8dbf14526a31a865fb3405063ff9d9bdc3c5fd27f479d32f80e9c9ed6d2e7d'
    );
            """)
            all_files = await result.fetchall()
            return all_files
        except Exception as e:
            log(f"Error getting bunkr failed via hash: {e}", 20)
            return []

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def fix_bunkr_v4_entries(self) -> None:
        """Fixes bunkr v4 entries in the database"""
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""SELECT * from media WHERE domain = 'bunkr' and completed = 1""")
        bunkr_entries = await result.fetchall()

        for entry in bunkr_entries:
            entry = list(entry)
            entry[0] = "bunkrr"
            await self.db_conn.execute("""INSERT or REPLACE INTO media VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)""",
                                    entry)
        await self.db_conn.commit()

        await self.db_conn.execute("""DELETE FROM media WHERE domain = 'bunkr'""")
        await self.db_conn.commit()

    async def fix_primary_keys(self) -> None:
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""pragma table_info(media)""")
        result = await result.fetchall()
        if result[0][5] == 0:
            print("Fixing primary keys in the database: DO NOT EXIT THE PROGRAM")
            await self.db_conn.execute(create_fixed_history)
            await self.db_conn.commit()

            await self.db_conn.execute(
                """INSERT INTO media_copy (domain, url_path, referer, download_path, download_filename, original_filename, completed) SELECT * FROM media GROUP BY domain, url_path, original_filename;""")
            await self.db_conn.commit()

            await self.db_conn.execute("""DROP TABLE media""")
            await self.db_conn.commit()

            await self.db_conn.execute("""ALTER TABLE media_copy RENAME TO media""")
            await self.db_conn.commit()

    async def add_columns_media(self) -> None:
        cursor = await self.db_conn.cursor()
        result = await cursor.execute("""pragma table_info(media)""")
        result = await result.fetchall()
        current_cols = [col[1] for col in result]

        if "album_id" not in current_cols:
            await self.db_conn.execute("""ALTER TABLE media ADD COLUMN album_id TEXT""")
            await self.db_conn.commit()

        if "created_at" not in current_cols:
            await self.db_conn.execute("""ALTER TABLE media ADD COLUMN created_at TIMESTAMP""")
            await self.db_conn.commit()

        if "completed_at" not in current_cols:
            await self.db_conn.execute("""ALTER TABLE media ADD COLUMN completed_at TIMESTAMP""")
            await self.db_conn.commit()

        if "file_size" not in current_cols:
            await self.db_conn.execute("""ALTER TABLE media ADD COLUMN file_size INT""")
            await self.db_conn.commit()
