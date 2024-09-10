import os  # Import os for file path manipulation

import aiofiles

from cyberdrop_dl.clients.hash_client import HashClient


class HashManager:
    def __init__(self, manager):
        self.hasher = self._get_hasher()  # Initialize hasher in constructor
        self.hash_client = HashClient(manager)  # Initialize hash client in constructor

    async def startup(self):
        await self.hash_client.startup()

    def _get_hasher(self):
        """This is different from upstream which uses xxhash if available and falls back to md5"""
        import hashlib
        return hashlib.blake2b

    async def hash_file(self, filename):
        file_path = os.path.join(os.getcwd(), filename)  # Construct full file path
        async with aiofiles.open(file_path, "rb") as fp:
            CHUNK_SIZE = 1024 * 1024  # 1 mb
            filedata = await fp.read(CHUNK_SIZE)
            hasher = self.hasher()  # Use the initialized hasher
            while filedata:
                hasher.update(filedata)
                filedata = await fp.read(CHUNK_SIZE)
            return hasher.hexdigest()
