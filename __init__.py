import aioredis
import json
import logging

class BigglesStorage:

    def __init__(self, endpoint, port=6379, db=0, logger=None):
        self.endpoint = endpoint
        self.port = port
        self.db = db
        
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    async def _get_redis(self):
        return await aioredis.create_redis_pool((self.endpoint, self.port), db=self.db, encoding='utf-8')

    async def store(self, key, data):
        redis = await self._get_redis()
        if isinstance(data, str):
            self.logger.debug(f"storing string data for key: {key}")
            await redis.set(key, data)
        else:
            self.logger.debug(f"storing object data for key: {key}")
            await redis.set(key, json.dumps(data))
        redis.close()
        await redis.wait_closed()

    async def get(self, key):
        redis = await self._get_redis()
        data = await redis.get(key)

        if data:
            self.logger.debug(f"Data found for key: {key}")
            return data
        else:
            self.logger.debug(f"No data found for key: {key}")
            return None
        redis.close()
        await redis.wait_closed()

