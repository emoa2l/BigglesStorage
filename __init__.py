import redis
import json
import logging

class BigglesStorage:

    def __init__(self, endpoint, port=6379, db=0):
        self.redis = redis.Redis(host=endpoint, port=port, db=db, decode_responses=True, logger=None)
        
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def store(self, key, data):
        if isinstance(data, str):
            self.logger.debug(f"storing string data for key: {key}")
            self.redis.set(key, data)
        else:
            self.logger.debug(f"storing object data for key: {key}")
            self.redis.set(key, json.dumps(data))

    def get(self, key):
        data = self.redis.get(key)

        if data:
            self.logger.debug(f"Data found for key: {key}")
            return data
        else:
            self.logger.debug(f"No data found for key: {key}")
            return None
