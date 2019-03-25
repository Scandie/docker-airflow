# -*- coding: utf-8 -*-
from redis import StrictRedis
from lazydb import Db as LazyDB


class MappingConfigurationException(Exception):
    pass


class MappingInterface(object):
    type = None

    def get(self, key):
        raise NotImplementedError

    def put(self, key, value):
        raise NotImplementedError

    def has(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def is_empty(self):
        raise NotImplementedError

    def clean(self):
        raise NotImplementedError


class LazyDBMapping(MappingInterface):

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.type = 'lazy'
        self.name = self.config.get('name', 'lots_mapping')
        self.db = LazyDB(self.name)
        self.logger.info('Set lazydb "{}" as lots mapping'.format(self.name))
        self._set_value = self.db.put
        self._has_value = self.db.has

    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        self.db.put(key, value)

    def has(self, key):
        return self.db.has(key)

    def delete(self, key):
        self.db.delete(key)

    def is_empty(self):
        return self.db.is_empty()

    def clean(self):
        LazyDB.destroy(self.name)


class RedisMapping(MappingInterface):

    def __init__(self, config, logger):
        self.validate_config(config)

        self.logger = logger
        self.config = config
        self.type = 'redis'

        self.expire_time = self.config.get('expire_time', 600)

        config = {
            'host': self.config.get('host'),
            'port': self.config.get('port') or 6379,
            'db': self.config.get('name') or 0,
            'password': self.config.get('password') or None
        }
        self.db = StrictRedis(**config)

        self._check_redis_connection()

        self.logger.info('Set redis store "{db}" at {host}:{port} '
                         'as lots mapping'.format(**config))

    def _check_redis_connection(self):
        self.db.set('check', 'check')
        self.db.delete('check')

    def get(self, key):
        return bool(self.db.get(key))

    def put(self, key, value):
        self.db.set(key, int(value), ex=self.expire_time)

    def has(self, key):
        return self.db.exists(key)

    def delete(self, key):
        self.db.delete(key)

    def is_empty(self):
        for _ in self.db.scan_iter(count=1):
            return True
        return False

    def clean(self):
        self.db.flushdb()

    def validate_config(self, config):
        if 'host' not in config:
            raise MappingConfigurationException('host should be present for redis type mapping')
        if 'port' not in config:
            raise MappingConfigurationException('port should be present for redis type mapping')


class VoidMapping(MappingInterface):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.type = 'void'
        self.logger.info('Set void mapping. Caching is disabled')

    def get(self, key):
        return None

    def put(self, key, value):
        return None

    def has(self, key):
        return False

    def delete(self, key):
        return None

    def is_empty(self):
        return True

    def clean(self):
        pass


MAPPING_TYPES = {
    'redis': RedisMapping,
    'lazy': LazyDBMapping,
    'void': VoidMapping
}
