import redis
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, Row
from typing import Dict, Optional, Any
import json

class Redis:
    
    def __init__(self):
        self.redisConnect = None
        self.redisHost = ""
        self.redisPort = 0
        self.redisTimeout = 30000
    
    # Connection management
    def closeRedisConnect(self):
        """Close Redis connection"""
        if self.redisConnect is not None:
            self.redisConnect.close()
            self.redisConnect = None
    
    def getOrCreateRedisConnect(self, host: str, port: int) -> Optional[redis.Redis]:
        """Get or create Redis connection"""
        if self.redisConnect is None:
            self.redisConnect = self.createRedisConnect(host, port)
        elif self.redisHost != host or self.redisPort != port:
            try:
                self.closeRedisConnect()
            except Exception:
                pass
            self.redisConnect = self.createRedisConnect(host, port)
        return self.redisConnect
    
    def getOrCreateRedisConnectFromConf(self, conf) -> Optional[redis.Redis]:
        """Get or create Redis connection from config"""
        return self.getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
    
    def createRedisConnect(self, host: str, port: int) -> Optional[redis.Redis]:
        """Create new Redis connection"""
        self.redisHost = host
        self.redisPort = port
        if host == "":
            return None
        return redis.Redis(host=host, port=port, socket_timeout=self.redisTimeout/1000, decode_responses=True)
    
    def createRedisConnectFromConf(self, conf) -> Optional[redis.Redis]:
        """Create Redis connection from config"""
        return self.createRedisConnect(conf.redisHost, conf.redisPort)

    def normalize_db(self, db):
        """Normalize Redis DB index to an integer."""
        if db is None:
            raise ValueError("Redis db index must be provided")
        try:
            return int(db)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid Redis db index: {db}")
    
    # Get key value
    def get(self, key: str, conf=None, host: str = None, port: int = None, db: int = None) -> str:
        """Get value by key"""
        if conf is not None:
            return self.getWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key)
        elif all(param is not None for param in [host, port, db]):
            return self.getWithParams(host, port, db, key)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
    
    def getWithParams(self, host: str, port: int, db: int, key: str) -> str:
        """Get value with retry logic"""
        try:
            return self.getWithoutRetry(host, port, db, key)
        except redis.RedisError:
            self.redisConnect = self.createRedisConnect(host, port)
            return self.getWithoutRetry(host, port, db, key)
    
    def getWithoutRetry(self, host: str, port: int, db: int, key: str) -> str:
        """Get value without retry"""
        if not key:
            print("WARNING: key is empty")
            return ""
        
        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"WARNING: jedis=None means host is not set, skipping fetching the redis key={key}")
            return ""
        
        # Select database if different
        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        if current_db != db:
            jedis = redis.Redis(host=host, port=port, db=db, socket_timeout=self.redisTimeout/1000, decode_responses=True)
        
        # Check if key exists
        if not jedis.exists(key):
            print(f"WARNING: Key={key} does not exist in Redis")
            return ""
        
        result = jedis.get(key)
        return result if result is not None else ""
    
    # Set key value
    def update(self, key: str, data: str, conf=None, db: int = None, host: str = None, port: int = None):
        """Update key value"""
        if conf is not None and db is not None:
            self.updateWithParams(conf.redisHost, conf.redisPort, db, key, data)
        elif conf is not None:
            self.updateWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
        elif all(param is not None for param in [host, port, db]):
            self.updateWithParams(host, port, db, key, data)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
    
    def updateWithParams(self, host: str, port: int, db: int, key: str, data: str):
        """Update with retry logic"""
        try:
            self.updateWithoutRetry(host, port, db, key, data)
        except redis.RedisError:
            self.redisConnect = self.createRedisConnect(host, port)
            self.updateWithoutRetry(host, port, db, key, data)
    
    def updateWithoutRetry(self, host: str, port: int, db: int, key: str, data: str):
        """Update without retry"""
        cleanedData = "" if data is None or data == "" else data
        if data is None or data == "":
            print(f"WARNING: data is empty, setting data='' for redis key={key}")
        
        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"WARNING: jedis=None means host is not set, skipping saving to redis key={key}")
            return
        
        # Select database if different
        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        if current_db != db:
            jedis = redis.Redis(host=host, port=port, db=db, socket_timeout=self.redisTimeout/1000, decode_responses=True)
        
        jedis.set(key, cleanedData)
    
    # Get map field value
    def getMapField(self, key: str, field: str, conf=None, host: str = None, port: int = None, db: int = None) -> str:
        """Get hash field value"""
        if conf is not None:
            return self.getMapFieldWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key, field)
        elif all(param is not None for param in [host, port, db]):
            return self.getMapFieldWithParams(host, port, db, key, field)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
    
    def getMapFieldWithParams(self, host: str, port: int, db: int, key: str, field: str) -> str:
        """Get map field with retry logic"""
        try:
            return self.getMapFieldWithoutRetry(host, port, db, key, field)
        except redis.RedisError:
            self.redisConnect = self.createRedisConnect(host, port)
            return self.getMapFieldWithoutRetry(host, port, db, key, field)
    
    def getMapFieldWithoutRetry(self, host: str, port: int, db: int, key: str, field: str) -> str:
        """Get map field without retry"""
        if not key:
            print("WARNING: key is empty")
            return ""
        
        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"WARNING: jedis=None means host is not set, skipping fetching the redis key={key}")
            return ""
        
        # Select database if different
        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        if current_db != db:
            jedis = redis.Redis(host=host, port=port, db=db, socket_timeout=self.redisTimeout/1000, decode_responses=True)
        
        # Check if key exists
        if not jedis.exists(key):
            print(f"WARNING: Key={key} does not exist in Redis")
            return ""
        
        result = jedis.hget(key, field)
        return result if result is not None else ""
    
    # Set map field value
    def updateMapField(self, key: str, field: str, data: str, conf=None, host: str = None, port: int = None, db: int = None):
        """Update hash field value"""
        if conf is not None:
            self.updateMapFieldWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key, field, data)
        elif all(param is not None for param in [host, port, db]):
            self.updateMapFieldWithParams(host, port, db, key, field, data)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
    
    def updateMapFieldWithParams(self, host: str, port: int, db: int, key: str, field: str, data: str):
        """Update map field with retry logic"""
        try:
            self.updateMapFieldWithoutRetry(host, port, db, key, field, data)
        except redis.RedisError:
            self.redisConnect = self.createRedisConnect(host, port)
            self.updateMapFieldWithoutRetry(host, port, db, key, field, data)
    
    def updateMapFieldWithoutRetry(self, host: str, port: int, db: int, key: str, field: str, data: str):
        """Update map field without retry"""
        cleanedData = "" if data is None or data == "" else data
        if data is None or data == "":
            print(f"WARNING: data is empty, setting data='' for redis key={key}")
        
        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"WARNING: jedis=None means host is not set, skipping saving to redis key={key}")
            return
        
        # Select database if different
        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        if current_db != db:
            jedis = redis.Redis(host=host, port=port, db=db, socket_timeout=self.redisTimeout/1000, decode_responses=True)
        
        jedis.hset(key, field, cleanedData)
    
    # Get map as DataFrame
    def getMapAsDataFrame(self, key: str, schema: StructType, spark: SparkSession, conf=None, host: str = None, port: int = None, db: int = None) -> DataFrame:
        """Get hash as DataFrame"""
        if conf is not None:
            data = self.getMapWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key)
        elif all(param is not None for param in [host, port, db]):
            data = self.getMapWithParams(host, port, db, key)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
        
        if not data:
            return self.emptySchemaDataFrame(schema, spark)
        else:
            rows = [Row(k, v) for k, v in data.items()]
            return spark.createDataFrame(rows, schema)
    
    def getMap(self, key: str, conf=None, host: str = None, port: int = None, db: int = None) -> Dict[str, str]:
        """Get entire hash"""
        if conf is not None:
            return self.getMapWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key)
        elif all(param is not None for param in [host, port, db]):
            return self.getMapWithParams(host, port, db, key)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
    
    def getMapWithParams(self, host: str, port: int, db: int, key: str) -> Dict[str, str]:
        """Get map with retry logic"""
        try:
            return self.getMapWithoutRetry(host, port, db, key)
        except redis.RedisError:
            self.redisConnect = self.createRedisConnect(host, port)
            return self.getMapWithoutRetry(host, port, db, key)
    
    def getMapWithoutRetry(self, host: str, port: int, db: int, key: str) -> Dict[str, str]:
        """Get map without retry"""
        if not key:
            print("WARNING: key is empty")
            return {}
        
        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"WARNING: jedis=None means host is not set, skipping fetching the redis key={key}")
            return {}
        
        # Select database if different
        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        if current_db != db:
            jedis = redis.Redis(host=host, port=port, db=db, socket_timeout=self.redisTimeout/1000, decode_responses=True)
        
        # Check if key exists
        if not jedis.exists(key):
            print(f"WARNING: Key={key} does not exist in Redis")
            return {}
        
        return jedis.hgetall(key)
    
    # Set map
    def dispatch(self, key: str, data: Dict[str, str], replace: bool = True, conf=None, db: int = None, host: str = None, port: int = None):
        """Dispatch map to Redis"""
        if conf is not None and db is not None:
            self.dispatchWithParams(conf.redisHost, conf.redisPort, db, key, data, replace)
        elif conf is not None:
            self.dispatchWithParams(conf.redisHost, conf.redisPort, conf.redisDB, key, data, replace)
        elif all(param is not None for param in [host, port, db]):
            self.dispatchWithParams(host, port, db, key, data, replace)
        else:
            raise ValueError("Either conf or host/port/db must be provided")
    
    def dispatchWithParams(self, host: str, port: int, db: int, key: str, data: Dict[str, str], replace: bool):
        """Dispatch with retry logic"""
        try:
            self.dispatchWithoutRetry(host, port, db, key, data, replace)
        except redis.RedisError:
            self.redisConnect = self.createRedisConnect(host, port)
            self.dispatchWithoutRetry(host, port, db, key, data, replace)
    
    def dispatchWithoutRetry(self, host: str, port: int, db: int, key: str, data: Dict[str, str], replace: bool):
        """Dispatch without retry"""
        if not data:
            print(f"WARNING: map is empty, skipping saving to redis key={key}")
            return
        
        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"WARNING: jedis=None means host is not set, skipping saving to redis key={key}")
            return
        
        # Select database if different
        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        if current_db != db:
            jedis = redis.Redis(host=host, port=port, db=db, socket_timeout=self.redisTimeout/1000, decode_responses=True)
        
        if replace:
            self.replaceMap(jedis, key, data)
        else:
            jedis.hset(key, mapping=data)
    
    def getHashKeys(self, jedis: redis.Redis, key: str) -> list:
        """Get all hash keys using HSCAN"""
        keys = []
        cursor = 0
        while True:
            cursor, data = jedis.hscan(key, cursor, count=100)
            keys.extend(data.keys())
            if cursor == 0:
                break
        return keys
    
    def replaceMap(self, jedis: redis.Redis, key: str, data: Dict[str, str]):
        """Replace entire hash map"""
        # Get existing keys and find ones to delete
        existing_keys = set(self.getHashKeys(jedis, key))
        new_keys = set(data.keys())
        to_delete = existing_keys - new_keys
        
        # Delete keys that don't exist in new data
        if to_delete:
            jedis.hdel(key, *to_delete)
        
        # Set new/updated keys
        jedis.hset(key, mapping=data)
    
    def dispatchDataFrame(self, redisKey: str, df: DataFrame, keyField: str, valueField: str, replace: bool = True, conf=None):
        """Convert DataFrame to map and save to Redis"""
        # Convert DataFrame to dictionary
        dfMap = self.dataFrameToMap(df, keyField, valueField)
        self.dispatch(redisKey, dfMap, replace, conf)
    
    def dataFrameToMap(self, df: DataFrame, keyField: str, valueField: str) -> Dict[str, str]:
        """Convert DataFrame to map"""
        # Collect DataFrame as list of rows and convert to dictionary
        rows = df.select(keyField, valueField).collect()
        return {str(row[keyField]): str(row[valueField]) for row in rows}

    def dispatchDataFrameList(self, redisKey: str, df: DataFrame, keyField: str, valueFields: list, replace: bool = True, conf=None):
        """"Convert DataFrame to map with multiple columns"""
        rows = df.select([keyField] + valueFields).collect()
        dfMap = {str(row[keyField]): json.dumps({field: row[field] for field in valueFields}) for row in rows}
        self.dispatchToKpRedis(redisKey, dfMap, replace, conf)
    
    def emptySchemaDataFrame(self, schema: StructType, spark: SparkSession) -> DataFrame:
        """Create empty DataFrame with given schema"""
        return spark.createDataFrame([], schema)

    # Set map
    def dispatchToKpRedis(self, key: str, data: Dict[str, str], replace: bool = True, conf=None, db: int = None, host: str = None, port: int = None):
        """Dispatch map to Redis"""
        if conf is not None and db is not None:
            self.dispatchWithParams(conf.redisKpHost, conf.redisPort, db, key, data, replace)
        elif conf is not None:
            self.dispatchWithParams(conf.redisKpHost, conf.redisPort, conf.redisDB, key, data, replace)
        elif all(param is not None for param in [host, port, db]):
            self.dispatchWithParams(host, port, db, key, data, replace)
        else:
            raise ValueError("Either conf or host/port/db must be provided")

    def bulk_update(self, key_value_map: dict, conf=None, db: int = None, host: str = None, port: int = None):
        """
        Bulk update key-value pairs in Redis using pipeline.
        Follows same signature pattern as update().
        All keys set in a single network round trip.

        Args:
            key_value_map : dict of {redis_key: value}
            conf          : config object with redisHost, redisPort, redisDB (optional)
            db            : Redis DB index (optional)
            host          : Redis host (optional) 
            port          : Redis port (optional)
        """
        src_host = host if host is not None else getattr(conf, 'redisHost', None)
        src_port = port if port is not None else getattr(conf, 'redisPort', None)
        print(f"[Redis] bulk_update called; host={src_host}, port={src_port}, db={db}, keys={len(key_value_map)}")
        if conf is not None and db is not None:
            self.bulk_update_with_params(conf.redisHost, conf.redisPort, db, key_value_map)
        elif conf is not None:
            self.bulk_update_with_params(conf.redisHost, conf.redisPort, conf.redisDB, key_value_map)
        elif all(param is not None for param in [host, port, db]):
            self.bulk_update_with_params(host, port, db, key_value_map)
        else:
            raise ValueError("Either conf or host/port/db must be provided")

    def bulk_update_in_batches(self, key_value_map: dict, batch_size: int = 1000, conf=None, db: int = None, host: str = None, port: int = None):
        """
        Bulk update key-value pairs in Redis in batches using pipeline.

        Args:
            key_value_map : dict of {redis_key: value}
            batch_size    : maximum number of keys per pipeline execution
            conf          : config object with redisHost, redisPort, redisDB (optional)
            db            : Redis DB index (optional)
            host          : Redis host (optional)
            port          : Redis port (optional)
        """
        src_host = host if host is not None else getattr(conf, 'redisHost', None)
        src_port = port if port is not None else getattr(conf, 'redisPort', None)
        print(f"[Redis] bulk_update_in_batches called; host={src_host}, port={src_port}, db={db}, batch_size={batch_size}, keys={len(key_value_map)}")
        if batch_size is None or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")

        if conf is not None and db is not None:
            self.bulk_update_in_batches_with_params(conf.redisHost, conf.redisPort, db, key_value_map, batch_size)
        elif conf is not None:
            self.bulk_update_in_batches_with_params(conf.redisHost, conf.redisPort, conf.redisDB, key_value_map, batch_size)
        elif all(param is not None for param in [host, port, db]):
            self.bulk_update_in_batches_with_params(host, port, db, key_value_map, batch_size)
        else:
            raise ValueError("Either conf or host/port/db must be provided")

    def bulk_update_in_batches_with_params(self, host: str, port: int, db: int, key_value_map: dict, batch_size: int = 1000):
        """Bulk update in batches with retry logic."""
        db = self.normalize_db(db)
        print(f"[Redis] bulk_update_in_batches_with_params host={host}, port={port}, db={db}, keys={len(key_value_map)}, batch_size={batch_size}")
        try:
            self.bulk_update_in_batches_without_retry(host, port, db, key_value_map, batch_size)
        except redis.RedisError as ex:
            print(f"[Redis] retrying bulk_update_in_batches after RedisError: {ex}")
            self.redisConnect = self.createRedisConnect(host, port)
            self.bulk_update_in_batches_without_retry(host, port, db, key_value_map, batch_size)

    def bulk_update_in_batches_without_retry(self, host: str, port: int, db: int, key_value_map: dict, batch_size: int = 1000):
        """Bulk update in batches without retry."""
        db = self.normalize_db(db)
        if not key_value_map:
            print("[Redis] WARNING: key_value_map is empty, nothing to push to Redis")
            return

        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"[Redis] WARNING: jedis=None means host is not set, skipping bulk redis push")
            return

        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        print(f"[Redis] current connection db={current_db}, target db={db}")
        if current_db != db:
            print(f"[Redis] switching connection to db={db}")
            jedis = redis.Redis(
                host=host,
                port=port,
                db=db,
                socket_timeout=self.redisTimeout / 1000,
                decode_responses=True
            )

        pipeline = jedis.pipeline()
        batch_count = 0
        item_count = 0

        for key, value in key_value_map.items():
            cleaned_value = "" if value is None or value == "" else value
            if value is None or value == "":
                print(f"[Redis] WARNING: data is empty, setting data='' for redis key={key}")
            pipeline.set(key, cleaned_value)
            item_count += 1

            if item_count % batch_size == 0:
                print(f"[Redis] executing batch {batch_count + 1} at {item_count} items")
                pipeline.execute()
                batch_count += 1
                pipeline = jedis.pipeline()

        if item_count % batch_size != 0:
            print(f"[Redis] executing final batch {batch_count + 1} at {item_count} items")
            pipeline.execute()
            batch_count += 1

        print(f"✅ Bulk pushed {item_count} keys to Redis in {batch_count} batch(es) via pipeline.")

    def bulk_update_with_params(self, host: str, port: int, db: int, key_value_map: dict):
        """Bulk update with retry logic — mirrors updateWithParams"""
        db = self.normalize_db(db)
        print(f"[Redis] bulk_update_with_params host={host}, port={port}, db={db}, keys={len(key_value_map)}")
        try:
            self.bulk_update_without_retry(host, port, db, key_value_map)
        except redis.RedisError as ex:
            print(f"[Redis] retrying bulk_update after RedisError: {ex}")
            self.redisConnect = self.createRedisConnect(host, port)
            self.bulk_update_without_retry(host, port, db, key_value_map)

    def bulk_update_without_retry(self, host: str, port: int, db: int, key_value_map: dict):
        """Bulk update without retry — mirrors updateWithoutRetry"""
        db = self.normalize_db(db)
        if not key_value_map:
            print("[Redis] WARNING: key_value_map is empty, nothing to push to Redis")
            return

        jedis = self.getOrCreateRedisConnect(host, port)
        if jedis is None:
            print(f"[Redis] WARNING: jedis=None means host is not set, skipping bulk redis push")
            return

        current_db = jedis.connection_pool.connection_kwargs.get('db', 0)
        print(f"[Redis] current connection db={current_db}, target db={db}")
        if current_db != db:
            print(f"[Redis] switching connection to db={db}")
            jedis = redis.Redis(
                host=host,
                port=port,
                db=db,
                socket_timeout=self.redisTimeout / 1000,
                decode_responses=True
            )

        pipeline = jedis.pipeline()
        for key, value in key_value_map.items():
            cleaned_value = "" if value is None or value == "" else value
            if value is None or value == "":
                print(f"[Redis] WARNING: data is empty, setting data='' for redis key={key}")
            pipeline.set(key, cleaned_value)
        pipeline.execute()
        print(f"✅ Bulk pushed {len(key_value_map)} keys to Redis via pipeline.")

# Create global instance (similar to Scala object)
Redis = Redis()