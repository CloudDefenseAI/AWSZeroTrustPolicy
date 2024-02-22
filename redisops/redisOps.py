import redis
import json


class RedisOperations:
    def connect(self, host, port, dbNum):
        self.r = redis.Redis(host=host, port=port, db=dbNum)

    def insertKeyVal(self, key, value):
        self.r.set(key, value)

    def createList(self, key, value_list):
        for item in value_list:
            self.r.rpush(key, item)

    # insert a value at the end of a list
    def push_back(self, key, value):
        self.r.rpush(key, value)

    # insert a value at the front of a list
    def push_front(self, key, value):
        self.r.lpush(key, value)

    def push_back_json(self, key, jsonObj):
        jsonStr = json.dumps(jsonObj)
        pos = self.r.lpos(key, jsonStr)
        if pos == None:
            self.r.rpush(key, jsonStr)

    def pop_back(self, key):
        self.r.rpop(key)

    def pop_front(self, key):
        self.r.lpop(key)

    def insert_json(self, key, json_object):
        self.r.set(key, json.dumps(json_object))

    def read_json(self, key):
        val = self.r.get(key)
        if val is not None:
            return json.loads(val.decode())
        return None

    def insert_jsonobj_list(self, key, listOfJsonObjects):
        for jsonObject in listOfJsonObjects:
            self.push_back(key, json.dumps(jsonObject))

    def get_list_items(self, key):
        items = self.r.lrange(key, 0, -1)
        return items

    def get_all_keys(self):
        return self.r.scan_iter()

    # flush all keys from current DB
    def flushdb(self):
        self.r.flushdb()

    def push_back_nested_json(self, outer_key, inner_key, jsonObj):
        jsonStr = json.dumps(jsonObj)
        current_data_str = self.r.get(outer_key)
        if current_data_str:
            current_data = json.loads(current_data_str)
        else:
            current_data = {}

        if inner_key not in current_data:
            current_data[inner_key] = []

        if jsonStr not in current_data[inner_key]:
            current_data[inner_key].append(jsonStr)

        self.r.set(outer_key, json.dumps(current_data))

    def get_redis_dump(self):
        all_keys = self.r.keys()
        redis_dump = {}
        for key in all_keys:
            key_str = key.decode()
            value = self.r.get(key)
            if value:
                try:
                    value_json = json.loads(value)
                except json.JSONDecodeError:
                    value_json = value.decode()
                redis_dump[key_str] = value_json
            else:
                list_items = self.r.lrange(key, 0, -1)
                redis_dump[key_str] = [json.loads(item) for item in list_items]
        return redis_dump

    def exists(self, key):
        return self.r.exists(key)
