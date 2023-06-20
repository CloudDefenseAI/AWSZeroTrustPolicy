import json
from redisops.redisOps import RedisOperations

redis_ops = RedisOperations()
redis_ops.connect("localhost", 6379, 0)
redis_dump = redis_ops.get_redis_dump()

# Print the JSON dump in a pretty format

print(json.dumps(redis_dump, indent=2))
