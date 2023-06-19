import pendulum

class DataCleanup:
    def __init__(self, redis_connection):
        self.redis_connection = redis_connection

    def cleanup(self):
        now = pendulum.now()
        keys_to_delete = []

        for key in self.redis_connection.r.scan_iter("*"):
            date_str = key.decode("utf-8").split("_")[2]
            try:
                date = pendulum.from_format(date_str, "YYYY/MM/DD")
            except ValueError:
                continue

            days_diff = (now - date).days
            if days_diff > 90:
                keys_to_delete.append(key)

        print(f"Found {len(keys_to_delete)} keys to delete.")

        for key in keys_to_delete:
            self.redis_connection.r.delete(key)
            print(f"Deleted key: {key.decode('utf-8')}")
