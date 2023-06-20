from aws.awsOps import AWSOperations
import json
from utils import helpers
from utils.colors import colors
# from constants import default_regions
from tqdm import tqdm
import os
import time
# from datetime import datetime
import threading
from aws.policygenOps import runPolicyGeneratorCRUD , generateLeastprivPolicies
from redisops.redisOps import RedisOperations
from aws.getPreviousPolicies import get_policies_for_users
from aws.comparePolicies import compare_policies
from aws.createServiceMap import create_service_map
from contextlib import contextmanager
import pendulum
from pendulum import timezone
from concurrent.futures import ThreadPoolExecutor
import itertools
from aws.dataCleanup import DataCleanup

completedBucketsLock = threading.Lock()

utc_timezone = timezone("UTC")
pendulum.set_local_timezone(utc_timezone)

@contextmanager
def measure_time_block(message: str = "Execution time"):
    start = time.time()
    yield
    end = time.time()
    duration = end - start

    if duration >= 3600:
        hours = int(duration // 3600)
        duration %= 3600
        print(f"{message} completed in {hours} hour(s) {int(duration // 60)} minute(s) {duration % 60:.2f} second(s)")
    elif duration >= 60:
        minutes = int(duration // 60)
        print(f"{message} completed in {minutes} minute(s) {duration % 60:.2f} second(s)")
    else:
        print(f"{message} completed in {duration:.2f} second(s)")


class s3Operations(AWSOperations):
    def __init__(self):
        super().__init__()
        self.crudConnection = RedisOperations()
        self.crudConnection.connect("localhost", 6379, 0)
        self.data_cleanup = DataCleanup(self.crudConnection)

    def getConfig(self):
        with open("config.json", "r") as f:
            data = json.loads(f.read())
            return data

    def mergeData(self, accountId, num_days, bucketData):
        print(f"Merging data for {num_days}")
        now = pendulum.now()
        previousData = [(now - pendulum.duration(days=i)).strftime("%Y/%m/%d") for i in range(num_days)]
        merged_data = {}

        for bucketName, regions in bucketData.items():
            for region, date in itertools.product(regions, previousData):
                day_data_str = self.crudConnection.read_json(f"{bucketName}_{accountId}_{date}_{region}")
                if day_data_str:
                    for username, actions in day_data_str.items():
                        if username not in merged_data:
                            merged_data[username] = set(actions)
                        else:
                            merged_data[username].update(actions)

        # Convert sets back to lists
        for username in merged_data:
            merged_data[username] = list(merged_data[username])
        return merged_data

    def is_request_processed(self, account_id, bucket_name, date, region):
        outer_key = f"{bucket_name}_{account_id}_{date}_{region}"
        return self.crudConnection.exists(outer_key)

    def getObjects(self, completedBuckets, bucketData, num_days,unique_id):
        self.data_cleanup.cleanup()
        try:
            config = self.getConfig()
            accountId = config['accountId']
            today = pendulum.now()
            endDay = today.date()
            startDay = today.subtract(days=num_days).date()

            day_diff = (endDay - startDay).days + 1

            for bucketName, regions in bucketData.items():

                thread = threading.Thread(target=bucketThreadFn, args=(completedBuckets, bucketName, regions, startDay, endDay, accountId, day_diff,unique_id))
                thread.start()

        except KeyError as err:
            print(helpers.colors.FAIL + "Invalid key " + str(err))

        except Exception as exp:
            helpers.logException(exp)

    def getPolicies(self, account_id, num_days, bucketData,unique_id):
        user_policies_dir = f"userPolicies_{account_id}_{unique_id}"
        present_policies_dir = f"presentPolicies_{account_id}_{unique_id}"

        mergedData = self.mergeData(account_id,num_days, bucketData)
        create_service_map(mergedData)

        with measure_time_block("Generating Policies"):
            print("Generating Policies")
            for username, actions in mergedData.items():
                actions_list = [json.loads(action) for action in actions]
                generateLeastprivPolicies(username, user_policies_dir, actions_list)

        with measure_time_block("Getting Present Policies"):
            print("Getting Present Policies")
            get_policies_for_users(present_policies_dir, mergedData)

        # with measure_time_block("Comparing the policies"):
        #     print("Comparing the policies to get excessive policies")
        #     compare_policies()

def bucketThreadFn(completedBuckets, bucketName, regions, startDay, endDay, accountId, day_diff,unique_id):
    print("Started thread for Bucket : " + bucketName)

    s3Ops = s3Operations()
    s3Client = s3Ops.getConnection()

    completedRegions = []
    region_events = [threading.Event() for _ in regions]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for idx, region in enumerate(regions):
            executor.submit(regionThreadFn, startDay, endDay, accountId, region, s3Client, bucketName, completedRegions, region_events[idx],unique_id)

    for event in region_events:
        event.wait()

    with completedBucketsLock:
        completedBuckets.append(bucketName)
    print(f"Completed thread for Bucket : {bucketName}")


def regionThreadFn(startDay, endDay, accountId, region, s3Client, bucketName ,completedRegions,region_event,unique_id):

    print(f"Starting thread for {region}")
    completedDays = []
    day_threads = []

    day = startDay
    while day <= endDay:
        thread = threading.Thread(target=dayThreadFn, args=(day, accountId, region, s3Client, bucketName,completedDays,unique_id))
        day_threads.append(thread)
        thread.start()
        day = day.add(days=1)

    for thread in day_threads:
        thread.join()

    print(f"Completed thread for {region}")
    completedRegions.append(region)
    region_event.set()


def dayThreadFn(day, accountId, region, s3Client, bucketName, completedDays,unique_id):

    date_str = day.strftime("%Y/%m/%d")
    s3obj = s3Operations()

    if s3obj.is_request_processed(accountId, bucketName, date_str, region):
        print(f"Skipping {region} : {date_str} as it has already been processed")
        completedDays.append(day)
        return

    try:
        print(f"Starting thread for {region} : {day.format('YYYY/MM/DD')}")
        prefix = f"AWSLogs/{accountId}/CloudTrail/{region}/{day.format('YYYY/MM/DD')}"
        response = s3Client.list_objects(Bucket=bucketName, Prefix=prefix)
        contents = response.get('Contents', [])

        total_items = len(contents)
        processed_items = 0

        for item in contents:
            key = str(item['Key'])
            filePath = key.split('/')[-1]
            with open(os.path.join(f"logs_{accountId}_{unique_id}", filePath), "wb") as data:
                s3Client.download_fileobj(bucketName, key, data)
            downloaded = False
            retry_count = 0
            max_retries = 3

            while not downloaded and retry_count < max_retries:
                try:
                    runPolicyGeneratorCRUD(f"logs_{accountId}_{unique_id}/{filePath}", f"{day.format('YYYY/MM/DD')}", region, accountId, bucketName)
                    os.remove(f"logs_{accountId}_{unique_id}/{filePath}")
                    downloaded = True
                except OSError as e:
                    if e.errno == 32:
                        time.sleep(0.1)
                    else:
                        retry_count += 1
                        time.sleep(1)
                except Exception as e:
                    print(f"Error in runPolicyGeneratorCRUD for {region} : {day.format('YYYY/MM/DD')}: {str(e)}")
                    retry_count += 1
                    time.sleep(1)

            processed_items += 1
            print(f"Processed {processed_items}/{total_items} items for {region} : {day.format('YYYY/MM/DD')}")

        print(f"Completed thread for {region} : {day.format('YYYY/MM/DD')}")
        completedDays.append(day)

    except Exception as e:
        print(f"Error in dayThreadFn for {region} : {day.format('YYYY/MM/DD')}: {str(e)}")


