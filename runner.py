from aws.s3Ops import s3Operations
import os
import shutil
import time
import json
import glob
from redisops.redisOps import RedisOperations
from contextlib import contextmanager
import uuid

arnStore = RedisOperations()
arnStore.connect("localhost", 6379, 2)

unique_id = uuid.uuid4().hex[:6]

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

def create_dirs(account_id):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    directories = [
        os.path.join(base_dir, f"logs_{account_id}_{unique_id}"),
        os.path.join(base_dir, f"userPolicies_{account_id}_{unique_id}"),
        os.path.join(base_dir, f"presentPolicies_{account_id}_{unique_id}"),
        # os.path.join(base_dir, f"excessivePolicies_{account_id}"),
    ]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)

    empty_directory(f"logs_{account_id}_{unique_id}")
    empty_directory(f"userPolicies_{account_id}_{unique_id}")
    empty_directory(f"presentPolicies_{account_id}_{unique_id}")
    # empty_directory(f"excessivePolicies_{account_id}")

def empty_directory(directory_name):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.join(base_dir, directory_name)

    if not os.path.exists(directory_path):
        print(f"Path '{directory_path}' does not exist.")
        return

    for file in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    print(f"Emptied directory: {directory_path}")


def get_policy_from_file(folder, username):
    filename = f"{username}.json"
    filepath = os.path.join(folder, filename)
    with open(filepath, 'r') as f:
        return json.load(f)


def get_user_arn(username):
    user_arn = arnStore.r.get(username)
    if user_arn is None:
        return None
    return user_arn.decode()


def load_policies_from_directory(directory_name: str):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    policies_path = os.path.join(base_dir, directory_name)
    policies_files = glob.glob(os.path.join(policies_path, "policy_*.json"))

    policies = {}

    for file_path in policies_files:
        with open(file_path, 'r') as policy_file:
            policy = json.load(policy_file)

        username = os.path.basename(file_path).replace('policy_', '').replace('.json', '')
        user_arn = get_user_arn(username)
        policies[user_arn] = policy

    return policies


def reformat_bucket_data(bucket_data):
    reformatted_data = {}
    for region, bucket_name in bucket_data.items():
        if bucket_name in reformatted_data:
            reformatted_data[bucket_name].append(region)
        else:
            reformatted_data[bucket_name] = [region]
    return reformatted_data

def runner(accountType,aws_access_key_id,aws_secret_access_key,accountId,num_days,bucketData,role_arn,externalid):
    print(f"Running for {num_days} days")
    with measure_time_block("Data Population"):
        create_dirs(accountId)
        s3Ops = s3Operations()

        print("Starting to generate Heap")

        bucketData = reformat_bucket_data(bucketData)

        config_data = {
                "accountType": accountType,
                "bucketData": bucketData,
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "externalid": externalid,
                "role_arn": role_arn,
                "accountId": accountId
            }

        with open('config.json', 'w') as config_file:
            json.dump(config_data, config_file)

        completedBuckets = []

        s3Ops.getObjects(completedBuckets,bucketData,num_days,unique_id)

        while len(completedBuckets) < len(bucketData):
            time.sleep(10)

    print("Generating Policies")
    s3Ops.getPolicies(accountId, num_days, bucketData,unique_id)

    generated_policies = load_policies_from_directory(f"userPolicies_{accountId}_{unique_id}")
    consolidated_policies = load_policies_from_directory(f"presentPolicies_{accountId}_{unique_id}")
    excessive_policies = load_policies_from_directory(f"excessivePolicies_{accountId}_{unique_id}")

    response = {
        "accountId": accountId,
        "generatedPolicies": generated_policies,
        "consolidatedPolicies": consolidated_policies,
        "excessivePolicies": excessive_policies,
    }
    
    return response
