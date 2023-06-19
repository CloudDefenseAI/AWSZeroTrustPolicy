import os
import json
from botocore.exceptions import ClientError
from aws.awsOps import AWSOperations
from utils.colors import colors
import concurrent.futures
import re
from redisops.redisOps import RedisOperations

arnStore = RedisOperations()
arnStore.connect("localhost",6379,2)
 
def is_valid_arn(arn):
    return re.match(r'^arn:aws:iam::\d{12}:user/[\w+=,.@-]+$', arn.decode()) is not None

def get_policies_for_users(path, merged_data):
    ops = AWSOperations()
    iam_client = ops.get_iam_connection()

    valid_user_list = [user.encode() for user in merged_data.keys() if is_valid_arn(user.encode())]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_previous_policies, iam_client, path, user.decode()) for user in valid_user_list]

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred during parallel processing: {e}")

def get_previous_policies(iam_client,path, current_user):
    user_arn = current_user
    current_user = current_user.split("/")[-1]
    arnStore.insertKeyVal(current_user, user_arn)

    print(f"Started Thread for Fetching policies for {current_user}")
    try:
        if isinstance(current_user, bytes):
            current_user = current_user.decode()

        policy_dict = {'Version': '2012-10-17', 'Statement': []}

        if user_exists(iam_client, current_user):
            policies = iam_client.list_attached_user_policies(UserName=current_user)['AttachedPolicies']
            inline_policies = iam_client.list_user_policies(UserName=current_user)['PolicyNames']

            # Fetch managed policies
            for policy in policies:
                policy_arn = policy['PolicyArn']
                policy_version = iam_client.get_policy(PolicyArn=policy_arn)['Policy']['DefaultVersionId']
                policy_doc = iam_client.get_policy_version(PolicyArn=policy_arn, VersionId=policy_version)['PolicyVersion']['Document']
                policy_dict['Statement'].extend(policy_doc['Statement'])

            # Fetch inline policies
            for policy_name in inline_policies:
                policy_doc = iam_client.get_user_policy(UserName=current_user, PolicyName=policy_name)['PolicyDocument']
                policy_dict['Statement'].extend(policy_doc['Statement'])
        else:
            print(f"User {current_user} does not exist. Creating an empty policy.")

        outfileName = f'policy_{current_user}.json'
        outfileName = outfileName.replace(":","-").replace("/","_").replace("\\","__")
        with open(os.path.join(path, outfileName), 'w') as f:
            json.dump(policy_dict, f, indent=2)

        print(
              f"Fetched Present Policies for {current_user}")
    except ClientError as e:
        print(f"Error occurred: {e}")

def user_exists(iam_client, user_name):
    try:
        iam_client.get_user(UserName=user_name)
        return True
    except iam_client.exceptions.NoSuchEntityException:
        return False