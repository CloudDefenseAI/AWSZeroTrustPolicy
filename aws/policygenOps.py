import ijson
import gzip
import re
import os
from tqdm import tqdm
from utils.colors import colors
from redisops.redisOps import RedisOperations
import json
# from policy_sentry.writing.template import get_crud_template_dict
# from policy_sentry.command.write_policy import write_policy_with_template
from collections import defaultdict

crudConnection = RedisOperations()
crudConnection.connect("localhost",6379,0)
arnStore = RedisOperations()
arnStore.connect("localhost",6379,2)

def get_event_type(event_name):
    read_events = ["^Get", "^Describe", "^Head"]
    write_events = ["^Create", "^Put", "^Post", "^Copy",
                    "^Complete", "^Delete", "^Update", "^Modify"]
    tagging_events = ["^Tag", "^Untag"]
    list_events = ["^List"]
    for pattern in read_events:
        if re.search(pattern, event_name):
            return "read"

    for pattern in write_events:
        if re.search(pattern, event_name):
            return "write"

    for pattern in tagging_events:
        if re.search(pattern, event_name):
            return "tagging"
    for pattern in list_events:
        if re.search(pattern,event_name):
            return "list"
    return None

def load_service_actions_cache(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def load_service_replace_map(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def generate_least_privilege_policy(user_arn, actions_data, service_actions_cache, service_replace_map):
    policy = {
        "Version": "2012-10-17",
        "Statement": []
    }
    resource_actions = defaultdict(lambda: defaultdict(set))

    for action_data in actions_data:
        if action_data["username"] != user_arn:
            continue

        service = action_data['eventSource'].split('.')[0]
        action = f"{service}:{action_data['eventName']}"

        if action in service_replace_map:
            action = service_replace_map[action]

        if action not in service_actions_cache.get(service, []):
            continue

        resource = action_data['arn']
        resource_prefix = resource.split(':')[5].split('/')[0]
        resource_actions[resource_prefix][resource].add(action)

    for resource_prefix, resources in resource_actions.items():
        statement = {
            "Effect": "Allow",
            "Action": [],
            "Resource": []
        }

        for resource, actions in resources.items():
            statement["Action"].extend(actions)
            statement["Resource"].append(resource)

        statement["Action"] = list(set(statement["Action"]))
        policy["Statement"].append(statement)

    policy_json = json.dumps(policy)

    if len(policy_json) > 6144:
        print(f"Generated policy exceeds the 6144 character limit for {user_arn}")

    return policy

def generateLeastprivPolicies(user_arn, policy_output_dir, actions_list):
    service_actions_cache = load_service_actions_cache("service_actions_cache.json")
    service_replace_map = load_service_replace_map("service_replace_map.json")
    policy = generate_least_privilege_policy(user_arn, actions_list, service_actions_cache, service_replace_map)
    username = user_arn.split("/")[-1]
    filename = f'policy_{username}.json'
    filename = filename.replace(":", "-").replace("/", "_").replace("\\", "__")

    with open(os.path.join(policy_output_dir, filename), "w") as f_write:
        f_write.write(json.dumps(policy, indent=2))
        print(f"Generated policy for {username}")


def runPolicyGeneratorCRUD(filePath,date,region,bucketName, accountID):
    with gzip.open(f"{filePath}", 'rt') as f:
        parser = ijson.parse(f)
        startKey = "Records.item.eventVersion"
        startLoop = False
        currentBlock = {}

        for prefix, event, value in parser:
            if prefix == startKey:
                if startLoop == False:
                    startLoop = True
                else:
                    startLoop = False
                    if currentBlock.get("username") is not None and currentBlock.get("arn") is not None:
                        userNameCB = currentBlock.get("username")
                        outerKey = f"{accountID}_{bucketName}_{date}_{region}"
                        crudConnection.push_back_nested_json(outerKey, userNameCB, currentBlock)
                    currentBlock = {}
            if startLoop:
                if prefix == "Records.item.resources.item.ARN":
                    currentBlock["arn"] = value
                if prefix == "Records.item.userIdentity.type":
                    currentBlock["userIdentityType"] = value
                if prefix == "Records.item.userIdentity.arn" and currentBlock.get("userIdentityType") == "IAMUser":
                    currentBlock["username"] = value
                if prefix == "Records.item.eventSource":
                    currentBlock["eventSource"] = value
                if prefix == "Records.item.eventName":
                    currentBlock["eventName"] = value

