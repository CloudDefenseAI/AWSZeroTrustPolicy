import json
import os
from policy_sentry.querying.actions import get_actions_for_service
from redisops.redisOps import RedisOperations
import re
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

crudConnection = RedisOperations()
crudConnection.connect("localhost", 6379, 0)

services = defaultdict(set)

# hdServices = ['a4b','account','amplify','apprunner','appsync','aps','billing','codebuild','codecommit','connect','databrew','eks','emrcontainers','forecast','frauddetector','fsx','gamelift','greengrassv2','health','iot','iotanalytics','iotevents','iotfleethub','iotthingsgraph','kafka','kendra','kinesisvideo','lakeformation','licensemanager','lookoutvision','macie','managedblockchain','marketplacecatalog','mediaconnect','mediaconvert','medialive','mediapackage','mediapackage-vod','mediastore','mediastore-data','mediatailor','meteringmarketplace','migrationhub-config','mobile','mq','neptune','networkmanager','outposts','personalize','pinpoint','pinpoint-email','pinpoint-sms-voice','polly','pricing','qldb','quicksight','ram','rds-data','robomaker','route53resolver','sagemaker','sagemaker-a2i-runtime','sagemaker-edge','sagemaker-featurestore-runtime','sagemaker-runtime','savingsplans','schemas','secretsmanager','securityhub','serverlessrepo','servicecatalog','servicecatalog-appregistry','servicequotas','sesv2','shield','signer','sms','snowball','snowball-edge','sso','sso-oidc','ssm','stepfunctions','storagegateway','synthetics','textract','transcribe','transfer','translate','waf-regional','wafv2','worklink','workmail','workmailmessageflow','workspaces','xray','autoscaling','iam','ec2','s3','rds','elasticache','elasticbeanstalk','elasticloadbalancing','elasticmapreduce','cloudfront','cloudtrail','cloudwatch','cloudwatchevents','cloudwatchlogs','config','datapipeline','directconnect','dynamodb','ecr','ecs','elasticfilesystem','elastictranscoder','glacier','kinesis','kms','lambda','opsworks','redshift','route53','route53domains','sdb','ses','sns','sqs','storagegateway','sts','support','swf','waf','workspaces','xray']
# hdServices.extend(['acm','acm-pca','alexaforbusiness','amplifybackend','appconfig','appflow','appintegrations','appmesh','appstream','appsync','athena','auditmanager','autoscaling-plans','backup','batch','braket','budgets','ce','chime','cloud9','clouddirectory','cloudformation','cloudhsm','cloudhsmv2','cloudsearch','cloudsearchdomain','cloudtrail','cloudwatch','cloudwatchevents','cloudwatchlogs','codeartifact','codebuild','codecommit','codedeploy','codeguru-reviewer','codeguru-reviewer-runtime','codeguru-profiler','codeguru-profiler-runtime','codepipeline','codestar','codestar-connections','codestar-notifications','cognito-identity','cognito-idp','cognito-sync','comprehend','comprehendmedical','compute-optimizer','connect','connect-contact-lens','connectparticipant','cur','customer-profiles','dataexchange','datapipeline','datasync','dax','detective','devicefarm','devops-guru','directconnect','discovery','dlm','dms','docdb','ds','dynamodb','dynamodbstreams','ec2','ec2-instance-connect','ecr','ecr-public','ecs','eks','elastic-inference','elasticache','elasticbeanstalk','elasticfilesystem','elasticloadbalancing','elasticloadbalancingv2','elasticmapreduce','elastictranscoder','email','es','events','firehose','fms','forecast','forecastquery','frauddetector','fsx','gamelift','glacier','globalaccelerator','glue','greengrass','greengrassv2','groundstation','guardduty','health','healthlake','honeycode','iam','identitystore','imagebuilder','importexport','inspector','iot','iot-data','iot-jobs-data','iot1click-devices','iot1click-projects','iotanalytics','iotdeviceadvisor','iotevents','iotevents-data','iotfleethub','iotsecuretunneling','iotthingsgraph','iotwireless','ivs','kafka','kendra','kinesis','kinesis-video-archived-media','kinesis-video-media','kinesis-video-signaling','kinesisvideo','kinesisanalytics','kinesisanalyticsv2','kinesisvideoarchivedmedia','kinesis'])

def create_services_list(actions_data):
    for action in actions_data:
        action_data = json.loads(action)
        event_source = action_data["eventSource"]
        service = event_source.split(".")[0]
        services[service].add(service)

def create_service_actions_cache(services):
    service_actions_cache = {}

    for service in services:
        actions = get_actions_for_service(service)
        service_actions_cache[service] = actions

    # for service in hdServices:
    #     if service in service_actions_cache:
    #         continue
    #     else:
    #         actions = get_actions_for_service(service)
    #         service_actions_cache[service] = actions

    return service_actions_cache

def write_service_actions_cache_to_file(service_actions_cache, file_path):
    with open(file_path, 'w') as f:
        json.dump(service_actions_cache, f, indent=2)

def load_policy(filepath):
    with open(filepath, "r") as f:
        policy = json.load(f)
    return policy

def is_valid_action(action):
    return re.match(r'^[a-zA-Z0-9_]+:(\*|[a-zA-Z0-9_\*]+)$', action)

def compare_policy_worker(present_policy_filepath, user_policy_filepath, output_filepath):
    print(f"Started thread for {user_policy_filepath}")

    current_policy = load_policy(present_policy_filepath)
    generated_policy = load_policy(user_policy_filepath)

    excessive_permissions = compare_policy(current_policy, generated_policy)

    with open(output_filepath, "w") as f_write:
        f_write.write(json.dumps(excessive_permissions, indent=2))
        print(f"Generated excessive policy for {os.path.basename(user_policy_filepath)}")

# def expand_wildcard_actions(actions_list, service_actions_cache=None):
#     if service_actions_cache is None:
#         with open("service_actions_cache.json", "r") as f:
#             service_actions_cache = json.load(f)

#     expanded_actions = []

#     for action in actions_list:
#         if re.match(r'^[a-zA-Z0-9_]+:[a-zA-Z0-9_]+$', action):
#             service, action_name = action.split(":")
#             if service in service_actions_cache:
#                 if "*" in action_name:
#                     expanded_actions.extend([f"{a}" for a in service_actions_cache[service] if action_name.replace("*", "") in a])
#                 else:
#                     expanded_actions.append(action)
#             else:
#                 print(f"Warning: Service '{service}' not found in the service_actions_cache. for this the action was {action}")
#         elif action == '*':
#             for service in service_actions_cache:
#                 expanded_actions.extend([f"{a}" for a in service_actions_cache[service]])

#     return expanded_actions

def expand_wildcard_actions(actions_list, service_actions_cache=None):
    if service_actions_cache is None:
        with open("service_actions_cache.json", "r") as f:
            service_actions_cache = json.load(f)

    expanded_actions = []

    if isinstance(actions_list, str):
        actions_list = [actions_list]

    for action in actions_list:
        if is_valid_action(action):
            service, action_name = action.split(":")
            if "*" in action_name:
                expanded_actions.extend([f"{a}" for a in service_actions_cache.get(service, []) if action_name.replace("*", "") in a])
            else:
                expanded_actions.append(action)

        elif action == '*':
            for service in service_actions_cache:
                expanded_actions.extend([f"{a}" for a in service_actions_cache[service]])

    return expanded_actions

def compare_policy(current_policy, generated_policy):
    excessive_permissions = {
        "Version": "2012-10-17",
        "Statement": []
    }

    for current_statement in current_policy["Statement"]:
        excessive_statement = {
            "Effect": current_statement["Effect"],
            "Action": [],
            "Resource": current_statement["Resource"]
        }

        current_actions_expanded = expand_wildcard_actions(current_statement["Action"])

        for action in current_actions_expanded:
            action_in_generated = False
            for generated_statement in generated_policy["Statement"]:
                generated_actions_expanded = expand_wildcard_actions(generated_statement["Action"])

                # Check if the action and resource match in both policies
                if action in generated_actions_expanded:
                    for current_resource in current_statement["Resource"]:
                        if current_resource in generated_statement["Resource"]:
                            action_in_generated = True
                            break
                    if action_in_generated:
                        break

            if not action_in_generated:
                excessive_statement["Action"].append(action)

        if excessive_statement["Action"]:
            excessive_permissions["Statement"].append(excessive_statement)

    return excessive_permissions

def compare_policies():
    crudKeys = crudConnection.get_all_keys()
    for user_arn in crudKeys:
        actions_data = crudConnection.get_list_items(user_arn)
        create_services_list(actions_data)

    service_actions_cache = create_service_actions_cache(services)
    write_service_actions_cache_to_file(service_actions_cache, 'service_actions_cache.json')
    print("Service actions cache created successfully.")

    # present_policies_dir = "presentPolicies"
    # user_policies_dir = "userPolicies"
    # output_dir = "excessivePolicies"

    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)

    # with ThreadPoolExecutor() as executor:
    #     futures = []
    #     for user_policy_filename in os.listdir(user_policies_dir):
    #         present_policy_filepath = os.path.join(present_policies_dir, user_policy_filename)
    #         user_policy_filepath = os.path.join(user_policies_dir, user_policy_filename)
    #         output_filepath = os.path.join(output_dir, user_policy_filename)

    #         if os.path.exists(present_policy_filepath):
    #             future = executor.submit(compare_policy_worker, present_policy_filepath, user_policy_filepath, output_filepath)
    #             futures.append(future)

    #     for future in futures:
    #         future.result()
