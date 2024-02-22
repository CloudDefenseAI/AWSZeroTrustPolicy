import json
import os
from policy_sentry.querying.actions import get_actions_for_service
import re
from collections import defaultdict

services = defaultdict(set)


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
    return service_actions_cache


def write_service_actions_cache_to_file(service_actions_cache, file_path):
    with open(file_path, "w") as f:
        json.dump(service_actions_cache, f, indent=2)


def load_policy(filepath):
    with open(filepath, "r") as f:
        policy = json.load(f)
    return policy


def is_valid_action(action):
    return re.match(r"^[a-zA-Z0-9_]+:(\*|[a-zA-Z0-9_\*]+)$", action)


def create_service_map(mergedData):
    print("Creating service map")
    for username, actions in mergedData.items():
        actions_list = [action for action in actions]
        create_services_list(actions_list)

    service_actions_cache = create_service_actions_cache(services)
    write_service_actions_cache_to_file(
        service_actions_cache, "service_actions_cache.json"
    )
    print("Service actions cache created successfully.")
