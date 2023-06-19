import requests
import json
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
import concurrent.futures

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/version")
async def health():
    return {"version": "3.5"}


@app.post("/graph/schema")
async def get_schema(query : Request):
    req_info = await query.json()
    data = get_schema(req_info['env'])
    return {"schema": data}

def get_user_data(user_arn, account_id, env):
    custom_query = f"""
    Match (z:Policy)<-[y:has_access_to_policy]-(x:User)
    WHERE x.arn="{user_arn}" AND x.accountid="{account_id}"
    RETURN x, y, z

    UNION ALL

    Match (z:Group)<-[y:knows]-(x:User)
    WHERE x.arn="{user_arn}" AND x.accountid="{account_id}"
    RETURN x, y, z

    UNION ALL

    Match (z)<-[y:has_access_to_resource]-(x:Policy)<-[a:has_access_to_policy]-(b:User)
    WHERE b.arn="{user_arn}" AND b.accountid="{account_id}"
    RETURN x, y, z

    UNION ALL

    Match (z:Policy)<-[y:has_access_to_policy]-(x:Group)<-[b:knows]-(a:User)
    WHERE a.arn="{user_arn}" AND a.accountid="{account_id}"
    RETURN x, y, z

    UNION ALL

    Match (z)<-[y:has_access_to_resource]-(x:Policy)<-[d:has_access_to_policy]-(c:Group)<-[b:knows]-(a:User)
    WHERE a.arn="{user_arn}" AND a.accountid="{account_id}"
    RETURN x, y, z
    """  
    # custom_query = custom_query.replace("{user_arn}", user_arn)
    # custom_query = custom_query.replace("{account_id}", account_id)
    data = handler_query(custom_query, env)
    return data



def login_to_memgraph_ui_qa():
    burp0_url = "https://memgraphui-qa.clouddefenseai.com:443/auth/login"
    burp0_headers = {"Accept": "application/json, text/plain, */*", "Content-Type": "application/json", "Sec-Ch-Ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"", "Sec-Ch-Ua-Mobile": "?0", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36", "Sec-Ch-Ua-Platform": "\"Windows\"", "Origin": "https://memgraphui-dev.clouddefenseai.com", "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}
    burp0_json={"host": "memgraphdb-qa.clouddefenseai.com", "isEncrypted": False, "password": "%9q66sz8R", "port": 7687, "username": "cdefense"}
    req = requests.post(burp0_url, headers=burp0_headers, json=burp0_json)
    if req.status_code == 201:
        #print("Login successful")
        return req.json()['data']['token']
    else:
        #print("Login failed")
        return None


def login_to_memgraph_ui():
    burp0_url = "https://memgraphui-dev.clouddefenseai.com:443/auth/login"
    burp0_headers = {"Accept": "application/json, text/plain, */*", "Content-Type": "application/json", "Sec-Ch-Ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"", "Sec-Ch-Ua-Mobile": "?0", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36", "Sec-Ch-Ua-Platform": "\"Windows\"", "Origin": "https://memgraphui-dev.clouddefenseai.com", "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}
    burp0_json={"host": "memgraphdb-dev.clouddefenseai.com", "isEncrypted": False, "password": "eL2%97", "port": 7687, "username": "cdefense"}
    req = requests.post(burp0_url, headers=burp0_headers, json=burp0_json)
    if req.status_code == 201:
        #print("Login successful")
        return req.json()['data']['token']
    else:
        #print("Login failed")
        return None

def login_to_memgraph_ui_prod():
    burp0_url = "https://memgraphui-us.clouddefenseai.com:443/auth/login"
    burp0_headers = {"Accept": "application/json, text/plain, */*", "Content-Type": "application/json", "Sec-Ch-Ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"", "Sec-Ch-Ua-Mobile": "?0", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36", "Sec-Ch-Ua-Platform": "\"Windows\"", "Origin": "https://memgraphui-dev.clouddefenseai.com", "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}
    burp0_json={"host": "memgraphdb-us.clouddefenseai.com", "isEncrypted": False, "password": "G20B5%yT", "port": 7687, "username": "cdefense"}
    req = requests.post(burp0_url, headers=burp0_headers, json=burp0_json)
    if req.status_code == 201:
        #print("Login successful")
        return req.json()['data']['token']
    else:
        #print("Login failed")
        return None

def run_query_qa(query, token):
    burp0_url = "https://memgraphui-qa.clouddefenseai.com:443/api/queries"
    burp0_headers = {"Sec-Ch-Ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"", "Sec-Ch-Ua-Mobile": "?0", "Authorization": "Bearer {0}".format(token), "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}
    burp0_json={"query": "{0}".format(query)}
    req = requests.post(burp0_url, headers=burp0_headers, json=burp0_json)
    if req.status_code == 201:
        #print("Query successful")
        return json.dumps(req.json()['data'])
    else:
        #print(req.status_code)
        #print("Query failed")
        return None


def run_query(query, token):
    burp0_url = "https://memgraphui-dev.clouddefenseai.com:443/api/queries"
    burp0_headers = {"Sec-Ch-Ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"", "Sec-Ch-Ua-Mobile": "?0", "Authorization": "Bearer {0}".format(token), "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}
    burp0_json={"query": "{0}".format(query)}
    req = requests.post(burp0_url, headers=burp0_headers, json=burp0_json)
    if req.status_code == 201:
        #print("Query successful")
        return json.dumps(req.json()['data'])
    else:
        #print(req.status_code)
        #print("Query failed")
        return None

def run_query_prod(query, token):
    burp0_url = "https://memgraphui-us.clouddefenseai.com:443/api/queries"
    burp0_headers = {"Sec-Ch-Ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"", "Sec-Ch-Ua-Mobile": "?0", "Authorization": "Bearer {0}".format(token), "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}
    burp0_json={"query": "{0}".format(query)}
    req = requests.post(burp0_url, headers=burp0_headers, json=burp0_json)
    if req.status_code == 201:
        #print("Query successful")
        return json.dumps(req.json()['data'])
    else:
        #print(req.status_code)
        #print("Query failed")
        return None

def get_schema_from_dev(token):
    headers = {
        'authority': 'memgraphui-dev.clouddefenseai.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'authorization': 'Bearer {0}'.format(token),
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-kl-ajax-request': 'Ajax_Request',
    }

    response = requests.get('https://memgraphui-dev.clouddefenseai.com/api/schema', headers=headers)
    if response.status_code == 200:
        #print("Schema successful")
        return json.dumps(response.json()['data'])
    else:
        #print(response.status_code)
        #print("Schema failed")
        return None


def get_schema_from_qa(token):
    headers = {
        'authority': 'memgraphui-qa.clouddefenseai.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'authorization': 'Bearer {0}'.format(token),
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-kl-ajax-request': 'Ajax_Request',
    }

    response = requests.get('https://memgraphui-qa.clouddefenseai.com:443/api/schema', headers=headers)
    if response.status_code == 200:
        #print("Schema successful")
        return json.dumps(response.json()['data'])
    else:
        #print(response.status_code)
        #print("Schema failed")
        return None

def get_schema_from_prod(token):
    headers = {
        'authority': 'memgraphui-us.clouddefenseai.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'authorization': 'Bearer {0}'.format(token),
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-kl-ajax-request': 'Ajax_Request',
    }

    response = requests.get('https://memgraphui-us.clouddefenseai.com:443/api/schema', headers=headers)
    
    if response.status_code == 200:
        #print("Schema successful")
        return json.dumps(response.json()['data'])
    else:
        #print(response.status_code)
        #print("Schema failed")
        return None

def add_count_and_linked_ids_prod(records):
    data = json.loads(records)
    all_ids_set = set()
    account_id_user = []
    count = 0
    for i in data['records']:
       for m, n in i.items():
           if 'start' not in n:
               if count == 0:
                   if 'accountid' in n['properties']:
                       account_id_user.append(n['properties']['accountid'])
                       count += 1
               all_ids_set.add(n['id'])
    data_return = [all_ids_set, account_id_user]
    return data_return

def get_query_ids(ids, token, env, account_id_user):
    id_map = []
    lable_map = {}
    misconfig_critical = set()
    misconfig_high = set()
    misconfig_medium = set()
    misconfig_low = set()
    cve_critical = set()
    cve_high = set()
    cve_medium = set()
    cve_low = set()
    file_critical = set()
    file_high = set()
    file_medium = set()
    file_low = set()
    query = ""
    if len(account_id_user) > 0:
        query += "UNWIND {0} AS nodeId MATCH (n)-[r]-(m) WHERE id(n) = nodeId and n.accountid = \"{1}\" RETURN n, r, m".format(list(ids), account_id_user[0])
        #print(query)
    else:
        query += "UNWIND {0} AS nodeId MATCH (n)-[r]-(m) WHERE id(n) = nodeId RETURN n, r, m".format(list(ids))
    if env == "prod":
        records = run_query_prod(query, token)
        if records is not None:
                data = json.loads(records)
                for i in data['records']:
                        if len(i) == 3:
                            first = list(i.values())[0]

                            second = list(i.values())[1]
                            third = list(i.values())[2]
                            send = {first['id']: third['id']}
                            id_map.append(send)
                            lable_map[first['id']] = first['labels'][0]
                            lable_map[third['id']] = third['labels'][0]
                            if first['labels'][0] == "Misconfiguration":
                                severity = first['properties']['severity']
                                if severity == "critical":
                                    misconfig_critical.add(first['id'])
                                if severity == "high":
                                    misconfig_high.add(first['id'])
                                if severity == "medium":
                                    misconfig_medium.add(first['id'])
                                if severity == "low":
                                    misconfig_low.add(first['id'])
                            if third['labels'][0] == "Misconfiguration":
                                severity = third['properties']['severity']
                                if severity == "critical":
                                    misconfig_critical.add(third['id'])
                                if severity == "high":
                                    misconfig_high.add(third['id'])
                                if severity == "medium":
                                    misconfig_medium.add(third['id'])
                                if severity == "low":
                                    misconfig_low.add(third['id'])

                            if first['labels'][0] == "CVE":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        cve_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        cve_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        cve_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        cve_low.add(first['id'])
                            if third['labels'][0] == "CVE":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        cve_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        cve_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        cve_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        cve_low.add(first['id'])
                            
                            if first['labels'][0] == "File":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        file_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        file_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        file_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        file_low.add(first['id'])
                            if third['labels'][0] == "File":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        file_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        file_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        file_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        file_low.add(first['id'])
        
                new_dict = {}
                for entry in id_map:
                    key = list(entry.keys())[0]  
                    value = list(entry.values())[0]  
                    
                    if key not in new_dict:
                        new_dict[key] = []  
                    
                    new_dict[key].append(value) 

                
                Misconfiguration_dict = {
                    "Critical": list(misconfig_critical),
                    "High": list(misconfig_high),
                    "Medium": list(misconfig_medium),
                    "Low": list(misconfig_low),
                }
                Cve_dict = {
                    "Critical": list(cve_critical),
                    "High": list(cve_high),
                    "Medium": list(cve_medium),
                    "Low": list(cve_low),
                }
                File_dict = {
                    "Critical": list(file_critical),
                    "High": list(file_high),
                    "Medium": list(file_medium),
                    "Low": list(file_low),
                }

                final_data = {
                    "Connected Nodes": new_dict,
                    "Node Labels": lable_map,
                    "Misconfiguration Breakout": Misconfiguration_dict,
                    "CVE Breakout": Cve_dict,
                    "File Breakout": File_dict
                }
                return final_data
    if env == "dev":
        records = run_query(query, token)
        if records is not None:
                data = json.loads(records)
                if data:
                    
                    for i in data['records']:
                        if len(i) == 3:
                            first = list(i.values())[0]

                            second = list(i.values())[1]
                            third = list(i.values())[2]
                            send = {first['id']: third['id']}
                            id_map.append(send)
                            lable_map[first['id']] = first['labels'][0]
                            lable_map[third['id']] = third['labels'][0]
                            if first['labels'][0] == "Misconfiguration":
                                severity = first['properties']['severity']
                                if severity == "critical":
                                    misconfig_critical.add(first['id'])
                                if severity == "high":
                                    misconfig_high.add(first['id'])
                                if severity == "medium":
                                    misconfig_medium.add(first['id'])
                                if severity == "low":
                                    misconfig_low.add(first['id'])
                            if third['labels'][0] == "Misconfiguration":
                                severity = third['properties']['severity']
                                if severity == "critical":
                                    misconfig_critical.add(third['id'])
                                if severity == "high":
                                    misconfig_high.add(third['id'])
                                if severity == "medium":
                                    misconfig_medium.add(third['id'])
                                if severity == "low":
                                    misconfig_low.add(third['id'])

                            if first['labels'][0] == "CVE":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        cve_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        cve_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        cve_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        cve_low.add(first['id'])
                            if third['labels'][0] == "CVE":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        cve_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        cve_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        cve_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        cve_low.add(first['id'])
                            
                            if first['labels'][0] == "File":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        file_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        file_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        file_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        file_low.add(first['id'])
                            if third['labels'][0] == "File":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        file_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        file_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        file_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        file_low.add(first['id'])
        
                new_dict = {}
                for entry in id_map:
                    key = list(entry.keys())[0]  
                    value = list(entry.values())[0]  
                    
                    if key not in new_dict:
                        new_dict[key] = []  
                    
                    new_dict[key].append(value) 

                
                Misconfiguration_dict = {
                    "Critical": list(misconfig_critical),
                    "High": list(misconfig_high),
                    "Medium": list(misconfig_medium),
                    "Low": list(misconfig_low),
                }
                Cve_dict = {
                    "Critical": list(cve_critical),
                    "High": list(cve_high),
                    "Medium": list(cve_medium),
                    "Low": list(cve_low),
                }
                File_dict = {
                    "Critical": list(file_critical),
                    "High": list(file_high),
                    "Medium": list(file_medium),
                    "Low": list(file_low),
                }

                final_data = {
                    "Connected Nodes": new_dict,
                    "Node Labels": lable_map,
                    "Misconfiguration Breakout": Misconfiguration_dict,
                    "CVE Breakout": Cve_dict,
                    "File Breakout": File_dict
                }
                return final_data
    if env == "qa":
        records = run_query_qa(query, token)
        if records is not None:
                data = json.loads(records)
                if data:
                    
                    for i in data['records']:
                        if len(i) == 3:
                            first = list(i.values())[0]

                            second = list(i.values())[1]
                            third = list(i.values())[2]
                            send = {first['id']: third['id']}
                            id_map.append(send)
                            lable_map[first['id']] = first['labels'][0]
                            lable_map[third['id']] = third['labels'][0]
                            if first['labels'][0] == "Misconfiguration":
                                severity = first['properties']['severity']
                                if severity == "critical":
                                    misconfig_critical.add(first['id'])
                                if severity == "high":
                                    misconfig_high.add(first['id'])
                                if severity == "medium":
                                    misconfig_medium.add(first['id'])
                                if severity == "low":
                                    misconfig_low.add(first['id'])
                            if third['labels'][0] == "Misconfiguration":
                                severity = third['properties']['severity']
                                if severity == "critical":
                                    misconfig_critical.add(third['id'])
                                if severity == "high":
                                    misconfig_high.add(third['id'])
                                if severity == "medium":
                                    misconfig_medium.add(third['id'])
                                if severity == "low":
                                    misconfig_low.add(third['id'])

                            if first['labels'][0] == "CVE":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        cve_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        cve_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        cve_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        cve_low.add(first['id'])
                            if third['labels'][0] == "CVE":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        cve_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        cve_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        cve_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        cve_low.add(first['id'])
                            
                            if first['labels'][0] == "File":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        file_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        file_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        file_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        file_low.add(first['id'])
                            if third['labels'][0] == "File":
                                if 'severity' in first['properties']:
                                    severity = first['properties']['severity']
                                    if str(severity.lower()) == "critical":
                                        file_critical.add(first['id'])
                                    if str(severity.lower()) == "high":
                                        file_high.add(first['id'])
                                    if str(severity.lower()) == "medium":
                                        file_medium.add(first['id'])
                                    if str(severity.lower()) == "low":
                                        file_low.add(first['id'])
        
                new_dict = {}
                for entry in id_map:
                    key = list(entry.keys())[0]  
                    value = list(entry.values())[0]  
                    
                    if key not in new_dict:
                        new_dict[key] = []  
                    
                    new_dict[key].append(value) 

                
                Misconfiguration_dict = {
                    "Critical": list(misconfig_critical),
                    "High": list(misconfig_high),
                    "Medium": list(misconfig_medium),
                    "Low": list(misconfig_low),
                }
                Cve_dict = {
                    "Critical": list(cve_critical),
                    "High": list(cve_high),
                    "Medium": list(cve_medium),
                    "Low": list(cve_low),
                }
                File_dict = {
                    "Critical": list(file_critical),
                    "High": list(file_high),
                    "Medium": list(file_medium),
                    "Low": list(file_low),
                }

                final_data = {
                    "Connected Nodes": new_dict,
                    "Node Labels": lable_map,
                    "Misconfiguration Breakout": Misconfiguration_dict,
                    "CVE Breakout": Cve_dict,
                    "File Breakout": File_dict
                }
                return final_data

def handler_query(query, env):
  
    if env == "prod":
        token = login_to_memgraph_ui_prod()
        if token is not None:
            records = run_query_prod(str(query), token)
            if records is not None:
               
                data_got = add_count_and_linked_ids_prod(records)
                ids_set = data_got[0]
                account_id_user = data_got[1]
                if ids_set is not None and len(account_id_user) > 0:
                    all_ids_records = get_query_ids(ids_set, token, env, account_id_user)
                    if all_ids_records is not None:
                        data_to_send = {
                            "Others":  all_ids_records,
                            "query": records  
                        }
                        return data_to_send
                if ids_set is not None:
                    all_ids_records = get_query_ids(ids_set, token, env, account_id_user)
                    if all_ids_records is not None:
                        data_to_send = {
                            "Others":  all_ids_records,
                            "query": records  
                        }
                        return data_to_send
 
    


    
    if env == "qa":
        token = login_to_memgraph_ui_qa()
        if token is not None:
            records = run_query_qa(str(query), token)
            if records is not None:
                data_got = add_count_and_linked_ids_prod(records)
                ids_set = data_got[0]
                account_id_user = data_got[1]
                if ids_set is not None and len(account_id_user) > 0:
                    all_ids_records = get_query_ids(ids_set, token, env, account_id_user)
                    if all_ids_records is not None:
                        data_to_send = {
                            "Others":  all_ids_records,
                            "query": records
                            
                        }
                        return data_to_send
                if ids_set is not None:
                    all_ids_records = get_query_ids(ids_set, token, env, account_id_user)
                    if all_ids_records is not None:
                        data_to_send = {
                            "Others":  all_ids_records,
                            "query": records  
                        }
                        return data_to_send
   

                
    if env == "dev":
        token = login_to_memgraph_ui()
        if token is not None:
            records = run_query(str(query), token)
            if records is not None:
                data_got = add_count_and_linked_ids_prod(records)
                ids_set = data_got[0]
                account_id_user = data_got[1]
                if ids_set is not None and len(account_id_user) > 0:
                    all_ids_records = get_query_ids(ids_set, token, env, account_id_user)
                    if all_ids_records is not None:
                        data_to_send = {
                            "Others":  all_ids_records,
                            "query": records
                            
                        }
                        return data_to_send
                if ids_set is not None:
                    all_ids_records = get_query_ids(ids_set, token, env, account_id_user)
                    if all_ids_records is not None:
                        data_to_send = {
                            "Others":  all_ids_records,
                            "query": records  
                        }
                        return data_to_send
                

def get_schema(env):
    if env == "prod":
        token = login_to_memgraph_ui_prod()
        if token is not None:
            records = get_schema_from_prod(token)
            if records is not None:
                return records
        
    if env == "qa":
        token = login_to_memgraph_ui_qa()
        if token is not None:
            records = get_schema_from_qa(token)
            if records is not None:
                return records
            
        
    if env == "dev":
        token = login_to_memgraph_ui()
        if token is not None:
            records = get_schema_from_dev(token)
            if records is not None:
                return records
            

handler = Mangum(app)