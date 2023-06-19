import json
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from schemas import Script
from runner import runner
from datetime import datetime

app = FastAPI()

@app.post("/run")
def run_script(script: Script):
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Payload:")
    print(json.dumps(script.dict(), indent=4))

    try:
        resp = runner(script.accountType, script.accessKey, script.secretKey, script.accountId, script.days
                      , script.bucketData, script.roleArn, script.externalId)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"accountId": resp['accountId'], "generatedPolicies": resp['generatedPolicies'], "consolidatedPolicies": resp['consolidatedPolicies'], "excessivePolicies": resp['excessivePolicies']}

