from pydantic import BaseModel
from typing import Dict


class Script(BaseModel):
    accountType: str
    accessKey: str
    secretKey: str
    externalId: str
    roleArn: str
    accountId: str
    days: int
    bucketData: Dict[str, str]
