import json
import boto3
from botocore.credentials import Credentials
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from fastapi import HTTPException


class AWSOperations:
    def connect_to_iam_with_assumed_role(self, aws_credentials):
        # Create a new session with the temporary credentials
        session = boto3.Session(
            aws_access_key_id=aws_credentials.access_key,
            aws_secret_access_key=aws_credentials.secret_key,
            aws_session_token=aws_credentials.token,
        )
        # Use the new session to connect to IAM
        iam_client = session.client("iam")
        return iam_client

    def get_iam_connection(self):
        try:
            with open("config.json", "r") as f:
                data = json.loads(f.read())
                if data["accountType"] == "CloudFormation":
                    aws_credentials = self.get_assume_role_credentials(data)
                    iam_client = self.connect_to_iam_with_assumed_role(aws_credentials)
                elif data["accountType"] == "Credential":
                    iam_client = self.connect(
                        "iam", data["aws_access_key_id"], data["aws_secret_access_key"]
                    )
                return iam_client
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise HTTPException(
                status_code=500, detail=f"Error reading or parsing config.json: {e}"
            )

        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            raise HTTPException(status_code=500, detail=f"Error connecting to IAM: {e}")

    def connect(self, serviceName, aws_access_key_id, aws_secret_access_key):
        s3Client = boto3.client(
            serviceName,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        return s3Client

    def connect_to_s3_with_assumed_role(self, aws_credentials):
        # Create a new session with the temporary credentials
        session = boto3.Session(
            aws_access_key_id=aws_credentials.access_key,
            aws_secret_access_key=aws_credentials.secret_key,
            aws_session_token=aws_credentials.token,
        )
        # Use the new session to connect to S3
        s3_client = session.client("s3")
        return s3_client

    def getConnection(self):
        try:
            with open("config.json", "r") as f:
                data = json.loads(f.read())
                if data["accountType"] == "CloudFormation":
                    aws_credentials = self.get_assume_role_credentials(data)
                    s3_client = self.connect_to_s3_with_assumed_role(aws_credentials)
                elif data["accountType"] == "Credential":
                    s3_client = self.connect(
                        "s3", data["aws_access_key_id"], data["aws_secret_access_key"]
                    )
                return s3_client
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise HTTPException(
                status_code=500, detail=f"Error reading or parsing config.json: {e}"
            )
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            raise HTTPException(status_code=500, detail=f"Error connecting to S3: {e}")

    def get_assume_role_credentials(self, account):
        try:
            # Create an STS client with the IAM user's access and secret keys
            sts_client = boto3.client(
                "sts",
                aws_access_key_id=account["aws_access_key_id"],
                aws_secret_access_key=account["aws_secret_access_key"],
            )

            # Assume the IAM role
            response = sts_client.assume_role(
                RoleArn=account["role_arn"],
                RoleSessionName="Assume_Role_Session",
                DurationSeconds=43200,
                ExternalId=account["externalid"],
            )

            # Extract the temporary credentials
            creds = response["Credentials"]
            session_credentials = boto3.Session(
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            ).get_credentials()

            # Create an AwsCredentials object with the temporary credentials
            aws_credentials = Credentials(
                access_key=session_credentials.access_key,
                secret_key=session_credentials.secret_key,
                token=session_credentials.token,
            )

            return aws_credentials

        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            raise HTTPException(status_code=500, detail=f"Error assuming role: {e}")
