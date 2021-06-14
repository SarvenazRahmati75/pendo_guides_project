import boto3
import hvac
import os

class Secrets:

    __vaultClient = None
    config = None

    def __init__(self):
        self.vaultClient = client = hvac.Client()

    def __init__(self, config):
        self.config = config
        self.vaultClient = client = hvac.Client(config.vaultUrl)

    def __authenticate_vault(self):
        if not self.vaultClient.is_authenticated():
            print("Authenticating to Vault")
            session = boto3._get_default_session() # new for local setup
            credentials = session.get_credentials()


            self.vaultClient.auth_aws_iam(
                credentials.access_key
                ,credentials.secret_key
                ,credentials.token
                ,mount_point=self.config.vaultAWSAuthMountPath
                ,role=self.config.vaultAWSAuthRole
            )

    def get(self,name,engine="secret"):
        self.__authenticate_vault()

        print("Trying to acquire lease on secret "+engine+"/"+name)
        response = self.vaultClient.read(engine+"/"+name)
        if "data" in response:
            return response["data"]

        return None

    def list(self,engine="secret"):
        self.__authenticate_vault()

        print("Trying to list secrets in "+engine+"/")
        response = self.vaultClient.list(engine)

        if "data" in response:
            return response["data"]

        return None
