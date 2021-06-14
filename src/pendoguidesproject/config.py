class SecretsConfig:
    def __init__(self, vaultUrl, vaultAWSAuthMountPath, vaultAWSAuthRole):
        self.vaultUrl = vaultUrl
        self.vaultAWSAuthMountPath = vaultAWSAuthMountPath
        self.vaultAWSAuthRole = vaultAWSAuthRole

    def setVaultUrl(self,url):
        self.vaultUrl=url

    def setAWSAuthMountPath(self,path):
        self.vaultAWSAuthMountPath=path

    def setAWSAuthRole(self,role):
        self.vaultAWSAuthRole=role