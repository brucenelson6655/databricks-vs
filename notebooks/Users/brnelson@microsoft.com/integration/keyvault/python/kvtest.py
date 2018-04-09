# Databricks notebook source
dbutils.widgets.text("MyCID","xxxxxxxx", "My Client ID")
dbutils.widgets.text("MySID","xxxxxxxx", "My Secret ID")
dbutils.widgets.text("MyTID","xxxxxxxx", "My Tenant ID")
dbutils.widgets.text("MyDir","xxxxxxxx", "My Data Folder")
dbutils.widgets.text("MyKV","xxxxxxxx", "My Key Vault")

# COMMAND ----------

# MAGIC %md
# MAGIC Key vault test

# COMMAND ----------

from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.common.credentials import ServicePrincipalCredentials
import json

credentials = None

workingdir = dbutils.widgets.get("MyDir")
KEY_VAULT_URI = 'https://'+dbutils.widgets.get("MyKV")+'.vault.azure.net/'



def auth_callack(server, resource, scope):
    credentials = ServicePrincipalCredentials(
        client_id = dbutils.widgets.get("MyCID"), # '7a858658-ba63-4d9e-8ca3-9d8b810d596c',
        secret = dbutils.widgets.get("MySID"), # 'g0DeEFCo8DP5Cj/m84peUDR+66HlMZCdlemQdfcYk1Q=',
        tenant = dbutils.widgets.get("MyTID"), # '72f988bf-86f1-41af-91ab-2d7cd011db47',
        resource = 'https://vault.azure.net'
    )
   # dbutils.widgets.removeAll()
    token = credentials.token
    return token['token_type'], token['access_token']

client = KeyVaultClient(KeyVaultAuthentication(auth_callack))

# key_bundle = client.get_key(vault_url, key_name, key_version)
# json_key = key_bundle.key

# COMMAND ----------

secrets = client.get_secrets(KEY_VAULT_URI)
myversion = "0"
for sec in secrets:
  if sec.id.rsplit('/')[4] == workingdir: 
    print(sec.id.rsplit('/')[4] + ' Match')
    versions = client.get_secret_versions(KEY_VAULT_URI, workingdir)
    for version in versions:
      if version.attributes.enabled:
         print(version)
         myversion = version.id.rsplit('/')[5] 
         print(myacversion)
  else:
    print(sec.id.rsplit('/')[4] + ' No Match')
    
if myversion == "0":
  dbutils.notebook.exit("no secret found")

# COMMAND ----------

#grab a secret from the vault 
mysecret = client.get_secret(KEY_VAULT_URI,workingdir,myversion)
# get_secret params (URL,secret,version)
mysecretjs = mysecret.value

# COMMAND ----------

print(mysecretjs)

# COMMAND ----------

stor = json.loads(mysecretjs)

# COMMAND ----------

print(stor["Mycontainer"])

# COMMAND ----------

stcon = stor["Mycontainer"]
stcon = "prod"
stacct = stor["Mystorageaccount"]
stdir = stor["Mydirectory"]
destdir = stor["Mydestdir"]
stkey = stor["Mystoragekey"]

storageuri = "wasbs://"+stcon+"@"+stacct+".blob.core.windows.net/"+stdir

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+stacct+".blob.core.windows.net",
  stkey)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/demo")
dbutils.fs.mkdirs("/mnt/demo/"+destdir)

# COMMAND ----------

dbutils.fs.rm("/mnt/demo/"+destdir+"/*", recurse = True) 
if (dbutils.fs.cp(storageuri,"/mnt/demo/"+destdir+"/", recurse = True)) :
   print("File Copied Successfully")


# COMMAND ----------

dbutils.fs.ls("/mnt/demo/"+destdir+"/")