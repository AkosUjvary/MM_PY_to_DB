import logging
import azure.functions as func
import requests

url="https://mm-dbloader.azurewebsites.net/api/mm-dbloader-code?code=XxvLb3Rr4pnC-JkK868pp2SG6rOSTO3ltQxz-tpWCMCyAzFuLvpkpQ==&adhoc=1"     
     
def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    r = requests.post(url)                 

    
