import datetime
import logging
import requests

import azure.functions as func

url="https://mm-dbloader.azurewebsites.net/api/mm-dbloader-code?code=XxvLb3Rr4pnC-JkK868pp2SG6rOSTO3ltQxz-tpWCMCyAzFuLvpkpQ==&adhoc=0"

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    r = requests.post(url)
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
