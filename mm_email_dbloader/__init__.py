import json
import logging
import azure.functions as func


def main(req: func.HttpRequest, sendGridMessage: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
 
    
    value = "No new film was loaded to the database in the last process."

    message = {
        "personalizations": [ {
          "to": [{
            "email": "ujvary.akos@gmail.com"
            }]}],
        "subject": "MM DB Loader",
        "content": [{
            "type": "text/plain",
            "value": value }]}

    sendGridMessage.set(json.dumps(message))

    return func.HttpResponse("Email is sent.")
    