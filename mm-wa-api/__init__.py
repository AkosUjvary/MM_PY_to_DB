import csv
from datetime import datetime
import json
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient  
import os

def exportCSV_blob(filename, listDict, id_flg, id_nm):  
    delimiter=";"          
    header = delimiter.join(list(listDict[0].keys())) 
    header = id_nm+delimiter+header if id_flg=="Y" and list(listDict[0].keys())[0]!=id_nm  else header
    writer=header    
      
    maxId=0

    if id_flg == "Y":
        for dict in listDict:
            if id_nm in dict.keys():
                if int(dict[id_nm])>maxId:
                    maxId=int(dict[id_nm])


    for dict in listDict:       
        if  id_flg=="Y" and id_nm not in dict.keys():
                    maxId=maxId+1;
                    writer=  writer+'\n'+str(maxId)+delimiter+delimiter.join(list(str(x) for x in dict.values())) 
        else:
            writer=writer+'\n'+delimiter.join(list(str(x) for x in dict.values()))


    blob_client = blob_service.get_blob_client(container=container_name, blob=filename+".csv")
    blob_client.upload_blob(writer, blob_type="BlockBlob", overwrite=True)
 
def importCSV_blob(importCSV):
    newDict=list(dict())

    container_client = blob_service.get_container_client(container= container_name) 
    readerOrig=container_client.download_blob(importCSV+".csv").readall().decode("utf-8").splitlines()

    reader = csv.reader(readerOrig, delimiter=';')
    headerCnt=1        
    for param in reader:   
        if headerCnt == 1:
           header=param
           colCnt=len(param)
        if headerCnt>1: 
                newDict.append({header[i]: cleanQuot(param[i]) for i in range(colCnt)})
        headerCnt=headerCnt+1
    return json.dumps(newDict)

def listCSVs_blob(folder): 
    container_client =blob_service.get_container_client(container=container_name)
    fileList=list(str(x.name).split(folder+'/')[1].replace(".csv", "") for x in container_client.list_blobs() if folder+'/' in str(x.name))
    return json.dumps(fileList)

def delete_blob(filename):
    container_client=blob_service.get_container_client(container=container_name)
    container_client.delete_blob(filename+".csv")
    return 1

def findBlobs(starts_with):
    container_client = blob_service.get_container_client(container= container_name) 
    blobs=list(x.name.replace(".csv", "") for x in container_client.list_blobs(name_starts_with=starts_with))    
    return blobs

def cleanQuot(str):
    cleaned_str=str.replace(r"\"", "\"")
    return cleaned_str

# config:

conn_str = os.environ["MM_STORAGE_CONNECTION_STRING"]
blob_service = BlobServiceClient.from_connection_string(conn_str)
container_name="mmdbloader"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
        
    fnc = req.params.get('fnc')

    if fnc=="listfiles":
        folder_name = req.get_json().get('folder')
        return_json=listCSVs_blob(folder_name)
        return func.HttpResponse(return_json, status_code=200)

    elif fnc=="lparam":        
        return_json=importCSV_blob("MMP_processes")
        return func.HttpResponse(return_json, status_code=200)        

    elif fnc=="lmapping":        
        return_json=importCSV_blob("load_to_DB/stage_mapping")
        return func.HttpResponse(return_json, status_code=200)

    elif fnc=="lsched":        
        return_json=importCSV_blob("load_to_DB/scheduler")
        return func.HttpResponse(return_json, status_code=200)

    elif fnc=="lcorrections":   
        return_json = importCSV_blob("load_to_DB/load_filmlist_to_db_corr") if len(findBlobs("load_to_DB/load_filmlist_to_db_corr"))>0 else json.dumps(list(dict()))              
        return func.HttpResponse(return_json, status_code=200)  
       
    elif fnc=="sparam":    
        req_json=req.get_json() 
        new_mmp_process=json.loads(req_json)
        exportCSV_blob("MMP_processes",new_mmp_process, "N", "")
        exportCSV_blob("MMP_processes_info", [{"last_update":str(datetime.now().strftime("%Y.%m.%d %H:%M:%S"))}], "N", "")       
        return func.HttpResponse("MMP_processes.csv saved.", status_code=200) 

    elif fnc=="smapping":    
        req_json=req.get_json() 
        new_map=json.loads(req_json)
        exportCSV_blob("load_to_DB/stage_mapping",new_map, "N", "")
        return func.HttpResponse("stage_mapping.csv saved.", status_code=200) 

    elif fnc=="ssched":    
        req_json=req.get_json() 
        new_scheduler=json.loads(req_json)
        exportCSV_blob("load_to_DB/scheduler",new_scheduler, "N", "")
        return func.HttpResponse("scheduler.csv saved.", status_code=200) 


    elif fnc=="saddedcorrs":    
        req_json=req.get_json() 
        new_map=json.loads(req_json)
        exportCSV_blob("load_to_DB/load_filmlist_to_db_corr",new_map, "Y", "ID")
        return func.HttpResponse("load_filmlist_to_db_corr.csv saved.", status_code=200) 

    elif fnc=="scorrections":    
        req_json=req.get_json() 
        new_map=json.loads(req_json)
        exportCSV_blob("load_to_DB/load_filmlist_to_db_corr",new_map, "N", "")
        return func.HttpResponse("load_filmlist_to_db_corr.csv saved.", status_code=200) 

    elif fnc=="viewfile":
        file_name = req.get_json().get('file')
        return_json=importCSV_blob(file_name)
        return func.HttpResponse(return_json, status_code=200) 

    elif fnc=="delblob":
        file_name=req.get_json().get('file')   
        delete_blob(file_name)
        return func.HttpResponse(file_name+" is deleted.", status_code=200) 

    else:
        return func.HttpResponse("NNo function found.",status_code=500)
