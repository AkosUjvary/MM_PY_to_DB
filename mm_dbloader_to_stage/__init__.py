import os
from azure.storage.blob import BlobServiceClient  
import azure.functions as func
import csv


def importCSV_blob(importCSV):
    newLoD=list(dict())

    container_client = blob_service.get_container_client(container= container_name) 
    readerOrig=container_client.download_blob(importCSV+".csv").readall().decode("utf-8").splitlines()

    reader = csv.reader(readerOrig, delimiter=';')
    headerCnt=1        
    for param in reader:   
        if headerCnt == 1:
           header=param
           colCnt=len(param)
        if headerCnt>1: 
                newLoD.append({header[i]: param[i] for i in range(colCnt)})
        headerCnt=headerCnt+1
    return newLoD

def mapping(map_lod, source_lod):
    newLoD=list(dict())
    for source_row in source_lod:
        newLoD.append({map_lod[i]["TARGET"]: source_row[map_lod[i]["FROM"]] if map_lod[i]["TYPE"]=="T" else map_lod[i]["FROM"]
                                                 for i in range(len(map_lod))})
    return newLoD

# config:
blob_service = BlobServiceClient(account_url="https://mmstrgaccount.blob.core.windows.net/", credential="?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-14T16:40:16Z&st=2022-10-14T08:40:16Z&spr=https&sig=RZbu%2BSWbiXkEFm%2FoMShfcyRetD%2BemNeGTIdt1%2BpD5nA%3D")
container_name="mmdbloader"

def main(myblob: func.InputStream, sqlstage: func.Out[func.SqlRowList]) -> func.HttpResponse:
#def main(myblob: func.InputStream, sqlstage: func.Out[func.SqlRowList]):

    deltaFilmList=importCSV_blob('load_to_DB/load_filmlist_delta_to_db')

    stage_map=importCSV_blob('load_to_DB/stage_mapping')
    deltaFilmList_structured=mapping(stage_map, deltaFilmList)
    sqlstage.set(func.SqlRowList(deltaFilmList_structured))
   
    
       
