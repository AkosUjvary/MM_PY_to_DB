from math import sqrt
import json
from operator import concat
import time
import requests
import csv
import datetime
from datetime import datetime, timedelta
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient  
 

def exportCSV_blob(filename, listDict):  
    delimiter=";"          
    header = delimiter.join(list(listDict[0].keys()))
    writer=header
    for dict in listDict:
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
                newDict.append({header[i]: param[i] for i in range(colCnt)})
        headerCnt=headerCnt+1
    return newDict

def existsCSV_blob(existCSV):
    container_client = blob_service.get_container_client(container= container_name) 
    folder='/'.join(existCSV.split('/')[:-1])    
    fileList=list(str(x.name).split(folder+'/')[1] for x in container_client.list_blobs() if folder+'/' in str(x.name))
    file=existCSV.split('/')[-1]+".csv"
    rtnExists=file in fileList
    return rtnExists

def logger(pr_id, msg):
    rtn={"process id": pr_id, "step message in detail": msg, "timestamp of the step": str((datetime.now()+ timedelta(hours=1)).strftime("%Y.%m.%d %H:%M:%S"))}
    return rtn


def getFeatureFilmCountByYear(year, country):
    countryInURL="" if country=="global" else "&countries="+country
 
    time.sleep(0.05)
   
    req = requests.get(f"http://www.imdb.com/search/title/?title_type=feature&release_date="+str(year)+"-01-01,"+str(year)+"-12-31&count=50&view=simple&sort=num_votes,desc"+countryInURL)

    html_bytes=req.content
    html = html_bytes.decode("utf-8")

    strToFind_1=r"""<div class="desc">"""
    pos1=html.find(strToFind_1)+len(strToFind_1)

    step_1=html[pos1:pos1+100]
    pos2=step_1.find("<span")+len("<span")
    pos3=step_1.find("title")

    step_2=step_1[pos2:pos3]

    i=-2
    result=""
    while step_2[i].isnumeric() or step_2[i]==",":
        result=concat(result,step_2[i].replace(',', ''))
        i=i-1

    rtn=int(result[::-1])
    return rtn
 

def getFeatureFilm_Title_ID_ByYear(year, country, title_lang, filmLimit, sort_type):
    countryInURL="" if country=="" or country=="global" else "&countries="+country
    title_lang=title_lang+"-"+title_lang
     
    movieList=list(dict())

    pageStart_counter=1
    film_counter=1
    while film_counter<=filmLimit:
        time.sleep(0.05)
        url="http://www.imdb.com/search/title/?title_type=feature&release_date="+str(year)+"-01-01,"+str(year)+"-12-31&count=250&sort="+sort_type+",desc&view=simple"+countryInURL+"&start="+str(pageStart_counter)+"&ref_=adv_nxt"
        req = requests.get(url,
                        headers={'Accept-Language': ''+title_lang+''})
        html_bytes=req.content
        html = html_bytes.decode("utf-8")

        strToFind_1_1=r"""<span class="lister-item-header">"""
        countFilms=html.count(strToFind_1_1)

        page_film_counter=1
        while page_film_counter<=countFilms and film_counter<=filmLimit:
            find_1_1=html.find(strToFind_1_1)+len(strToFind_1_1)

            strToFind_1_2="lister-item-year text-muted unbold"
            find_1_2=html.find(strToFind_1_2)

            slice_1=html[find_1_1:find_1_2]

            strToFind_2_1=r"""<a href="/"""
            find_2_1=slice_1.find(strToFind_2_1)+len(strToFind_2_1)

            strToFind_2_2="</a"
            find_2_2=slice_1.find(strToFind_2_2)

            slice_2=slice_1[find_2_1:find_2_2]

            strToFind_3_1="title/"
            find_3_1=slice_2.find(strToFind_3_1)+len(strToFind_3_1)

            strToFind_3_2="\n"
            find_3_2=slice_2.find(strToFind_3_2)-2
        
            find_4_1=slice_2.find(strToFind_3_2)+2

            imdb_id=slice_2[find_3_1:find_3_2]
            title=slice_2[find_4_1::]  
            clnd_title=cleanStr(title) 

            html=html[find_1_2+len(strToFind_1_2)::]
            movieList.append({"imdbId": imdb_id, "title" : clnd_title})
            film_counter=film_counter+1
            page_film_counter=page_film_counter+1
        pageStart_counter=pageStart_counter+250
    return movieList

def cleanStr(str):   
    apos_str= str.replace("&apos;", "'")
    amp_str=apos_str.replace("&amp;", "&")
    quot_orig_str=amp_str.replace("\"", r"\"")
    quot_str=quot_orig_str.replace("&quot;", r"\"")

    clnd_str=quot_str
    return clnd_str

def getFeatureFilm_keywords_origTitle_ById(movieListArr):
   
    movie_KW_origTitle_List=list(dict())
    err_count=0

    for movie in movieListArr:
        url="https://www.imdb.com/title/"+movie["imdbId"]+"/?ref_=fn_al_tt_0"
        req = requests.get(url, headers={
            'Accept-Language': 'HU-hu',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            })
        html_bytes=req.content
        html = html_bytes.decode("utf-8")

        strToFind_1_1=r"""{"@context":"https://schema.org"""
        findPos_1_1=html.find(strToFind_1_1)+len(strToFind_1_1)
        strToFind_1_2=r"}</script>" 
        findPos_1_2=findPos_1_1+html[findPos_1_1::].find(strToFind_1_2)
        slice_1=html[findPos_1_1:findPos_1_2]

        strToFind_2_1=r"""name":"""
        findPos_2_1=slice_1.find(strToFind_2_1)+len(strToFind_2_1)+1
        strToFind_2_2="""\",\""""
        findPos_2_2=findPos_2_1+slice_1[findPos_2_1+2::].find(strToFind_2_2)+2
        orig_title=slice_1[findPos_2_1:findPos_2_2]

        strToFind_3_1="keywords\":\""
        findPos_3_1=slice_1.find(strToFind_3_1)
        if findPos_3_1>-1:
            findPos_3_1=findPos_3_1+len(strToFind_3_1)
            strToFind_3_2=strToFind_2_2
            findPos_3_2=findPos_3_1+slice_1[findPos_3_1::].find(strToFind_3_2)
            keywords=slice_1[findPos_3_1:findPos_3_2]
            #keywordsList=keywords.split(',')
            err_count=0;
        else:
            #keywordsList=list()
            keywords=str()
            err_count=err_count+1

        movie_KW_origTitle_List.append({"imdbId": movie["imdbId"], "title": movie["title"],  "orig_title" : cleanStr(orig_title), "keywordsList" : cleanStr(keywords)})
 
    return {"err_count":err_count, "rtn":movie_KW_origTitle_List}

def filmLoaderCalc(w,limit,filmCountList):
    maxValue=max(filmCountList, key=lambda x:x['filmCount'])['filmCount']
    filmCountBiasedList=list(dict())
    for film in filmCountList:
        currCount=film["filmCount"]
        biasedCount = sqrt(currCount) * ((currCount+(w*(maxValue-currCount)))/(maxValue))
        filmCountBiasedList.append({"year": film["year"], "filmCount": film["filmCount"], "filmCountBiased": biasedCount})

    sumBiasedValue=sum(film['filmCountBiased'] for film in filmCountBiasedList)

    filmCountBiasedListLimit=list(dict())
    for film in filmCountBiasedList:
        if film["year"]!=datetime.now().year:
            filmCountBiasedLimit=round(film["filmCountBiased"] * (limit/sumBiasedValue))
        else:
            filmCountBiasedLimit=limit/12/4*datetime.now().isocalendar().week 
        filmCountBiasedListLimit.append({"year": film["year"], "filmCount": film["filmCount"], "filmCountBiased": film["filmCountBiased"], "filmCountBiasedLimit": filmCountBiasedLimit})


    return filmCountBiasedListLimit

def omdb(filmlist):
   
    filmlist_omdb = list(dict())
    for movie in filmlist:
        url="http://www.omdbapi.com/?i="+movie["imdbId"]+"&apikey=63b34753"
        req = requests.get(url)
        html_bytes=req.content
        html = html_bytes.decode("utf-8")
        omdbJSON = json.loads(html)    
        if omdbJSON["Response"]=="True":
            filmlist_omdb.append({"imdbId": movie["imdbId"], "title": movie["title"],  "orig_title" : movie["orig_title"], "keywordsList" : movie["keywordsList"],
                                "omdb_ReleaseYear": omdbJSON["Year"],
                                "omdb_Runtime": omdbJSON["Runtime"],
                                "omdb_Genre": omdbJSON["Genre"],
                                "omdb_Director": cleanStr(omdbJSON["Director"]),
                                "omdb_Writer": cleanStr(omdbJSON["Writer"]),
                                "omdb_Actors": cleanStr(omdbJSON["Actors"]),
                                "omdb_Plot": cleanStr(omdbJSON["Plot"]).replace(";", ","),
                                "omdb_Language": omdbJSON["Language"],
                                "omdb_Country": omdbJSON["Country"],
                                "omdb_ReleasedDate": omdbJSON["Released"].replace("N/A", "01 Apr "+str(omdbJSON["Year"])),
                                "omdb_Awards": omdbJSON["Awards"],
                                "omdb_Poster": omdbJSON["Poster"],
                                "omdb_Rating_IMDB": omdbJSON["imdbRating"],
                                "omdb_Rating_TOMATOES": str(omdbJSON["Ratings"][1]["Value"]).replace("%", "") if len(omdbJSON["Ratings"])==3 else '0', #tomatometer -> remove percent sign and replace '' with 0
                                "omdb_Rating_METASCORE": str(omdbJSON["Metascore"]).replace("N/A", '0') if len(omdbJSON["Metascore"])>0 else '0', #replace N/A with 0 and '' with 0
                                "omdb_imdbVotes": str(omdbJSON["imdbVotes"]).replace(",", "") #imdb_votes -> remove colon
                        })    
    
    return filmlist_omdb


def findFilmLists(starts_with):
    container_client = blob_service.get_container_client(container= container_name) 
    filmlists=list(x.name.replace(".csv", "") for x in container_client.list_blobs(name_starts_with=starts_with))    
    return filmlists

def DB_Loader_lists():
    filmListsCSV=findFilmLists("output/filmlist_omdb/filmlist_omdb_")
    finalFilmList = importCSV_blob("load_to_DB/load_filmlist_to_db") if len(findFilmLists("load_to_DB/load_filmlist_to_db.csv"))>0 else list(dict()) 
    finalFilmListDelta=list(dict())

    filmListCorr = importCSV_blob("load_to_DB/load_filmlist_to_db_corr") if len(findFilmLists("load_to_DB/load_filmlist_to_db_corr.csv"))>0 else list(dict()) 
    

    filmListsCSV_cleaned=list(dict())

    for filmListCSV in filmListsCSV:
        filmList=importCSV_blob(filmListCSV)

        filmList_CLND_IMDBIDs=[x['imdbId'] for x in filmListsCSV_cleaned]

        for film in filmList:
            if film["imdbId"] not in filmList_CLND_IMDBIDs:
                film['load_type']="STNDRD"
                filmListsCSV_cleaned.append(film)

    if not finalFilmList:
        finalFilmList=filmListsCSV_cleaned
        finalFilmListDelta=filmListsCSV_cleaned
    else:
        filmList_FINAL_IMDBIDs=[x['imdbId'] for x in finalFilmList]

        for film in filmListsCSV_cleaned:
            if film["imdbId"] not in filmList_FINAL_IMDBIDs:
                finalFilmList.append(film)
                finalFilmListDelta.append(film)

    if filmListCorr:
        for filmCorr in filmListCorr:
            del filmCorr["ID"]
            filmCorr['load_type']="CORR"
            finalFilmListDelta.append(filmCorr)


    if finalFilmList: exportCSV_blob('load_to_DB/load_filmlist_to_db', finalFilmList)
    if finalFilmListDelta: exportCSV_blob('load_to_DB/load_filmlist_delta_to_db', finalFilmListDelta)


# config:
blob_service = BlobServiceClient(account_url="https://mmstrgaccount.blob.core.windows.net/", credential="?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-14T16:40:16Z&st=2022-10-14T08:40:16Z&spr=https&sig=RZbu%2BSWbiXkEFm%2FoMShfcyRetD%2BemNeGTIdt1%2BpD5nA%3D")
container_name="mmdbloader"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    isAdhocRun = 1 if req.params.get('adhoc')=='1' else 0
    log=list(dict())
    new_processes=list(dict())
    processes=importCSV_blob('MMP_processes')
    err_flg="N"

    processesToRun=list()
    if (isAdhocRun==0):
        allScheduledProcesses=importCSV_blob("load_to_DB/scheduler")
        processesToRun=[x["Process ID"] for x in allScheduledProcesses 
            if                
                     datetime.strptime(x["Schedule"], "%H:%M").time()<=(datetime.now()+ timedelta(hours=1)).time()
                and (datetime.now()+ timedelta(hours=1)).time()<=(datetime.strptime(x["Schedule"], "%H:%M")+ timedelta(minutes=5)).time()
            ]


    for process in processes:
        if (isAdhocRun==1 and process["Adhoc"]=='Y' and (process["Status"]=='N' or process["Status"]=='P') and err_flg=="N") or (process["Process ID"] in processesToRun and process["Adhoc"]=='N' and (process["Status"]=='N' or process["Status"]=='D') and err_flg=="N"):        
            yearFrom=int(process["Year From"])
            yearTo=int(process["Year To"])
            country="global" if process["Country"]=="" else process["Country"]
            title_lang=process["Title Language"]
            filmLimit=int(0 if process["Film limit"]=='' else process["Film limit"])  
            runBiasedList=1 if process["Run Biased List"].lower()=='y' else 0
            runLoadingFilms=1 if process["Run Film List"].lower()=='y' else 0
            runWithImportedBiasedList=1 if process["Run with Imported Biased List"].lower()=='y' else 0
            sort_type="num_votes" if process["Biased List Sorting"]=="" else process["Biased List Sorting"]
            importBiasedListFile=process["Imported Biased List File"]
            w=float(0 if process["Biased List W"]=="" else process["Biased List W"].replace(",", "."))

            log.append(logger(process["Process ID"], "Params: FilmLimit: "+str(filmLimit)+" YearFrom: "+str(yearFrom)+" YearTo: "+str(yearTo)+" Country: "+str(country)+" Title_lang: "+str(title_lang)+""))
            
            if runWithImportedBiasedList==1:
                log.append(logger(process["Process ID"], "import "+importBiasedListFile))
                biasedCountByYear=importCSV_blob(process["Imported Biased List File"])

            if runBiasedList==1:
                log.append(logger(process["Process ID"], "Start of biasedCountByYear"))
                countFilmsList=list(dict())
                currYear=yearFrom
                while currYear<=yearTo:
                    countFilmsList.append({"year":currYear, "filmCount": getFeatureFilmCountByYear(currYear, country)})
                    currYear=currYear+1

                biasedCountByYear=filmLoaderCalc(w,filmLimit,countFilmsList)
                exportCSV_blob(process["Exported Biased List File"],biasedCountByYear)

            if runLoadingFilms==1:
                log.append(logger(process["Process ID"], "Start of getFeatureFilm_Title_ID_ByYear"))

                movieList_IMDBID_Title=list(dict())
                if process["Status"]!="P":                       
                    for countFilmYear in biasedCountByYear:
                        if yearFrom<=int(countFilmYear["year"]) and int(countFilmYear["year"])<=yearTo:
                            movieList_IMDBID_Title.extend(getFeatureFilm_Title_ID_ByYear(countFilmYear["year"],country, title_lang, int(countFilmYear["filmCountBiasedLimit"]), sort_type))
                    exportCSV_blob('output/filmlist_imdbid/filmlist_imdbid_'+process["Process ID"]+'',movieList_IMDBID_Title)                        
                
                if process["Status"]=="P" and existsCSV_blob('output/filmlist_imdbid/filmlist_imdbid_'+process["Process ID"]):
                    movieList_IMDBID_Title=importCSV_blob('output/filmlist_imdbid/filmlist_imdbid_'+process["Process ID"])

                log.append(logger(process["Process ID"], "Start of getFeatureFilm_keywords_origTitle_ById"))                

                if filmLimit>0 and len(movieList_IMDBID_Title)>filmLimit:                    
                    movieList_IMDBID_Title_over=movieList_IMDBID_Title[filmLimit::]
                    newID= process["Process ID"][0:-1]+str((int(process["Process ID"][-1::])+1)) 
                    exportCSV_blob('output/filmlist_imdbid/filmlist_imdbid_'+newID+'',movieList_IMDBID_Title_over)
                    movieList_IMDBID_Title=movieList_IMDBID_Title[0:filmLimit]
              
                if len(movieList_IMDBID_Title)>0:
                    rtn_movieList_KW=getFeatureFilm_keywords_origTitle_ById(movieList_IMDBID_Title)
                    movieList_KW=rtn_movieList_KW["rtn"]
                    err_flg="Y" if (country!="global" and rtn_movieList_KW["err_count"]>5) or (country=="global" and rtn_movieList_KW["err_count"]>2) else "N"

                    if err_flg!="Y":
                        log.append(logger(process["Process ID"], "Start of filmlist_omdb"))
                        filmlist_omdb = omdb(movieList_KW)
                        exportCSV_blob('output/filmlist_omdb/filmlist_omdb_'+process["Process ID"]+'_'+str(yearFrom)+'_'+str(yearTo)+'_'+country,filmlist_omdb)
                    else:
                        log.append(logger(process["Process ID"], "Empty keywords error"))
                else:
                    log.append(logger(process["Process ID"], "No films being processed.")) 

            process["Last Run"]=str((datetime.now()+ timedelta(hours=1)).strftime("%Y.%m.%d %H:%M:%S"))
            if err_flg=="Y":process["Status"]="E"
            else: process["Status"] = "D" if process["Status"] != "P" else process["Status"]
                

        new_processes.append(process)
                
    exportCSV_blob('MMP_processes',new_processes)

    log.append(logger("-", "Start of Loader refresh"))
    DB_Loader_lists()

    log.append(logger("-", "End of calculation"))
    exportCSV_blob('logs/log_'+str(((datetime.now())+ timedelta(hours=1)).strftime("%Y%m%d_%H%M%S")),log)
        


    if isAdhocRun==1:
        return func.HttpResponse("Adhoc calculation successfully processed.", status_code=200)
    else:
        return func.HttpResponse("Timer triggered calculation successfully processed.", status_code=200)

    #imported_CSV=importCSV_blob("MMP_processes")
    #exportCSV_blob(f"output/biased_list/blobtest/adhoc", imported_CSV)