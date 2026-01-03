from math import sqrt
import json
from operator import concat
import time
import requests
import csv
import datetime
from datetime import datetime, timedelta
import logging
import hashlib
import azure.functions as func
from azure.storage.blob import BlobServiceClient  

# --- CONFIG ---------------------------------------------------
blob_service = BlobServiceClient(account_url="https://mmstrgaccount.blob.core.windows.net/", credential="sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2027-01-04T00:05:31Z&st=2026-01-03T15:50:31Z&spr=https&sig=VZwfSg9vLomc%2BszCzCt3yHmNZv1PlyHvDFb3mwzhXHw%3D")
container_name = "mmdbloader"
delayHRS = 2
SLICE_SIZE = 250

# --- BLOB HELPERS --------------------------------------------
def exportCSV_blob(filename, listDict):
    if not listDict: 
        return
    delimiter=";"
    header = delimiter.join(list(listDict[0].keys()))
    writer = header
    for dict_item in listDict:
        writer = writer + '\n' + delimiter.join(list(str(x) for x in dict_item.values()))
    blob_client = blob_service.get_blob_client(container=container_name, blob=filename+".csv")
    blob_client.upload_blob(writer, blob_type="BlockBlob", overwrite=True)

def importCSV_blob(importCSV):
    newDict=list(dict())
    container_client = blob_service.get_container_client(container=container_name)
    try:
        readerOrig=container_client.download_blob(importCSV+".csv").readall().decode("utf-8").splitlines()
    except:
        return newDict
    reader = csv.reader(readerOrig, delimiter=';')
    headerCnt=1
    for param in reader:
        if headerCnt == 1:
            header=param
            colCnt=len(param)
        else:
            newDict.append({header[i]: param[i] for i in range(colCnt)})
        headerCnt += 1
    return newDict

def existsCSV_blob(existCSV):
    container_client = blob_service.get_container_client(container=container_name)
    folder='/'.join(existCSV.split('/')[:-1])
    fileList=list(str(x.name).split(folder+'/')[1] for x in container_client.list_blobs() if folder+'/' in str(x.name))
    file=existCSV.split('/')[-1]+".csv"
    return file in fileList

def logger(pr_id, msg):
    return {
        "process id": pr_id,
        "step message in detail": msg,
        "timestamp of the step": str((datetime.now()+ timedelta(hours=delayHRS)).strftime("%Y.%m.%d %H:%M:%S"))
    }

# --- SLICE STATE -------------------------------------------------
def load_slice_state(process_id):
    state_path = f'output/processing_state/state_{process_id}'
    if not existsCSV_blob(state_path):
        return {
            "next_index": 0,
            "slice_size": SLICE_SIZE
        }

    rows = importCSV_blob(state_path)
    if not rows:
        return {
            "next_index": 0,
            "slice_size": SLICE_SIZE
        }

    return {
        "next_index": int(rows[0].get("next_index", 0)),
        "slice_size": int(rows[0].get("slice_size", SLICE_SIZE))
    }


def save_slice_state(process_id, state):
    exportCSV_blob(
        f'output/processing_state/state_{process_id}',
        [{
            "next_index": state["next_index"],
            "slice_size": state["slice_size"]
        }]
    )

def clear_slice_state(process_id):
    try:
        blob_client = blob_service.get_blob_client(
            container=container_name,
            blob=f'output/processing_state/state_{process_id}.csv'
        )
        blob_client.delete_blob()
    except:
        pass

# --- FILM HASH ---------------------------------------------------
HASH_FIELDS = [
    "imdbId",
    "orig_title",
    "keywordsList",
    "omdb_ReleaseYear",
    "omdb_Runtime",
    "omdb_Genre",
    "omdb_Director",
    "omdb_Writer",
    "omdb_Actors",
    "omdb_Plot"
]

def calculate_film_hash(film):
    base = {k: film.get(k, "") for k in HASH_FIELDS}
    hash_input = json.dumps(base, sort_keys=True).encode("utf-8")
    return hashlib.sha1(hash_input).hexdigest()

def load_film_state(imdbId):
    if existsCSV_blob(f'output/film_state/{imdbId}'):
        data = importCSV_blob(f'output/film_state/{imdbId}')
        if data:
            return data[0].get('hash','')
    return ''

def save_film_state(imdbId, hash_value):
    exportCSV_blob(f'output/film_state/{imdbId}', [{"hash": hash_value}])

# --- IMDB / OMDB -------------------------------------------------
def cleanStr(str_input):
    return str_input.replace("&apos;", "'").replace("&amp;", "&").replace("\"", r"\"").replace("&quot;", r"\"")

def getFeatureFilmCountByYear(year, country):
    countryInURL="" if country=="global" else "&countries="+country
    time.sleep(0.05)
    req = requests.get(f"http://www.imdb.com/search/title/?title_type=feature&release_date={year}-01-01,{year}-12-31&count=50&view=simple&sort=num_votes,desc{countryInURL}")
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
    return int(result[::-1])

def getFeatureFilm_Title_ID_ByYear(year, country, title_lang, sort_type):
    countryInURL="" if country=="" or country=="global" else "&countries="+country
    title_lang=title_lang+"-"+title_lang
    movieList=list(dict())
    pageStart_counter=1
    film_counter=1
    while True:
        time.sleep(0.05)
        url=f"http://www.imdb.com/search/title/?title_type=feature&release_date={year}-01-01,{year}-12-31&count=250&sort={sort_type}&view=simple{countryInURL}&start={pageStart_counter}&ref_=adv_nxt"
        req = requests.get(url, headers={'Accept-Language': title_lang})
        html_bytes=req.content
        html = html_bytes.decode("utf-8")
        strToFind_1_1=r"""<span class="lister-item-header">"""
        countFilms=html.count(strToFind_1_1)
        if countFilms == 0:
            break
        page_film_counter=1
        while page_film_counter<=countFilms:
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
            film_counter +=1
            page_film_counter +=1
        pageStart_counter += 250
    return movieList

def getFeatureFilm_keywords_origTitle_ById(film):
    url = f"https://www.imdb.com/title/{film['imdbId']}/?ref_=fn_al_tt_0"
    print("KW:", url)

    req = requests.get(url, headers={
        'Accept-Language': 'HU-hu',
        'user-agent': 'Mozilla/5.0'
    })

    html = req.content.decode("utf-8")

    try:
        strToFind_1 = r"""{"@context":"https://schema.org"""
        p1 = html.find(strToFind_1) + len(strToFind_1)
        p2 = p1 + html[p1:].find(r"}</script>")
        slice_1 = html[p1:p2]

        # original title
        t1 = slice_1.find('name":') + 7
        t2 = t1 + slice_1[t1:].find('","')
        orig_title = slice_1[t1:t2]

        # keywords
        kw = ""
        k1 = slice_1.find('keywords":"')
        if k1 > -1:
            k1 += len('keywords":"')
            k2 = k1 + slice_1[k1:].find('","')
            kw = slice_1[k1:k2]

        return {
            "imdbId": film["imdbId"],
            "title": film["title"],
            "orig_title": cleanStr(orig_title),
            "keywordsList": cleanStr(kw)
        }

    except Exception:
        return None

def omdb(film):
    url = f"http://www.omdbapi.com/?i={film['imdbId']}&apikey=63b34753"
    req = requests.get(url)
    data = json.loads(req.content.decode("utf-8"))

    if data.get("Response") != "True":
        return None

    return {
        "imdbId": film["imdbId"],
        "title": film["title"],
        "orig_title": film["orig_title"],
        "keywordsList": film["keywordsList"],
        "omdb_ReleaseYear": data.get("Year",""),
        "omdb_Runtime": data.get("Runtime",""),
        "omdb_Genre": data.get("Genre",""),
        "omdb_Director": cleanStr(data.get("Director","")),
        "omdb_Writer": cleanStr(data.get("Writer","")),
        "omdb_Actors": cleanStr(data.get("Actors","")),
        "omdb_Plot": cleanStr(data.get("Plot","")).replace(";", ","),
        "omdb_Language": data.get("Language",""),
        "omdb_Country": data.get("Country",""),
        "omdb_ReleasedDate": data.get("Released",""),
        "omdb_Awards": data.get("Awards",""),
        "omdb_Poster": data.get("Poster",""),
        "omdb_Rating_IMDB": data.get("imdbRating","0"),
        "omdb_Rating_TOMATOES":
            str(data["Ratings"][1]["Value"]).replace("%","")
            if len(data.get("Ratings",[])) > 1 else "0",
        "omdb_Rating_METASCORE": data.get("Metascore","0").replace("N/A","0"),
        "omdb_imdbVotes": data.get("imdbVotes","0").replace(",", "")
    }


# --- MASTER LIST BUILDER ----------------------------------------
def build_master_imdb_list(process):
    yearFrom=int(process["Year From"])
    yearTo=int(process["Year To"])
    country="global" if process["Country"]=="" else process["Country"]
    title_lang=process["Title Language"]
    sort_type="num_votes,desc" if process["Biased List Sorting"]=="" else process["Biased List Sorting"]

    full_list=[]
    for year in range(yearFrom, yearTo+1):
        year_list=getFeatureFilm_Title_ID_ByYear(year, country, title_lang, sort_type)
        full_list.extend(year_list)
    return full_list

# --- NEXT SLICE -----------------------------------------------
def get_next_slice(process_id, master_list):
    state=load_slice_state(process_id)
    start=state["next_index"]
    end=min(start+state["slice_size"], len(master_list))
    slice_part=master_list[start:end]
    state["next_index"]=end
    save_slice_state(process_id, state)
    return slice_part, end>=len(master_list)

# --- PROCESS SLICE ---------------------------------------------
def process_slice(slice_list):
    delta_films = []
    for film in slice_list:
        imdb_data = getFeatureFilm_keywords_origTitle_ById(film)
        if not imdb_data:
            continue

        omdb_data = omdb(imdb_data)
        if not omdb_data:
            continue

        new_hash = calculate_film_hash(omdb_data)
        old_hash = load_film_state(film["imdbId"])

        if new_hash != old_hash:
            delta_films.append(omdb_data)
            save_film_state(film["imdbId"], new_hash)

    return delta_films

# --- MAIN -------------------------------------------------------
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    isAdhocRun = 1 if req.params.get('adhoc')=='1' else 0
    log=list(dict())
    new_processes=list(dict())
    processes=importCSV_blob('MMP_processes')
    err_flg="N"
    processesToRun=list()
    
    # --- SCHEDULED RUN ---
    if isAdhocRun==0:
        dayMap={"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4,"Sat":5,"Sun":6}
        allScheduledProcesses=importCSV_blob("load_to_DB/scheduler")
        processesToRun=[x["Process ID"] for x in allScheduledProcesses
                        if datetime.strptime(x["Time"], "%H:%M").time()<=(datetime.now()+ timedelta(hours=delayHRS)).time()
                        and (datetime.now()+ timedelta(hours=delayHRS)).time()<=(datetime.strptime(x["Time"], "%H:%M")+ timedelta(minutes=5)).time()
                        and (datetime.now()+ timedelta(hours=delayHRS)).weekday()==dayMap[x["Day"]]]

    if not processesToRun:
        processesToRun=["none"]

    exportCSV_blob('load_to_DB/processes_curr_ong',[{"process":p} for p in processesToRun])

    for process in processes:
        if (isAdhocRun==1 and process["Adhoc"]=='Y') or (process["Process ID"] in processesToRun):
            master_list=build_master_imdb_list(process)
            finished=False
            while not finished:
                slice_part, finished=get_next_slice(process["Process ID"], master_list)
                delta_films=process_slice(slice_part)
                if delta_films:
                    exportCSV_blob(f'load_to_DB/load_filmlist_delta_to_db_{process["Process ID"]}', delta_films)
            clear_slice_state(process["Process ID"])
            # --- update status ---
            if int(process["Year To"])==datetime.now().year:
                process["Status"]="P"
            else:
                process["Status"]="D"
            process["Last Run"]=str((datetime.now()+ timedelta(hours=delayHRS)).strftime("%Y.%m.%d %H:%M:%S"))
        new_processes.append(process)
    
    exportCSV_blob('MMP_processes', new_processes)
    log.append(logger("-", "Processing finished"))
    exportCSV_blob('logs/log_'+str(((datetime.now())+ timedelta(hours=delayHRS)).strftime("%Y%m%d_%H%M%S")),log)
    
    return func.HttpResponse("Processing successfully completed.", status_code=200)
