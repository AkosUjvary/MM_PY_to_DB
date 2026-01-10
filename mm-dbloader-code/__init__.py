from math import sqrt
import json
import time
import requests
import csv
from datetime import datetime, timedelta
import hashlib
import azure.functions as func
from azure.storage.blob import BlobServiceClient  
import os

# ==============================================================
# CONFIG
# ==============================================================
conn_str = os.environ["MM_STORAGE_CONNECTION_STRING"]
blob_service = BlobServiceClient.from_connection_string(conn_str)
container_name="mmdbloader"

delayHRS = 2
SLICE_SIZE = 250

HASH_FIELDS = [
    "imdbId","orig_title","keywordsList",
    "omdb_ReleaseYear","omdb_Runtime","omdb_Genre",
    "omdb_Director","omdb_Writer","omdb_Actors","omdb_Plot"
]

# ==============================================================
# CSV HELPERS
# ==============================================================
def exportCSV_blob(filename, rows):
    if not rows:
        return
    delimiter=";"
    header = delimiter.join(rows[0].keys())
    body = [delimiter.join(str(r.get(k,"")) for k in rows[0].keys()) for r in rows]
    blob = blob_service.get_blob_client(container=container_name, blob=f"{filename}.csv")
    blob.upload_blob(header + "\n" + "\n".join(body), overwrite=True)

def importCSV_blob(path):
    try:
        blob = blob_service.get_blob_client(container=container_name, blob=f"{path}.csv")
        lines = blob.download_blob().readall().decode("utf-8").splitlines()
    except:
        return []
    reader = csv.reader(lines, delimiter=";")
    header = next(reader)
    return [{header[i]: row[i] for i in range(len(header))} for row in reader]

def existsCSV_blob(path):
    try:
        blob_service.get_blob_client(container=container_name, blob=f"{path}.csv").get_blob_properties()
        return True
    except:
        return False

# ==============================================================
# LOGGER
# ==============================================================
def logger(pid, msg):
    return {
        "process id": pid,
        "step message in detail": msg,
        "timestamp": (datetime.now()+timedelta(hours=delayHRS)).strftime("%Y.%m.%d %H:%M:%S")
    }

# ==============================================================
# IMDB HELPERS
# ==============================================================
def cleanStr(s):
    if not s:
        return ""
    return (
        s.replace("&apos;","'")
         .replace("&amp;","&")
         .replace("&quot;",'"')
    )

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": "\"Brave\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "sec-gpc": "1",
    "upgrade-insecure-requests": "1",
	"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}

def getFeatureFilmCountByYear(year, country):
    country_q = "" if country=="global" else f"&countries={country}"
    time.sleep(0.05)
    html = requests.get(
        f"https://www.imdb.com/search/title/?title_type=feature&release_date={year}-01-01,{year}-12-31{country_q}", headers=headers
    ).text
    p = html.find('<div class="desc">') + 17
    t = html[p:p+100]
    i=-2; r=""

    while t[i].isdigit() or t[i]==",":
        r += t[i].replace(",","")
        i-=1
    return int(r[::-1])

def getFeatureFilm_Title_ID_ByYear(year, country, title_lang, limit, sort_type):
    out=[]
    start=1
    headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": ""+title_lang+",en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": "\"Brave\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "sec-gpc": "1",
    "upgrade-insecure-requests": "1",
	"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}
    while len(out)<limit:
        url = (
            f"https://www.imdb.com/search/title/?title_type=feature"
            f"&release_date={year}-01-01,{year}-12-31"
            f"&sort={sort_type}&count=250&start={start}"
        )
        html = requests.get(url, headers=headers).text
        if html.count("lister-item-header")==0:
            break
        while "lister-item-header" in html and len(out)<limit:
            p = html.find("title/")+6
            imdbId = html[p:p+9]
            t1 = html.find('">',p)+2
            t2 = html.find("</a>",t1)
            title = cleanStr(html[t1:t2])
            out.append({"imdbId":imdbId,"title":title})
            html = html[t2:]
        start+=250
    return out

# ==============================================================
# KEYWORDS + OMDB
# ==============================================================
def getFeatureFilm_keywords_origTitle_ById(film):
    html = requests.get(
        f"https://www.imdb.com/title/{film['imdbId']}/",
        headers={"Accept-Language":"HU-hu","User-Agent":"Mozilla/5.0"}
    ).text
    try:
        p = html.find('"@context":"https://schema.org"')
        s = html[p:html.find("</script>",p)]
        ot = s[s.find('"name":"')+8:s.find('","',s.find('"name":"')+8)]
        kw = ""
        if '"keywords":"' in s:
            k1 = s.find('"keywords":"')+12
            kw = s[k1:s.find('","',k1)]
        return {
            "imdbId":film["imdbId"],
            "title":film["title"],
            "orig_title":cleanStr(ot),
            "keywordsList":cleanStr(kw)
        }
    except:
        return None

def omdb(film):
    data = requests.get(
        f"http://www.omdbapi.com/?i={film['imdbId']}&apikey=63b34753"
    ).json()
    if data.get("Response")!="True":
        return None
    return {
        **film,
        "omdb_ReleaseYear":data.get("Year",""),
        "omdb_Runtime":data.get("Runtime",""),
        "omdb_Genre":data.get("Genre",""),
        "omdb_Director":cleanStr(data.get("Director","")),
        "omdb_Writer":cleanStr(data.get("Writer","")),
        "omdb_Actors":cleanStr(data.get("Actors","")),
        "omdb_Plot":cleanStr(data.get("Plot","")),
        "omdb_Language":data.get("Language",""),
        "omdb_Country":data.get("Country",""),
        "omdb_Poster":data.get("Poster",""),
        "omdb_Rating_IMDB":data.get("imdbRating",""),
        "omdb_imdbVotes":str(data.get("imdbVotes","")).replace(",","")
    }

# ==============================================================
# HASH
# ==============================================================
def film_hash(f):
    payload = {k:f.get(k,"") for k in HASH_FIELDS}
    return hashlib.sha1(json.dumps(payload,sort_keys=True).encode()).hexdigest()

# ==============================================================
# BIASED LIST
# ==============================================================
def filmLoaderCalc(w,limit,filmCounts):
    maxv = max(x["filmCount"] for x in filmCounts)
    out=[]
    total=0
    for f in filmCounts:
        b = sqrt(f["filmCount"]) * ((f["filmCount"]+(w*(maxv-f["filmCount"])))/maxv)
        total+=b
        out.append({**f,"filmCountBiased":b})
    final=[]
    now=datetime.now()
    for f in out:
        if f["year"]==now.year:
            prev=[x for x in out if x["year"]==now.year-1][0]["filmCountBiased"]
            weeks=now.isocalendar().week
            lim = round((prev/52)*weeks)
        else:
            lim = round(f["filmCountBiased"]*(limit/total))
        final.append({**f,"filmCountBiasedLimit":lim})
    return final

# ==============================================================
# SCRAPE STATE
# ==============================================================
def load_scrape_state(pid):
    path=f"output/process_state/scrape_{pid}"
    if not existsCSV_blob(path):
        return {"current_index":"0","done":"N"}
    return importCSV_blob(path)[0]

def save_scrape_state(pid, idx, done):
    exportCSV_blob(
        f"output/process_state/scrape_{pid}",
        [{"current_index":idx,"done":done}]
    )

# ==============================================================
# MAIN
# ==============================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    processes = importCSV_blob("MMP_processes")
    logs=[]

    for p in processes:

        # ---------------- BIASED LIST ----------------
        if p["Run Biased List"].lower()=="y":
            counts=[]
            for y in range(int(p["Year From"]),int(p["Year To"])+1):
                counts.append({
                    "year":y,
                    "filmCount":getFeatureFilmCountByYear(y,p["Country"] or "global")
                })
            biased=filmLoaderCalc(
                float(p["Biased List W"]),
                int(p["Film limit"]),
                counts
            )
            exportCSV_blob(p["Exported Biased List File"],biased)

        # ---------------- FILM LIST ----------------
        if p["Run Film List"].lower()=="y":

            biased = importCSV_blob(p["Imported Biased List File"])
            imdb_list=[]

            for y in biased:
                imdb_list.extend(
                    getFeatureFilm_Title_ID_ByYear(
                        int(y["year"]),
                        p["Country"] or "global",
                        p["Title Language"],
                        int(float(y["filmCountBiasedLimit"])),
                        p["Biased List Sorting"]
                    )
                )

            state = load_scrape_state(p["Process ID"])
            start = int(state["current_index"])
            end = min(start+SLICE_SIZE, len(imdb_list))

            scraped=[]
            for f in imdb_list[start:end]:
                kw = getFeatureFilm_keywords_origTitle_ById(f)
                if not kw: continue
                om = omdb(kw)
                if not om: continue
                scraped.append(om)

            if scraped:
                existing = importCSV_blob(f"output/tmp_scrape/process_{p['Process ID']}")
                exportCSV_blob(
                    f"output/tmp_scrape/process_{p['Process ID']}",
                    existing + scraped
                )

            done = "Y" if end>=len(imdb_list) else "N"
            save_scrape_state(p["Process ID"], end, done)

            # -------- FINALIZE PROCESS --------
            if done=="Y":
                all_films = importCSV_blob(f"output/tmp_scrape/process_{p['Process ID']}")
                full=[]
                delta=[]
                for f in all_films:
                    f['load_type']="STNDRD"
                    h=film_hash(f)
                    old=importCSV_blob(f"output/film_state/{f['imdbId']}")
                    if not old or old[0]["hash"]!=h:
                        delta.append(f)
                        exportCSV_blob(
                            f"output/film_state/{f['imdbId']}",
                            [{"hash":h}]
                        )
                    full.append(f)

                filmListCorr = importCSV_blob("load_to_DB/load_filmlist_to_db_corr")
                for filmCorr in filmListCorr:
                    filmCorr['load_type']="CORR"
                    full.append(filmCorr)
                    delta.append(filmCorr)

                exportCSV_blob("load_to_DB/load_filmlist_to_db_corr", list(dict()))

                if delta:
                    exportCSV_blob("load_to_DB/load_filmlist_delta_to_db",delta)

                exportCSV_blob("load_to_DB/load_filmlist_to_db",full)
                exportCSV_blob(
                    f"output/filmlist_omdb/filmlist_omdb_{p['Process ID']}",
                    full
                )

        p["Last Run"]=(datetime.now()+timedelta(hours=delayHRS)).strftime("%Y.%m.%d %H:%M:%S")
        p["Status"]="P" if int(p["Year To"])==datetime.now().year else "D"
        logs.append(logger(p["Process ID"],"Finished"))

    exportCSV_blob("MMP_processes",processes)
    exportCSV_blob(f"logs/log_{datetime.now().strftime('%Y%m%d_%H%M%S')}",logs)
    return func.HttpResponse("OK",status_code=200)
