from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from datetime import datetime as dt
from joblib import Parallel, delayed
from re import sub, split, UNICODE
import unidecode

from langid import classify
import pycld2 as cld2
from langdetect import DetectorFactory, PROFILES_DIRECTORY
from fastspell import FastSpell
from lingua import LanguageDetectorBuilder
import iso639

fast_spell = FastSpell("en", mode="cons")


def lang_poll(text, verbose=0):
    text = text.lower()
    text = text.replace("\n", "")
    lang_list = []

    lang_list.append(classify(text)[0].lower())

    detected_language = None
    try:
        _, _, _, detected_language = cld2.detect(text, returnVectors=True)
    except Exception as e:
        if verbose > 4:
            print("Language detection error using cld2, trying without ascii")
            print(e)
        try:
            text = str(unidecode.unidecode(text).encode("ascii", "ignore"))
            _, _, _, detected_language = cld2.detect(text, returnVectors=True)
        except Exception as e:
            if verbose > 4:
                print("Language detection error using cld2")
                print(e)

    if detected_language:
        lang_list.append(detected_language[0][-1].lower())

    try:
        _factory = DetectorFactory()
        _factory.load_profile(PROFILES_DIRECTORY)
        detector = _factory.create()
        detector.append(text)
        lang_list.append(detector.detect().lower())
    except Exception as e:
        if verbose > 4:
            print("Language detection error using langdetect")
            print(e)

    try:
        result = fast_spell.getlang(text)  # low_memory breaks the function
        lang_list.append(result.lower())
    except Exception as e:
        if verbose > 4:
            print("Language detection error using fastSpell")
            print(e)

    detector = LanguageDetectorBuilder.from_all_languages().build()
    res = detector.detect_language_of(text)
    if res:
        if res.name.capitalize() == "Malay":
            la = "ms"
        elif res.name.capitalize() == "Sotho":
            la = "st"
        elif res.name.capitalize() == "Bokmal":
            la = "no"
        elif res.name.capitalize() == "Swahili":
            la = "sw"
        elif res.name.capitalize() == "Nynorsk":
            la = "is"
        elif res.name.capitalize() == "Slovene":
            la = "sl"
        else:
            la = iso639.find(
                res.name.capitalize())["iso639_1"].lower()
        lang_list.append(la)

    lang = None
    for prospect in set(lang_list):
        votes = lang_list.count(prospect)
        if votes > len(lang_list) / 2:
            lang = prospect
            break
    return lang


def split_names(s, exceptions=['GIL', 'LEW', 'LIZ', 'PAZ', 'REY', 'RIO', 'ROA', 'RUA', 'SUS', 'ZEA']):
    """
    Extract the parts of the full name `s` in the format ([] → optional):

    [SMALL_CONECTORS] FIRST_LAST_NAME [SMALL_CONECTORS] [SECOND_LAST_NAME] NAMES

    * If len(s) == 2 → Foreign name assumed with single last name on it
    * If len(s) == 3 → Colombian name assumed two last mames and one first name

    Add short last names to `exceptions` list if necessary

    Works with:
    ----
        s='LA ROTTA FORERO DANIEL ANDRES'
        s='MONTES RAMIREZ MARIA DEL CONSUELO'
        s='CALLEJAS POSADA RICARDO DE LA MERCED'
        s='DE LA CUESTA BENJUMEA MARIA DEL CARMEN'
        s='JARAMILLO OCAMPO NICOLAS CARLOS MARTI'
        s='RESTREPO QUINTERO DIEGO ALEJANDRO'
        s='RESTREPO ZEA JAIRO HUMBERTO'
        s='JIMENEZ DEL RIO MARLEN'
        s='RESTREPO FERNÁNDEZ SARA' # Colombian: two LAST_NAMES NAME
        s='NARDI ENRICO' # Foreing
    Fails:
    ----
        s='RANGEL MARTINEZ VILLAL ANDRES MAURICIO' # more than 2 last names
        s='ROMANO ANTONIO ENEA' # Foreing → LAST_NAME NAMES
    """
    s = s.title()
    exceptions = [e.title() for e in exceptions]
    sl = sub('(\s\w{1,3})\s', r'\1-', s, UNICODE)  # noqa: W605
    sl = sub('(\s\w{1,3}\-\w{1,3})\s', r'\1-', sl, UNICODE)  # noqa: W605
    sl = sub('^(\w{1,3})\s', r'\1-', sl, UNICODE)  # noqa: W605
    # Clean exceptions
    # Extract short names list
    lst = [s for s in split(
        '(\w{1,3})\-', sl) if len(s) >= 1 and len(s) <= 3]  # noqa: W605
    # intersection with exceptions list
    exc = [value for value in exceptions if value in lst]
    if exc:
        for e in exc:
            sl = sl.replace('{}-'.format(e), '{} '.format(e))

    # if sl.find('-')>-1:
    # print(sl)
    sll = [s.replace('-', ' ') for s in sl.split()]
    if len(s.split()) == 2:
        sll = [s.split()[0]] + [''] + [s.split()[1]]
    #
    d = {'NOMBRE COMPLETO': ' '.join(sll[2:] + sll[:2]),
         'PRIMER APELLIDO': sll[0],
         'SEGUNDO APELLIDO': sll[1],
         'NOMBRES': ' '.join(sll[2:]),
         'INICIALES': ' '.join([i[0] + '.' for i in ' '.join(sll[2:]).split()])
         }
    return d


def parse_openalex(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "openalex", "time": int(time())}]
    if reg["title"]:
        if "http" in reg["title"]:
            reg["title"] = reg["title"].split("//")[-1]
        lang = lang_poll(reg["title"], verbose=verbose)
        entry["titles"].append(
            {"title": reg["title"], "lang": lang, "source": "openalex"})
    for source, idx in reg["ids"].items():
        if "doi" in source:
            idx = idx.replace("https://doi.org/", "").lower()
        entry["external_ids"].append({"source": source, "id": idx})
    entry["year_published"] = reg["publication_year"]
    entry["date_published"] = int(dt.strptime(
        reg["publication_date"], "%Y-%m-%d").timestamp())
    entry["types"].append({"source": "openalex", "type": reg["type"]})
    entry["citations_by_year"] = reg["counts_by_year"]

    entry["source"] = {
        "name": reg["host_venue"]["display_name"],
        "external_ids": [{"source": "openalex", "id": reg["host_venue"]["id"]}]
    }

    if "issn_l" in reg["host_venue"].keys():
        if reg["host_venue"]["issn_l"]:
            entry["source"]["external_ids"].append(
                {"source": "issn_l", "id": reg["host_venue"]["issn_l"]})
    if "issn" in reg["host_venue"].keys():
        if reg["host_venue"]["issn"]:
            entry["source"]["external_ids"].append(
                {"source": "issn", "id": reg["host_venue"]["issn"][0]})

    entry["citations_count"].append(
        {"source": "openalex", "count": reg["cited_by_count"]})

    if "volume" in reg["biblio"]:
        if reg["biblio"]["volume"]:
            entry["bibliographic_info"]["volume"] = reg["biblio"]["volume"]
    if "issue" in reg["biblio"]:
        if reg["biblio"]["issue"]:
            entry["bibliographic_info"]["issue"] = reg["biblio"]["issue"]
    if "first_page" in reg["biblio"]:
        if reg["biblio"]["first_page"]:
            entry["bibliographic_info"]["start_page"] = reg["biblio"]["first_page"]
    if "last_page" in reg["biblio"]:
        if reg["biblio"]["last_page"]:
            entry["bibliographic_info"]["end_page"] = reg["biblio"]["last_page"]
    if "open_access" in reg.keys():
        if "is_oa" in reg["open_access"].keys():
            entry["bibliographic_info"]["is_open_access"] = reg["open_access"]["is_oa"]
        if "oa_status" in reg["open_access"].keys():
            entry["bibliographic_info"]["open_access_status"] = reg["open_access"]["oa_status"]
        if "oa_url" in reg["open_access"].keys():
            if reg["open_access"]["oa_url"]:
                entry["external_urls"].append(
                    {"source": "oa", "url": reg["open_access"]["oa_url"]})

    # authors section
    for author in reg["authorships"]:
        if not author["author"]:
            continue
        affs = []
        for inst in author["institutions"]:
            if inst:
                aff_entry = {
                    "external_ids": [{"source": "openalex", "id": inst["id"]}],
                    "name": inst["display_name"]
                }
                if "ror" in inst.keys():
                    aff_entry["external_ids"].append(
                        {"source": "ror", "id": inst["ror"]})
                affs.append(aff_entry)
        author = author["author"]
        author_entry = {
            "external_ids": [{"source": "openalex", "id": author["id"]}],
            "full_name": author["display_name"],
            "types": [],
            "affiliations": affs
        }
        if author["orcid"]:
            author_entry["external_ids"].append(
                {"source": "orcid", "id": author["orcid"].replace("https://orcid.org/", "")})
        entry["authors"].append(author_entry)
    # concepts section
    subjects = []
    for concept in reg["concepts"]:
        sub_entry = {
            "external_ids": [{"source": "openalex", "id": concept["id"]}],
            "name": concept["display_name"],
            "level": concept["level"]
        }
        subjects.append(sub_entry)
    entry["subjects"].append({"source": "openalex", "subjects": subjects})

    return entry


def process_one(oa_reg, url, db_name, empty_work, verbose=0):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["works"]
    doi = None
    # register has doi
    if "doi" in oa_reg.keys():
        if oa_reg["doi"]:
            doi = oa_reg["doi"].split(".org/")[-1].lower()
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "openalex":
                    client.close()
                    return None  # Register already on db
                    # Could be updated with new information when openalex database changes
            entry = parse_openalex(oa_reg, empty_work.copy(), verbose=verbose)
            colav_reg["updated"].append(
                {"source": "openalex", "time": int(time())})
            # titles
            colav_reg["titles"].extend(entry["titles"])
            # external_ids
            ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
            for ext in entry["external_ids"]:
                if ext["id"] not in ext_ids:
                    colav_reg["external_ids"].append(ext)
                    ext_ids.append(ext["id"])
            # types
            colav_reg["types"].extend(entry["types"])
            # open access
            if "is_open_acess" not in colav_reg["bibliographic_info"].keys():
                if "is_open_access" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["is_open_acess"] = entry["bibliographic_info"]["is_open_access"]
            if "open_access_status" not in colav_reg["bibliographic_info"].keys():
                if "open_access_status" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["open_access_status"] = entry["bibliographic_info"]["open_access_status"]
            # external urls
            urls_sources = [url["source"]
                            for url in colav_reg["external_urls"]]
            if "oa" not in urls_sources:
                oa_url = None
                for ext in entry["external_urls"]:
                    if ext["source"] == "oa":
                        oa_url = ext["url"]
                        break
                if oa_url:
                    colav_reg["external_urls"].append(
                        {"source": "oa", "url": entry["external_urls"][0]["url"]})
            # citations by year
            colav_reg["citations_by_year"] = entry["counts_by_year"]
            # citations count
            if entry["citations_count"]:
                colav_reg["citations_count"].extend(entry["citations_count"])
            # subjects
            subject_list = []
            for subjects in entry["subjects"]:
                for i, subj in enumerate(subjects["subjects"]):
                    for ext in subj["external_ids"]:
                        sub_db = db["subjects"].find_one(
                            {"external_ids.id": ext["id"]})
                        if sub_db:
                            name = sub_db["names"][0]["name"]
                            for n in sub_db["names"]:
                                if n["lang"] == "en":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "es":
                                    name = n["name"]
                            subject_list.append({
                                "id": sub_db["_id"],
                                "name": name,
                                "level": sub_db["level"]
                            })
                            break
            colav_reg["subjects"].append(
                {"source": "openalex", "subjects": subject_list})

            collection.update_one(
                {"_id": colav_reg["_id"]},
                {"$set": {
                    "updated": colav_reg["updated"],
                    "titles": colav_reg["titles"],
                    "external_ids": colav_reg["external_ids"],
                    "types": colav_reg["types"],
                    "bibliographic_info": colav_reg["bibliographic_info"],
                    "external_urls": colav_reg["external_urls"],
                    "subjects": colav_reg["subjects"],
                    "citations_count": colav_reg["citations_count"],
                    "citations_by_year": colav_reg["citations_by_year"]
                }}
            )
        else:  # insert a new register
            # parse
            entry = parse_openalex(oa_reg, empty_work.copy(), verbose=verbose)
            # link
            source_db = None
            if "external_ids" in entry["source"].keys():
                for ext in entry["source"]["external_ids"]:
                    source_db = db["sources"].find_one(
                        {"external_ids.id": ext["id"]})
                    if source_db:
                        break
            if source_db:
                name = source_db["names"][0]["name"]
                for n in source_db["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    if n["lang"] == "en":
                        name = n["name"]
                entry["source"] = {
                    "id": source_db["_id"],
                    "name": name
                }
            else:
                if len(entry["source"]["external_ids"]) == 0:
                    print(
                        f'Register with doi: {oa_reg["doi"]} does not provide a source')
                else:
                    print("No source found for\n\t",
                          entry["source"]["external_ids"])
                entry["source"] = {
                    "id": "",
                    "name": entry["source"]["name"]
                }
            for subjects in entry["subjects"]:
                for i, subj in enumerate(subjects["subjects"]):
                    for ext in subj["external_ids"]:
                        sub_db = db["subjects"].find_one(
                            {"external_ids.id": ext["id"]})
                        if sub_db:
                            name = sub_db["names"][0]["name"]
                            for n in sub_db["names"]:
                                if n["lang"] == "en":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "es":
                                    name = n["name"]
                            entry["subjects"][0]["subjects"][i] = {
                                "id": sub_db["_id"],
                                "name": name,
                                "level": sub_db["level"]
                            }
                            break
            # search authors and affiliations in db
            for i, author in enumerate(entry["authors"]):
                author_db = None
                for ext in author["external_ids"]:
                    author_db = db["person"].find_one(
                        {"external_ids.id": ext["id"]})
                    if author_db:
                        break
                if author_db:
                    sources = [ext["source"]
                               for ext in author_db["external_ids"]]
                    ids = [ext["id"] for ext in author_db["external_ids"]]
                    for ext in author["external_ids"]:
                        if ext["id"] not in ids:
                            author_db["external_ids"].append(ext)
                            sources.append(ext["source"])
                            ids.append(ext["id"])
                    entry["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    if "external_ids" in author.keys():
                        del (author["external_ids"])
                else:
                    author_db = db["person"].find_one(
                        {"full_name": author["full_name"]})
                    if author_db:
                        sources = [ext["source"]
                                   for ext in author_db["external_ids"]]
                        ids = [ext["id"] for ext in author_db["external_ids"]]
                        for ext in author["external_ids"]:
                            if ext["id"] not in ids:
                                author_db["external_ids"].append(ext)
                                sources.append(ext["source"])
                                ids.append(ext["id"])
                        entry["authors"][i] = {
                            "id": author_db["_id"],
                            "full_name": author_db["full_name"],
                            "affiliations": author["affiliations"]
                        }
                    else:
                        entry["authors"][i] = {
                            "id": "",
                            "full_name": author["full_name"],
                            "affiliations": author["affiliations"]
                        }
                for j, aff in enumerate(author["affiliations"]):
                    aff_db = None
                    if "external_ids" in aff.keys():
                        for ext in aff["external_ids"]:
                            aff_db = db["affiliations"].find_one(
                                {"external_ids.id": ext["id"]})
                            if aff_db:
                                break
                    if aff_db:
                        name = aff_db["names"][0]["name"]
                        for n in aff_db["names"]:
                            if n["source"] == "ror":
                                name = n["name"]
                                break
                            if n["lang"] == "en":
                                name = n["name"]
                            if n["lang"] == "es":
                                name = n["name"]
                        entry["authors"][i]["affiliations"][j] = {
                            "id": aff_db["_id"],
                            "name": name,
                            "types": aff_db["types"]
                        }
                    else:
                        aff_db = db["affiliations"].find_one(
                            {"names.name": aff["name"]})
                        if aff_db:
                            name = aff_db["names"][0]["name"]
                            for n in aff_db["names"]:
                                if n["source"] == "ror":
                                    name = n["name"]
                                    break
                                if n["lang"] == "en":
                                    name = n["name"]
                                if n["lang"] == "es":
                                    name = n["name"]
                            entry["authors"][i]["affiliations"][j] = {
                                "id": aff_db["_id"],
                                "name": name,
                                "types": aff_db["types"]
                            }
                        else:
                            entry["authors"][i]["affiliations"][j] = {
                                "id": "",
                                "name": aff["name"],
                                "types": []
                            }

            entry["author_count"] = len(entry["authors"])
            # insert in mongo
            try:
                collection.insert_one(entry)
            except Exception as e:
                client.close()
                print(entry)
                print(e)
                print(doi)
                print(entry["autrhors_count"])
                raise

            # insert in elasticsearch
    else:  # does not have a doi identifier
        # elasticsearch section
        pass
    client.close()


class Kahi_openalex_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])

        self.openalex_client = MongoClient(
            config["openalex_works"]["database_url"])
        self.openalex_db = self.openalex_client[config["openalex_works"]
                                                ["database_name"]]
        self.openalex_collection = self.openalex_db[config["openalex_works"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_works"]["num_jobs"] if "num_jobs" in config["openalex_works"].keys(
        ) else 1
        self.verbose = config["openalex_works"]["verbose"] if "verbose" in config["openalex_works"].keys(
        ) else 0

    def process_openalex(self):
        paper_list = list(self.openalex_collection.find())
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                paper,
                self.mongodb_url,
                self.config["database_name"],
                self.empty_work(),
                verbose=self.verbose
            ) for paper in paper_list
        )

    def run(self):
        self.process_openalex()
        return 0
