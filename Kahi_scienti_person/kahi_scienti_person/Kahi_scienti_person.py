from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from datetime import datetime as dt
from time import time


class Kahi_scienti_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.verbose = config["scienti_person"]["verbose"] if "verbose" in config["scienti_person"].keys(
        ) else 0

        name_index = False
        for key, val in self.collection.index_information().items():
            if key == "names.name_text":
                name_index = True
                break
        if not name_index:
            self.collection.create_index([("names.name", TEXT)])
            print("Text index created on names.name field")

    def update_inserted(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for person in self.collection.find():
            idx = None
            for ext in person["external_ids"]:
                if ext["source"] == "Cédula de Ciudadanía":
                    idx = ext["id"]
                elif ext["source"] == "Cédula de Extranjería":
                    idx = ext["id"]
            scienti_reg = scienti.find_one({"author.NRO_DOCUMENTO_IDENT": idx})
            if scienti_reg:
                author = scienti_reg["author"][0]
                person["external_ids"].append(
                    {"source": "scienti", "id": author["COD_RH"]})
                if "COD_ORCID" in author.keys():
                    if author["COD_ORCID"]:
                        person["external_ids"].append(
                            {"source": "orcid", "id": author["COD_ORCID"]})
                person["first_names"] = author["TXT_NAMES_RH"].strip().split()
                person["last_names"] = []
                if "TXT_PRIM_APELL" in author.keys():
                    person["last_names"].append(author["TXT_PRIM_APELL"])
                if "TXT_SEG_APELL" in author.keys():
                    person["last_names"].append(author["TXT_SEG_APELL"])
                initials = "".join([p[0].upper()
                                   for p in person["first_names"]])

                marital = None
                if "TPO_ESTADO_CIVIL" in author.keys():
                    if author["TPO_ESTADO_CIVIL"] == "C":
                        marital = "Married"
                    elif author["TPO_ESTADO_CIVIL"] == "S":
                        marital = "Single"
                    elif author["TPO_ESTADO_CIVIL"] == "U":
                        marital = "Domestic Partnership"
                    elif author["TPO_ESTADO_CIVIL"] == "D":
                        marital = "Divorced"
                    elif author["TPO_ESTADO_CIVIL"] == "V":
                        marital = "Widowed"

                if "city" in author.keys():
                    city = author["city"][0]
                    birthplace = {
                        "city": city["TXT_NME_MUNICIPIO"].capitalize(),
                        "state": city["department"][0]["TXT_NME_DEPARTAMENTO"].capitalize(),
                        "country": city["department"][0]["country"][0]["TXT_NME_PAIS"].capitalize()
                    }

                person["updated"].append(
                    {"time": int(time()), "source": "scienti"})

                rank = person["ranking"]
                ranks = []
                for prod in scienti.find({"author.NRO_DOCUMENTO_IDENT": idx}):
                    group_entry = {}
                    group_db = db["affiliations"].find_one(
                        {"external_ids.id": prod["group"][0]["COD_ID_GRUPO"]})
                    if group_db:
                        name = group_db["names"][0]["name"]
                        for n in group_db["names"]:
                            if n["lang"] == "es":
                                name = n["name"]
                                break
                            elif n["lang"] == "en":
                                name = n["name"]
                        aff_found = False
                        for aff in person["affiliations"]:
                            if group_db["_id"] == aff["id"]:
                                aff_found = True
                                break
                        if aff_found:
                            continue
                        time_str = ""
                        if len(str(prod["NRO_ANO_PRESENTA"])) == 4:
                            time_str += str(prod["NRO_ANO_PRESENTA"])
                        else:
                            continue
                        if len(str(prod["NRO_MES_PRESENTA"])) < 2:
                            time_str += "-0" + str(prod["NRO_MES_PRESENTA"])
                        elif len(str(prod["NRO_MES_PRESENTA"])) == 2:
                            time_str += "-" + str(prod["NRO_MES_PRESENTA"])
                        aff_time = int(dt.strptime(
                            time_str, "%Y-%m").timestamp())
                        group_entry = {
                            "id": group_db["_id"], "name": name, "types": group_db["types"], "start_date": aff_time, "end_date": -1}
                        if group_entry not in person["affiliations"]:
                            person["affiliations"].append(group_entry)

                    au = prod["author"][0]
                    if "TPO_PERFIL" not in au.keys():
                        continue
                    if au["TPO_PERFIL"] in ranks:
                        continue
                    date = ""
                    if "DTA_CREACION" in prod.keys():
                        date = int(dt.strptime(
                            prod["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                    rank_entry = {
                        "date": date,
                        "rank": au["TPO_PERFIL"],
                        "source": "scienti"
                    }
                    rank.append(rank_entry)
                    ranks.append(au["TPO_PERFIL"])

                self.collection.update_one({"_id": person["_id"]}, {"$set": {
                    "external_ids": person["external_ids"],
                    "first_names": person["first_names"],
                    "last_names": person["last_names"],
                    "initials": initials,
                    "full_name": " ".join(person["first_names"]) + " " + " ".join(person["last_names"]),
                    "updated": person["updated"],
                    "affiliations": person["affiliations"],
                    "ranking": rank,
                    "marital_status": marital,
                    "birthplace": birthplace
                }})

    def insert_scienti(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]

        for rh in scienti.distinct("author.COD_RH"):
            author_db = self.collection.find_one({"external_ids.id": rh})
            if author_db:
                continue
            author_scienti = scienti.find_one({"author.COD_RH": rh})
            if author_scienti:
                author = author_scienti["author"][0]
                if "NRO_DOCUMENTO_IDENT" in author.keys():
                    author_db = self.collection.find_one(
                        {"external_ids.id": author["NRO_DOCUMENTO_IDENT"]})
                if not author_db:
                    if "COD_ORCID" in author.keys():
                        author_db = self.collection.find_one(
                            {"external_ids.id": author["COD_ORCID"]})
                if not author_db:
                    if "AUTHOR_ID_SCP" in author.keys():
                        author_db = self.collection.find_one(
                            {"external_ids.id": author["AUTHOR_ID_SCP"]})
                if not author_db:
                    entry = self.empty_person()
                    entry["updated"].append(
                        {"time": int(time()), "source": "scienti"})
                    if author["TPO_DOCUMENTO_IDENT"] == "P":
                        entry["external_ids"].append(
                            {"source": "Passport", "id": author["NRO_DOCUMENTO_IDENT"]})
                    if author["TPO_DOCUMENTO_IDENT"] == "C":
                        entry["external_ids"].append(
                            {"source": "Cédula de Ciudadanía", "id": author["NRO_DOCUMENTO_IDENT"]})
                    if author["TPO_DOCUMENTO_IDENT"] == "E":
                        entry["external_ids"].append(
                            {"source": "Cédula de Extranjería", "id": author["NRO_DOCUMENTO_IDENT"]})
                    entry["external_ids"].append(
                        {"source": "scienti", "id": author["COD_RH"]})
                    if "COD_ORCID" in author.keys():
                        if author["COD_ORCID"]:
                            entry["external_ids"].append(
                                {"source": "orcid", "id": author["COD_ORCID"]})
                    entry["first_names"] = author["TXT_NAMES_RH"].strip().split()
                    entry["last_names"] = []
                    if "TXT_PRIM_APELL" in author.keys():
                        entry["last_names"].append(author["TXT_PRIM_APELL"])
                    if "TXT_SEG_APELL" in author.keys():
                        entry["last_names"].append(author["TXT_SEG_APELL"])
                    entry["full_name"] = " ".join(
                        entry["first_names"]) + " " + " ".join(entry["last_names"])
                    entry["initials"] = "".join(
                        [p[0].upper() for p in entry["first_names"]])
                    if "TXT_CITACION_BIBLIO" in author.keys():
                        entry["aliases"].append(
                            author["TXT_CITACION_BIBLIO"].lower())
                    if "TPO_SEXO" in author.keys():
                        entry["sex"] = author["TPO_SEXO"].lower()
                    if "TPO_PERFIL" in author.keys():
                        ranking = {
                            "date": "", "rank": author["TPO_PERFIL"], "source": "scienti"}
                        if ranking not in entry["ranking"]:
                            entry["ranking"].append(ranking)
                    if "institution" in author_scienti.keys():
                        aff_db = db["affiliations"].find_one(
                            {"external_ids.id": author_scienti["institution"][0]["COD_INST"]})
                        if aff_db:
                            name = aff_db["names"][0]["name"]
                            for n in aff_db["names"]:
                                if n["lang"] == "es":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "en":
                                    name = n["name"]
                            entry["affiliations"].append({
                                "id": aff_db["_id"],
                                "name": name,
                                "types": aff_db["types"],
                                "start_date": -1,
                                "end_date": -1
                            })

                    if "TPO_ESTADO_CIVIL" in author.keys():
                        if author["TPO_ESTADO_CIVIL"] == "C":
                            entry["marital_status"] = "Married"
                        elif author["TPO_ESTADO_CIVIL"] == "S":
                            entry["marital_status"] = "Single"
                        elif author["TPO_ESTADO_CIVIL"] == "U":
                            entry["marital_status"] = "Domestic Partnership"
                        elif author["TPO_ESTADO_CIVIL"] == "D":
                            entry["marital_status"] = "Divorced"
                        elif author["TPO_ESTADO_CIVIL"] == "V":
                            entry["marital_status"] = "Widowed"

                    if "city" in author.keys():
                        city = author["city"][0]
                        entry["birthplace"] = {
                            "city": city["TXT_NME_MUNICIPIO"].capitalize(),
                            "state": city["department"][0]["TXT_NME_DEPARTAMENTO"].capitalize(),
                            "country": city["department"][0]["country"][0]["TXT_NME_PAIS"].capitalize()
                        }

                    rank = []
                    ranks = []
                    for prod in scienti.find({"author.COD_RH": rh}):
                        group_entry = {}
                        group_db = db["affiliations"].find_one(
                            {"external_ids.id": prod["group"][0]["COD_ID_GRUPO"]})
                        if group_db:
                            name = group_db["names"][0]["name"]
                            for n in group_db["names"]:
                                if n["lang"] == "es":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "en":
                                    name = n["name"]
                            aff_found = False
                            for aff in entry["affiliations"]:
                                if group_db["_id"] == aff["id"]:
                                    aff_found = True
                                    break
                            if aff_found:
                                continue
                            time_str = ""
                            if len(str(prod["NRO_ANO_PRESENTA"])) == 4:
                                time_str += str(prod["NRO_ANO_PRESENTA"])
                            else:
                                continue
                            if len(str(prod["NRO_MES_PRESENTA"])) < 2:
                                time_str += "-0" + \
                                    str(prod["NRO_MES_PRESENTA"])
                            elif len(str(prod["NRO_MES_PRESENTA"])) == 2:
                                time_str += "-" + str(prod["NRO_MES_PRESENTA"])
                            aff_time = int(dt.strptime(
                                time_str, "%Y-%m").timestamp())
                            group_entry = {
                                "id": group_db["_id"], "name": name, "types": group_db["types"], "start_date": aff_time, "end_date": -1}
                            if group_entry not in entry["affiliations"]:
                                entry["affiliations"].append(group_entry)

                        au = prod["author"][0]
                        if "TPO_PERFIL" not in au.keys():
                            continue
                        if au["TPO_PERFIL"] in ranks:
                            continue
                        date = ""
                        if "DTA_CREACION" in prod.keys():
                            date = int(dt.strptime(
                                prod["DTA_CREACION"], "%a, %d %b %Y %H:%M:%S %Z").timestamp())
                        rank_entry = {
                            "date": date,
                            "rank": au["TPO_PERFIL"],
                            "source": "scienti"
                        }
                        rank.append(rank_entry)
                        ranks.append(au["TPO_PERFIL"])
                    if rank:
                        entry["ranking"] = rank

                    self.collection.insert_one(entry)

    def run(self):
        for config in self.config["scienti_person"]["databases"]:
            if self.verbose > 0:
                print("Processing {} database".format(config["database_name"]))
            if self.verbose > 4:
                print("Updating already inserted entries")
            self.update_inserted(config, verbose=self.verbose)
            if self.verbose > 4:
                print("Inserting new entries")
            self.insert_scienti(config, verbose=self.verbose)

        return 0