from kahi_impactu_utils.Utils import check_date_format
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
from datetime import datetime as dt
from kahi.KahiBase import KahiBase
from unidecode import unidecode
from thefuzz import fuzz
from time import time


class Kahi_minciencias_opendata_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("types.type")
        self.collection.create_index("names.name")
        self.collection.create_index([("names.name", TEXT)])

        self.openadata_client = MongoClient(
            config["minciencias_opendata_affiliations"]["database_url"])
        if config["minciencias_opendata_affiliations"]["database_name"] not in self.openadata_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["minciencias_opendata_affiliations"]['database_name'], config["minciencias_opendata_affiliations"]["database_url"]))

        self.openadata_db = self.openadata_client[config["minciencias_opendata_affiliations"]["database_name"]]

        if config["minciencias_opendata_affiliations"]["collection_name"] not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["minciencias_opendata_affiliations"]['collection_name'], config["minciencias_opendata_affiliations"]["database_url"]))

        self.openadata_collection = self.openadata_db[
            config["minciencias_opendata_affiliations"]["collection_name"]]

        self.n_jobs = config["minciencias_opendata_affiliations"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_affiliations"].keys(
        ) else 1

        self.verbose = config["minciencias_opendata_affiliations"][
            "verbose"] if "verbose" in config["minciencias_opendata_affiliations"].keys() else 0

        self.inserted_cod_grupo = []

        for reg in self.collection.find({"types.type": "group"}):
            for ext in reg["external_ids"]:
                if ext["source"] == "minciencias":
                    self.inserted_cod_grupo.append(ext["id"])

    def rename_institution(self, name):
        if name == "Colegio Mayor Nuestra Señora del Rosario".lower() or name == "Colegio Mayor de Nuestra Señora del Rosario".lower():
            return "universidad del rosario"
        elif name == "universidad de la guajira":
            return "guajira"
        elif "minuto" in name and "dios" in name:
            return "minuto dios"
        elif "salle" in name:
            return "universidad salle"
        elif "icesi" in name:
            return "icesi"
        elif "sede" in name:
            return name.split("sede")[0].strip()
        elif name == "universidad militar nueva granada":
            return "nueva granada"
        elif "pamplona" in name:
            return "pamplona"
        elif "sucre" in name:
            return "sucre"
        elif "santo tomás" in name or "santo tomas" in name:
            return "santo tomas"
        elif name == "universidad simón bolívar":
            return "simon bolivar"
        elif "unidades" in name and "santander" in name:
            return "unidades tecnológicas santander"
        elif "popayán" in name:
            return "popayán"
        elif "tecnológico metropolitano" in name:
            return "tecnológico metropolitano"
        elif "cesmag" in name:
            return "estudios superiores maría goretti"
        elif "distrital francisco" in name:
            return "distrital francisco josé"
        elif "santander" in name and "industrial" in name:
            return "industrial santander"
        elif "santander" in name and "industrial" not in name:
            return "universidad santander"
        elif "francisco" in name and "paula" in name and "santander" in name:
            return "francisco paula"
        elif "magdalena" in name:
            return "magdalena"
        elif "corporacion universitaria iberoamericana" == name:
            return "iberoamericana"
        else:
            return name

    def process_one(self, reg, collection, empty_affiliation, verbose):
        if "cod_grupo_gr" not in reg.keys() or not reg["cod_grupo_gr"]:
            return
        idgr = reg["cod_grupo_gr"]
        if idgr:
            db_reg = collection.find_one({"external_ids.id": idgr})
            if db_reg:
                if idgr not in self.inserted_cod_grupo:
                    self.inserted_cod_grupo.append(idgr)
                if "minciencias" in [idx["source"] for idx in db_reg["updated"]]:
                    return
                db_reg["updated"].append(
                    {"time": int(time()), "source": "minciencias"})
                if not db_reg["year_established"]:
                    date_established = check_date_format(
                        reg["fcreacion_gr"]) if "fcreacion_gr" in reg.keys() else ""
                    if date_established:
                        db_reg["year_established"] = dt.fromtimestamp(
                            date_established).year
                if not db_reg["addresses"]:
                    if not db_reg["relations"]:
                        pass
                    else:
                        if not db_reg["relations"][0]["id"]:
                            pass
                        else:
                            aff_db = collection.find_one({"_id": db_reg["relations"][0]["id"]})
                            if aff_db:
                                db_reg["addresses"].append({
                                    "lat": aff_db["addresses"][0].get("lat", None),
                                    "lng": aff_db["addresses"][0].get("lng", None),
                                    "postcode": aff_db["addresses"][0].get("postcode", None),
                                    "state": aff_db["addresses"][0].get("state", None),
                                    "city": aff_db["addresses"][0].get("city", None),
                                    "country": aff_db["addresses"][0].get("country", None),
                                    "country_code": aff_db["addresses"][0].get("country_code", None)
                                })
                collection.update_one(
                    {"_id": db_reg["_id"]},
                    {"$set": {
                        "updated": db_reg["updated"],
                        "year_established": db_reg.get("year_established"),
                        "addresses": db_reg.get("addresses")
                    }}, upsert=True)
                if verbose > 4:
                    print("Updated group {}".format(idgr))
                return

            self.inserted_cod_grupo.append(idgr)
            entry = empty_affiliation.copy()
            entry["updated"].append(
                {"source": "minciencias", "time": int(time())})
            entry["names"].append(
                {"source": "minciencias", "lang": "es", "name": reg["nme_grupo_gr"] if "nme_grupo_gr" in reg.keys() else ""})
            entry["types"].append({"source": "minciencias", "type": "group"})
            year_established = ""
            date_established = check_date_format(reg["fcreacion_gr"]) if "fcreacion_gr" in reg.keys() else ""
            if date_established:
                year_established = dt.fromtimestamp(date_established).year
            entry["year_established"] = year_established
            entry["external_ids"].append(
                {"source": "minciencias", "id": reg["cod_grupo_gr"]})
            entry["subjects"].append({
                "provenance": "minciencias",
                "source": "OECD",
                "subjects": [
                    {
                        "level": 0,
                        "name": reg["nme_gran_area_gr"] if "nme_gran_area_gr" in reg.keys() else "",
                        "id": "",
                        "external_ids": [{"source": "OECD", "id": reg["id_area_con_gr"][0] if "id_area_con_gr" in reg.keys() else ""}]
                    },
                    {
                        "level": 1,
                        "name": reg["nme_area_gr"] if "nme_area_gr" in reg.keys() else "",
                        "id": "",
                        "external_ids": [{"source": "OECD", "id": reg["id_area_con_gr"][1] if "id_area_con_gr" in reg.keys() else ""}]
                    },
                ]
            })

            # START AVAL INSTITUTION SECTION
            if "inst_aval" in reg.keys():
                for inst_aval in reg["inst_aval"].split("|"):
                    inst_aval = inst_aval.lower().strip()

                    inst_aval = self.rename_institution(inst_aval)

                    inst_aval = unidecode(inst_aval)
                    institutions = self.collection.find(
                        {"$text": {"$search": inst_aval}, "addresses.country": "Colombia"}).limit(50)
                    institution = ""
                    score = 10
                    for inst in institutions:
                        method = ""
                        name = ""
                        for n in inst["names"]:
                            if n["lang"] == "es":
                                name = n["name"]
                                break
                            elif n["lang"] == "en":
                                name = n["name"]
                        name_mod = name.lower().replace("(colombia)", "").replace(
                            "(", "").replace(")", "").replace("bogotá", "")
                        # name_mod=name_mod.replace("universidad","").replace("de","").replace("del","").replace("los","").strip()
                        name_mod = unidecode(name_mod)

                        if "santander" in name_mod and "industrial" in name_mod:
                            name_mod = "industrial santander"
                        if "santander" in name_mod and "industrial" not in name_mod:
                            name_mod = "universidad santander"
                        if "francisco" in name_mod and "paula" in name_mod and "santander" in name_mod:
                            inst_aval = "francisco paula"
                        score = fuzz.ratio(name_mod, inst_aval)
                        if score > 90:
                            method = "ratio"
                            institution = inst
                            break
                        elif score > 39:
                            score = fuzz.partial_ratio(name_mod, inst_aval)
                            # print("Partial ratio score: {}. {} -against- {}".format(score,name,reg["INST_AVAL"]))
                            if score > 93:
                                method = "partial ratio"
                                institution = inst
                                break
                            elif score > 55:
                                score = fuzz.token_set_ratio(name_mod, inst_aval)
                                # print("Token set ratio score: {}. {} -against- {}".format(score,name,reg["INST_AVAL"]))
                                if score > 98:
                                    method = "token set ratio"
                                    # print("Token set ratio score: {}. {} -against- {}".format(score,name,inst_aval))
                                    institution = inst
                                    break
                    if institution != "":
                        name = ""
                        for n in inst["names"]:
                            if n["lang"] == "es":
                                name = n["name"]
                                break
                            elif n["lang"] == "en":
                                name = n["name"]
                        entry["relations"].append(
                            {"types": institution["types"], "id": institution["_id"], "name": name})
                        entry["addresses"].append({
                            "lat": institution["addresses"][0].get("lat", None),
                            "lng": institution["addresses"][0].get("lng", None),
                            "postcode": institution["addresses"][0].get("postcode", None),
                            "state": institution["addresses"][0].get("state", None),
                            "city": institution["addresses"][0].get("city", None),
                            "country": institution["addresses"][0].get("country", None),
                            "country_code": institution["addresses"][0].get("country_code", None)
                        })
                    else:
                        if score == 98 and method == "token set ratio":
                            print(
                                "(LAST) {} score: {}. {} -against- {}".format(method, score, name, inst_aval))
                        entry["addresses"].append({
                            "lat": "",
                            "lng": "",
                            "postcode": "",
                            "state": reg["nme_departamento_gr"],
                            "city": reg["nme_municipio_gr"],
                            "country": "Colombia",
                            "country_code": "CO"
                        })
            # END AVAL INSTITUTION
            entry_rank = {
                "source": "minciencias",
                "rank": reg["nme_clasificacion_gr"] if "nme_clasificacion_gr" in reg.keys() else "",
                "order": reg["orden_clas_gr"] if "orden_clas_gr" in reg.keys() else "",
                "date": check_date_format(reg["ano_convo"] if "ano_convo" in reg.keys() else ""),
            }
            entry["ranking"].append(entry_rank)
            # END CLASSIFICATION SECTION
            entry["_id"] = idgr
            self.collection.insert_one(entry)
            if verbose > 4:
                print("Inserted group {}".format(idgr))

    def process_openadata(self):
        # Pipeline to find duplicate documents and keep the one with the highest edad_anos_gr in each group
        pipeline = [
            {
                "$sort": {"ano_convo": -1}  # Sort documents by edad_anos_gr in descending order
            },
            {
                "$group": {
                    "_id": "$cod_grupo_gr",  # Group documents by the group code
                    "doc": {"$first": "$$ROOT"}  # Select the first document of each group
                }
            },
            {
                "$replaceRoot": {"newRoot": "$doc"}  # Replace the root of the document with the selected documents
            }
        ]
        affiliation_cursor = self.openadata_collection.aggregate(
            pipeline, allowDiskUse=True)
        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["affiliations"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.process_one)(
                    aff,
                    collection,
                    self.empty_affiliation(),
                    self.verbose,
                ) for aff in affiliation_cursor
            )
            client.close()

    def run(self):
        self.process_openadata()
        self.client.close()
        return 0
