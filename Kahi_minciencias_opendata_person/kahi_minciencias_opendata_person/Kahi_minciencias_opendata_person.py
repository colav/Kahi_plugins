from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import get_id_from_url, get_id_type_from_url, parse_sex, check_date_format, split_names


def process_one(author, db, collection, empty_person, cvlac_profile, groups_production_list, verbose):
    if not author or not cvlac_profile:
        return
    auid = author["id_persona_pr"]
    reg_db = collection.find_one({"external_ids.id.COD_RH": auid})
    if reg_db:
        # Author update
        sources = [x["source"] for x in reg_db["updated"]]
        if "minciencias" in sources:
            return
        # Updated
        reg_db["updated"].append({
            "source": "minciencias",
            "time": int(time())})
        # Identifiers
        ids = set()
        if "red_identificadores" in cvlac_profile.keys():
            if cvlac_profile["red_identificadores"]:
                for rid in cvlac_profile["red_identificadores"].values():
                    ids.add(rid)
        if "redes_identificadoes" in cvlac_profile.keys():
            if cvlac_profile["redes_identificadoes"]:
                for rid in cvlac_profile["redes_identificadoes"].values():
                    ids.add(rid)
        if ids:
            for _id in list(ids):
                if isinstance(_id, str):
                    value = get_id_from_url(_id)
                    if value:
                        rec = {
                            "provenance": "minciencias",
                            "source": get_id_type_from_url(_id),
                            "id": value
                        }
                        if rec["id"] not in [x["id"] for x in reg_db["external_ids"]]:
                            if rec not in reg_db["external_ids"]:
                                reg_db["external_ids"].append(rec)
        # Subjects
        reg_db["subjects"].append({
            "provenance": "minciencias",
            "source": "OECD",
            "subjects": [
                {
                    "level": 0,
                    "name": author["nme_gran_area_pr"],
                    "id": "",
                    "external_ids": [{"source": "OECD", "id": author["id_area_con_pr"][0]}]
                },
                {
                    "level": 1,
                    "name": author["nme_area_pr"],
                    "id": "",
                    "external_ids": [{"source": "OECD", "id": author["id_area_con_pr"][1]}]
                },
            ]
        })
        # Works
        papers = []
        for prod in groups_production_list:
            if prod["_id"] == author["id_persona_pr"]:
                papers = prod["products"]
                break
        for reg in papers:
            if reg["id_producto_pd"]:
                cod_rh, cod_producto = reg["id_producto_pd"].split(
                    "-")[-2], reg["id_producto_pd"].split("-")[-1]
                rec = {"provenance": "minciencias", "source": "scienti",
                       "id": {"COD_RH": cod_rh, "COD_PRODUCTO": cod_producto}}
                if rec not in reg_db["related_works"]:
                    reg_db["related_works"].append({"provenance": "minciencias", "source": "scienti", "id": {
                                                   "COD_RH": cod_rh, "COD_PRODUCTO": cod_producto}})
        # Ranking
        entry_rank = {
            "source": "minciencias",
            "rank": author["nme_clasificacion_pr"],
            "id": author["id_clas_pr"],
            "order": author["orden_clas_pr"],
            "date": check_date_format(author["ano_convo"])
        }
        reg_db["ranking"].append(entry_rank)
        # Update the record
        collection.update_one(
            {"_id": reg_db["_id"]},
            {"$set": {
                "updated": reg_db["updated"],
                "external_ids": reg_db["external_ids"],
                "subjects": reg_db["subjects"],
                "related_works": reg_db["related_works"],
                "ranking": reg_db["ranking"]
            }})
        if verbose > 4:
            print("Updated author {}".format(auid))
        return

    # Author creation
    if "0000000082" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Mujer"
    if "0001385093" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Mujer"
    if "0001506130" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001393305" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001353302" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001165976" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001437782" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0000287938" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001511182" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0000037796" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001386076" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0000346748" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0000327220" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001317792" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0001103741" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0000059161" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"
    if "0000896519" in author["id_persona_pr"]:
        cvlac_profile["datos_generales"]["Sexo"] = "Hombre"

    entry = empty_person.copy()
    entry["updated"].append({
        "source": "minciencias",
        "time": int(time())})

    full_name = split_names(cvlac_profile["datos_generales"]["Nombre"])
    entry["full_name"] = full_name["full_name"]
    entry["first_names"] = full_name["first_names"]
    entry["last_names"] = full_name["last_names"]
    entry["initials"] = full_name["initials"]

    entry["sex"] = parse_sex(cvlac_profile["datos_generales"]["Sexo"].lower(
    )) if "Sexo" in cvlac_profile["datos_generales"].keys() else ""

    entry["external_ids"].append({
        "provenance": "minciencias",
        "source": "scienti",
        "id": {"COD_RH": cvlac_profile["id_persona_pr"]}
    })

    # all the ids are mixed, so we need to check each one in the next columns
    ids = set()
    if "red_identificadores" in cvlac_profile.keys():
        if cvlac_profile["red_identificadores"]:
            for rid in cvlac_profile["red_identificadores"].values():
                ids.add(rid)

    if "redes_identificadoes" in cvlac_profile.keys():
        if cvlac_profile["redes_identificadoes"]:
            for rid in cvlac_profile["redes_identificadoes"].values():
                ids.add(rid)

    if ids:
        for _id in list(ids):
            if isinstance(_id, str):
                value = get_id_from_url(_id)
                if value:
                    rec = {
                        "provenance": "minciencias",
                        "source": get_id_type_from_url(_id),
                        "id": value
                    }
                    if rec not in entry["external_ids"]:
                        entry["external_ids"].append(rec)

    # degrees

    # subjects
    entry["subjects"].append({
        "provenance": "minciencias",
        "source": "OECD",
        "subjects": [
            {
                "level": 0,
                "name": author["nme_gran_area_pr"],
                "id": "",
                "external_ids": [{"source": "OECD", "id": author["id_area_con_pr"][0]}]
            },
            {
                "level": 1,
                "name": author["nme_area_pr"],
                "id": "",
                "external_ids": [{"source": "OECD", "id": author["id_area_con_pr"][1]}]
            },
        ]
    })

    papers = []
    for prod in groups_production_list:
        if prod["_id"] == author["id_persona_pr"]:
            papers = prod["products"]
            break

    if papers:
        groups_cod = []
        inst_cod = []
        for reg in papers:
            if reg["cod_grupo_gr"] in groups_cod:
                continue
            groups_cod.append(reg["cod_grupo_gr"])
            group_db = db["affiliations"].find_one(
                {"external_ids.id": reg["cod_grupo_gr"]})
            if group_db:
                name = group_db["names"][0]["name"]
                for n in group_db["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                entry["affiliations"].append({
                    "name": name,
                    "id": group_db["_id"],
                    "types": group_db["types"],
                    "start_date": check_date_format(reg["fcreacion_pd"]),
                    "end_date": "",
                    "position": ""
                })
                if "relations" in group_db.keys():
                    if group_db["relations"]:
                        for rel in group_db["relations"]:
                            if rel["id"] in inst_cod:
                                continue
                            inst_cod.append(rel["id"])
                            if "names" in rel.keys():
                                name = rel["names"][0]["name"]
                                for n in rel["names"]:
                                    if n["lang"] == "es":
                                        name = n["name"]
                                        break
                                    elif n["lang"] == "en":
                                        name = n["name"]
                            else:
                                name = rel["name"]
                            entry["affiliations"].append({
                                "name": name,
                                "id": rel["id"],
                                "types": rel["types"] if "types" in rel.keys() else [],
                                "start_date": check_date_format(reg["fcreacion_pd"]),
                                "end_date": "",
                                "position": ""
                            })
        # Works
        for reg in papers:
            if reg["id_producto_pd"]:
                cod_rh, cod_producto = reg["id_producto_pd"].split(
                    "-")[-2], reg["id_producto_pd"].split("-")[-1]
                entry["related_works"].append({"provenance": "minciencias", "source": "scienti", "id": {
                                              "COD_RH": cod_rh, "COD_PRODUCTO": cod_producto}})

    # print("Adding ranks to ", auid)
    entry_rank = {
        "source": "minciencias",
        "rank": author["nme_clasificacion_pr"],
        "id": author["id_clas_pr"],
        "order": author["orden_clas_pr"],
        "date": check_date_format(author["ano_convo"])
    }
    entry["ranking"].append(entry_rank)
    collection.insert_one(entry)


class Kahi_minciencias_opendata_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.openadata_client = MongoClient(
            config["minciencias_opendata_person"]["database_url"])
        if config["minciencias_opendata_person"]["database_name"] not in self.openadata_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["minciencias_opendata_person"]['database_name'], config["minciencias_opendata_person"]["database_url"]))
        self.openadata_db = self.openadata_client[config["minciencias_opendata_person"]["database_name"]]

        if config["minciencias_opendata_person"]["researchers"] not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["minciencias_opendata_person"]['researchers'], config["minciencias_opendata_person"]["database_url"]))
        self.researchers_collection = self.openadata_db[
            config["minciencias_opendata_person"]["researchers"]]

        if config["minciencias_opendata_person"]["cvlac"] not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["minciencias_opendata_person"]['cvlac'], config["minciencias_opendata_person"]["database_url"]))
        self.cvlac_stage = self.openadata_db[config["minciencias_opendata_person"]["cvlac"]]

        if config["minciencias_opendata_person"]["groups_production"] not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["minciencias_opendata_person"]['groups_production'], config["minciencias_opendata_person"]["database_url"]))
        self.groups_production = self.openadata_db[config["minciencias_opendata_person"]
                                                   ["groups_production"]]

        self.n_jobs = config["minciencias_opendata_person"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_person"].keys(
        ) else 1

        self.verbose = config["minciencias_opendata_person"][
            "verbose"] if "verbose" in config["minciencias_opendata_person"].keys() else 0

    def process_openadata(self):

        # Authors aggregate
        if self.verbose > 4:
            print("Creating the aggregate for {} authors.".format(
                self.researchers_collection.count_documents({})))
        pipeline = [
            {"$sort": {"edad_anos_pr": -1}},
            {"$group": {"_id": "$id_persona_pr", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}}
        ]
        authors_cursor = self.researchers_collection.aggregate(
            pipeline, allowDiskUse=True)

        # Group production aggregate
        if self.verbose > 4:
            print("Creating the aggregate for {} products.".format(
                self.groups_production.count_documents({})))
        categories = ['ART-00', 'ART-ART_A1', 'ART-ART_A2',
                      'ART-ART_B', 'ART-ART_C', 'ART-ART_D', 'ART-GC_ART']
        pipeline = [
            {'$match': {'id_tipo_pd_med': {'$in': categories}}},
            {"$sort": {"ano_convo": -1}},
            {'$group': {'_id': '$id_producto_pd', 'originalDoc': {'$first': '$$ROOT'}}},
            {'$replaceRoot': {'newRoot': '$originalDoc'}},
            {'$group': {'_id': '$id_persona_pd', 'products': {'$push': '$$ROOT'}}}
        ]
        production_cursor = self.groups_production.aggregate(
            pipeline, allowDiskUse=True)
        if production_cursor:
            groups_production_list = list(production_cursor)

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            person_collection = db["person"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="threading")(
                delayed(process_one)(
                    author,
                    db,
                    person_collection,
                    self.empty_person(),
                    self.cvlac_stage.find_one(
                        {"id_persona_pr": author["id_persona_pr"]}),
                    groups_production_list,
                    self.verbose
                ) for author in authors_cursor
            )
            client.close()

    def run(self):
        self.process_openadata()
        return 0
