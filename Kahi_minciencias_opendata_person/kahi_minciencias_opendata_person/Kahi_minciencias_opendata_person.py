from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from re import search
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import get_id_from_url, get_id_type_from_url, parse_sex, check_date_format, split_names


def parse_ids(product_id, regex, values):
    """
    depending of the product type, the id is parsed in different ways. This function is used to parse the id of the product
    to extract the different ids that are used in the scienti database.

    Parameters
    ----------
    product_id : str
        The id of the product.
    regex : str
        The regex to be used to parse the id.
    values : list
        The values to be extracted from the id.
    """
    match = search(regex, product_id)
    ids = {}
    if match:
        for i, value in enumerate(values):
            ids[value] = match.group(i + 1)
    return ids


def process_info_from_works(db, author, entry, groups_production_list):
    # Works
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
                aff = {
                    "name": name,
                    "id": group_db["_id"],
                    "types": group_db["types"],
                    "start_date": check_date_format(reg["fcreacion_pd"]),
                    "end_date": "",
                    "position": ""
                }
                found = False
                for i in entry["affiliations"]:
                    if i["id"] == aff["id"]:
                        found = True
                        break
                if not found:
                    entry["affiliations"].append(aff)
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
                            aff = {
                                "name": name,
                                "id": rel["id"],
                                "types": rel["types"] if "types" in rel.keys() else [],
                                "start_date": check_date_format(reg["fcreacion_pd"]),
                                "end_date": "",
                                "position": ""
                            }

                            found = False
                            for i in entry["affiliations"]:
                                if i["id"] == aff["id"]:
                                    found = True
                                    break
                            if not found:
                                entry["affiliations"].append(aff)

    patents = ["Patente de invención", "Patente modelo de utilidad"]
    events = ["Evento científico",
              "Eventos artísticos, de arquitectura o de diseño con componentes de apropiación", "Eventos artísticos"]

    for reg in papers:
        if reg["nme_tipologia_pd"] in ['Obras o productos de arte, arquitectura y diseño']:
            ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})', [
                "COD_RH", "COD_PRODUCTO", "SEQ_PRODUCTO"])
            if ids:
                entry["related_works"].append(
                    {"provenance": "minciencias", "source": "scienti", "id": ids})

        elif reg["nme_tipologia_pd"] in ['Registro general', 'Registros de acuerdos de licencia para la explotación de obras']:
            ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})', [
                            "COD_RH", "COD_PRODUCTO", "COD_REGISTRO"])
            if ids:
                entry["related_works"].append(
                    {"provenance": "minciencias", "source": "scienti", "id": ids})

        elif reg["nme_tipologia_pd"] in ['Secreto empresarial']:
            ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})', [
                "COD_RH", "COD_PRODUCTO", "COD_SECRETO_INDUSTRIAL"])
            if ids:
                entry["related_works"].append(
                    {"provenance": "minciencias", "source": "scienti", "id": ids})

        elif reg["nme_tipologia_pd"] in patents:
            ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})$', [
                "COD_RH", "COD_PRODUCTO", " COD_PATENTE"])
            if ids:
                entry["related_works"].append(
                    {"provenance": "minciencias", "source": "scienti", "id": ids})

        elif reg["nme_tipologia_pd"] in events:
            ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})$', [
                "COD_RH", "COD_EVENTO"])
            if ids:
                entry["related_works"].append(
                    {"provenance": "minciencias", "source": "scienti", "id": ids})

        else:
            ids = parse_ids(
                reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})$', ["COD_RH", "COD_PRODUCTO"])
            if ids:
                entry["related_works"].append(
                    {"provenance": "minciencias", "source": "scienti", "id": ids})


def process_one(author, db, collection, empty_person, cvlac_profile, groups_production_list, verbose):

    if not author or not cvlac_profile:
        return

    # Define the author as a dictionary if it is not to permit the use of the same function for the cvlac_profile and the private_profiles.
    author = author if isinstance(author, dict) else {"id_persona_pr": author}

    auid = author["id_persona_pr"]
    reg_db = collection.find_one({"external_ids.id.COD_RH": auid})
    if reg_db:
        # Author update
        sources = [x["source"] for x in reg_db["updated"]]
        if "minciencias" in sources:
            return
        # Updated
        sources = [x["source"] for x in reg_db["updated"]]
        if "minciencias" not in sources:
            reg_db["updated"].append({
                "source": "minciencias",
                "time": int(time())})

        if cvlac_profile:
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
        if "nme_gran_area_pr" and "nme_area_pr" in author.keys():
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
        # Ranking
        if "nme_clasificacion_pr" in author.keys():
            entry_rank = {
                "source": "minciencias",
                "rank": author["nme_clasificacion_pr"],
                "id": author["id_clas_pr"],
                "order": author["orden_clas_pr"],
                "date": check_date_format(author["ano_convo"])
            }
            reg_db["ranking"].append(entry_rank)

        # Affiliations and related_works
        process_info_from_works(db, author, reg_db, groups_production_list)
        # Update the record
        collection.update_one(
            {"_id": reg_db["_id"]},
            {"$set": {
                "updated": reg_db["updated"],
                "external_ids": reg_db["external_ids"],
                "subjects": reg_db["subjects"],
                "related_works": reg_db["related_works"],
                "ranking": reg_db["ranking"],
                "affiliations": reg_db["affiliations"]
            }})
        return

    entry = empty_person.copy()
    entry["updated"].append({
        "source": "minciencias",
        "time": int(time())})

    # Author creation
    if cvlac_profile:
        if "datos_generales" in cvlac_profile.keys():
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

        entry["external_ids"].append({
            "provenance": "minciencias",
            "source": "scienti",
            "id": {"COD_RH": cvlac_profile["id_persona_pr"]}
        })

        if "datos_generales" in cvlac_profile.keys() and cvlac_profile["datos_generales"]:
            full_name = split_names(cvlac_profile["datos_generales"]["Nombre"])
            full_name_clean = " ".join(full_name.replace(".", " ").split())
            entry["full_name"] = full_name_clean
            entry["first_names"] = full_name["first_names"]
            entry["last_names"] = full_name["last_names"]
            entry["initials"] = full_name["initials"]

        if "sexo" in cvlac_profile["datos_generales"].keys():
            entry["sex"] = parse_sex(cvlac_profile["datos_generales"]["Sexo"].lower(
            )) if "Sexo" in cvlac_profile["datos_generales"].keys() else ""

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
    # Pending to add the degrees

    # subjects
    if "nme_gran_area_pr" and "nme_area_pr" in author.keys():
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

    # affiliations and related works
    process_info_from_works(db, author, entry, groups_production_list)

    # Ranking
    if "nme_clasificacion_pr" in author.keys():
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

        if config["minciencias_opendata_person"]["private_profiles"] not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["minciencias_opendata_person"]['private_profiles'], config["minciencias_opendata_person"]["database_url"]))
        self.private_profiles = self.openadata_db[config["minciencias_opendata_person"]
                                                  ["private_profiles"]]

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
        cvlac_authors_list = list(self.researchers_collection.aggregate(
            pipeline, allowDiskUse=True))

        # Authors with private profile
        authors_private_profile_list = list(
            self.private_profiles.distinct("id_persona_pr"))

        if self.verbose > 4:
            print("Creating the aggregate for {} products.".format(
                self.groups_production.count_documents({})))

        # Group production aggregate
        pipeline = [
            # 0000000000 is a placeholder for missing id_persona_pd, there is not record for it, then we can omit it
            {'$match': {'id_persona_pd': {'$ne': '0000000000'}}},
            {"$sort": {"ano_convo": -1}},
            {'$group': {'_id': '$id_producto_pd', 'originalDoc': {'$first': '$$ROOT'}}},
            {'$replaceRoot': {'newRoot': '$originalDoc'}},
            {'$group': {'_id': '$id_persona_pd', 'products': {'$push': '$$ROOT'}}}
        ]
        production_cursor = self.groups_production.aggregate(
            pipeline, allowDiskUse=True)
        if production_cursor:
            groups_production_list = list(production_cursor)

        # authors not in the cvlac collection
        cvlac_data_ids = list(
            self.researchers_collection.distinct("id_persona_pr"))

        pipeline = [
            # 0000000000 is a placeholder for missing id_persona_pd, there is not record for it, then we can omit it
            {'$match': {'id_persona_pd': {'$ne': '0000000000', '$nin': cvlac_data_ids}}},
            {"$sort": {"ano_convo": -1}},
            {'$group': {'_id': '$id_producto_pd', 'originalDoc': {'$first': '$$ROOT'}}},
            {'$replaceRoot': {'newRoot': '$originalDoc'}},
            {'$group': {'_id': '$id_persona_pd', 'products': {'$push': '$$ROOT'}}}
        ]
        production_not_cvlac_cursor = self.groups_production.aggregate(
            pipeline, allowDiskUse=True)

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            person_collection = db["person"]
            # Process the authors with cvlac profile
            if self.verbose > 4:
                print("Processing {} authors in cvlac.".format(
                    len(cvlac_authors_list)))
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="threading")(
                delayed(process_one)(
                    author,
                    db,
                    person_collection,
                    self.empty_person(),
                    # Find the document in the cvlac_stage collection using the id_persona_pr field.
                    self.cvlac_stage.find_one(
                        {"id_persona_pr": author["id_persona_pr"]}),
                    groups_production_list,
                    self.verbose
                ) for author in cvlac_authors_list  # Iterate over the cvlac_authors_list
            )
            # Process the authors with private profiles
            if self.verbose > 4:
                print("Processing {} authors with private profiles.".format(
                    len(authors_private_profile_list)))
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="threading")(
                delayed(process_one)(
                    author,
                    db,
                    person_collection,
                    self.empty_person(),
                    # Find the document in the private_profiles collection using the id_persona_pr field.
                    self.private_profiles.find_one(
                        {"id_persona_pr": author}),
                    groups_production_list,
                    self.verbose
                ) for author in authors_private_profile_list  # Iterate over the authors_private_profile_list
            )
            if production_not_cvlac_cursor:
                groups_production_not_cvlac_list = list(
                    production_not_cvlac_cursor)
                # Extract the id_persona_pr id from the groups_production_not_cvlac_list
                authors_not_cvlac_ids = set(
                    [author["_id"] for author in groups_production_not_cvlac_list])
                if self.verbose > 4:
                    print("Processing {} authors not in cvlac.".format(
                        len(groups_production_not_cvlac_list)))
                Parallel(
                    n_jobs=self.n_jobs,
                    verbose=10,
                    backend="threading")(
                    delayed(process_one)(
                        author,
                        db,
                        person_collection,
                        self.empty_person(),
                        # Find the document in the cvlac_stage collection using the id_persona_pr field.
                        self.cvlac_stage.find_one(
                            {"id_persona_pr": author}),
                        groups_production_not_cvlac_list,
                        self.verbose
                    ) for author in list(authors_not_cvlac_ids)  # Iterate over the ids of the authors not in cvlac.
                )
            client.close()

    def run(self):
        self.process_openadata()
        return 0
