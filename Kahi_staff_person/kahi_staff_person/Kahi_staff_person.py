from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_excel, isna, to_datetime
from time import time
from kahi_impactu_utils.String import title_case
from datetime import datetime as dt


class Kahi_staff_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.verbose = config["verbose"] if "verbose" in config else 0

        self.required_columns = [
            "tipo_documento", "identificación", "primer_apellido", "segundo_apellido",
            "nombres", "nivel_académico", "tipo_contrato", "jornada_laboral",
            "categoría_laboral", "sexo", "fecha_nacimiento", "fecha_inicial_vinculación",
            "fecha_final_vinculación", "código_unidad_académica", "unidad_académica", "código_subunidad_académica",
            "subunidad_académica"
        ]

    def process_staff(self):
        for idx in list(self.cedula_dep.keys()):
            check_db = self.collection.find_one({"external_ids.id": idx})
            if check_db:
                continue
            entry = self.empty_person()
            entry["updated"].append({"time": int(time()), "source": "staff", "provenance": "staff"})
            entry["first_names"] = self.data[self.data["identificación"] == idx].iloc[0]["nombres"].split()
            entry["last_names"].append(self.data[self.data["identificación"] == idx].iloc[0]["primer_apellido"])

            segundo_apellido = None
            segundo_apellido = self.data[self.data["identificación"] == idx].iloc[0]["segundo_apellido"]
            if segundo_apellido != "":
                entry["last_names"].append(segundo_apellido)

            entry["full_name"] = " ".join(entry["first_names"] + entry["last_names"])
            entry["initials"] = "".join([name[0] for name in entry["first_names"]])
            for i, reg in self.data[self.data["identificación"] == idx].iterrows():
                aff_time = None
                end_date = None
                if reg["fecha_inicial_vinculación"]:
                    aff_time = int(dt.strptime(reg["fecha_inicial_vinculación"], "%d/%m/%Y").timestamp())
                if reg["fecha_final_vinculación"]:
                    end_date = int(dt.strptime(reg["fecha_final_vinculación"], "%d/%m/%Y").timestamp())
                name = self.staff_reg["names"][0]["name"]
                for n in self.staff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                name = title_case(name)
                udea_aff = {"id": self.staff_reg["_id"], "name": name,
                            "types": self.staff_reg["types"], "start_date": aff_time, "end_date": end_date if end_date else -1}
                if udea_aff["id"] not in [aff["id"] for aff in entry["affiliations"]]:
                    entry["affiliations"].append(udea_aff)
                if reg["tipo_documento"].strip() == "cédula de ciudadanía":
                    id_entry = {"provenance": "staff",
                                "source": "Cédula de Ciudadanía", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                elif reg["tipo_documento"].strip() == "cédula de extranjería":
                    id_entry = {"provenance": "staff",
                                "source": "Cédula de Extranjería", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                elif reg["tipo_documento"].strip() == "pasaporte":
                    id_entry = {"provenance": "staff",
                                "source": "Pasaporte", "id": idx}
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                else:
                    print(
                        f"ERROR: tipo_documento have to be cédula de ciudadanía, cédula de extranjería or pasaporte not {reg['tipo_documento']}")
                if reg["nombres"].lower() not in entry["aliases"]:
                    entry["aliases"].append(reg["nombres"].lower())

                dep = self.db["affiliations"].find_one(
                    {"names.name": title_case(reg["subunidad_académica"]), "relations.id": self.staff_reg["_id"]})
                if dep:
                    name = dep["names"][0]["name"]
                    for n in dep["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    dep_affiliation = {
                        "id": dep["_id"], "name": name, "types": dep["types"], "start_date": aff_time, "end_date": end_date if end_date else -1}
                    if dep_affiliation["id"] not in [aff["id"] for aff in entry["affiliations"]]:
                        entry["affiliations"].append(dep_affiliation)
                fac = self.db["affiliations"].find_one(
                    {"names.name": title_case(reg["unidad_académica"]), "relations.id": self.staff_reg["_id"]})
                if fac:
                    name = fac["names"][0]["name"]
                    for n in fac["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    fac_affiliation = {
                        "id": fac["_id"], "name": name, "types": fac["types"], "start_date": aff_time, "end_date": end_date if end_date else -1}
                    if fac_affiliation["id"] not in [aff["id"] for aff in entry["affiliations"]]:
                        entry["affiliations"].append(fac_affiliation)

                if reg["fecha_nacimiento"] != "":
                    entry["birthdate"] = int(dt.strptime(reg["fecha_nacimiento"], "%d/%m/%Y").timestamp())
                else:
                    entry["birthdate"] = None
                entry["sex"] = reg["sexo"].lower()
                if not isna(reg["nivel_académico"]):
                    degree = {"date": -1, "degree": reg["nivel_académico"], "id": "", "institutions": [
                    ], "source": "nivel_académico", "provenance": "staff"}
                    if degree not in entry["degrees"]:
                        entry["degrees"].append(degree)
                if not isna(reg["tipo_contrato"]):
                    ranking = {"date": aff_time,
                               "rank": reg["tipo_contrato"], "source": "tipo_contrato", "provenace": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)
                if not isna(reg["jornada_laboral"]):
                    ranking = {"date": aff_time,
                               "rank": reg["jornada_laboral"], "source": "jornada_laboral", "provenace": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)
                if not isna(reg["categoría_laboral"]):
                    ranking = {"date": aff_time,
                               "rank": reg["categoría_laboral"], "source": "categoría_laboral", "provenace": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)

            self.collection.insert_one(entry)

    def run(self):
        if self.verbose > 4:
            start_time = time()

        for config in self.config["staff_person"]["databases"]:
            institution_id = config["institution_id"]

            self.staff_reg = self.db["affiliations"].find_one(
                {"external_ids.id": institution_id})
            if not self.staff_reg:
                print("Institution not found in database")
                raise ValueError(
                    f"Institution {institution_id} not found in database")

            file_path = config["file_path"]

            # read the excel file
            dtype_mapping = {col: str for col in self.required_columns}
            self.data = read_excel(file_path, dtype=dtype_mapping).fillna("")

            # check if all required columns are present
            for aff in self.required_columns:
                if aff not in self.data.columns:
                    print(
                        f"Column {aff} not found in file {file_path}, and it is required.")
                    raise ValueError(
                        f"Column {aff} not found in file {file_path}")

            # logs for higher verbosity
            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.cedula_dep = {}
            self.cedula_fac = {}

            if self.verbose > 1:
                print("Processing staff authors for institution: ", self.staff_reg["names"][0]["name"])

            for idx, reg in self.data.iterrows():
                self.cedula_fac[reg["identificación"]] = title_case(reg["unidad_académica"])
                self.cedula_dep[reg["identificación"]] = title_case(reg["subunidad_académica"])

            # convert dates to the correct format
            for col in ["fecha_nacimiento", "fecha_inicial_vinculación", "fecha_final_vinculación"]:
                self.data[col] = to_datetime(self.data[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna("")

            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.process_staff()

        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
