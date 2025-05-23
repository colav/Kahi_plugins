from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_excel
from time import time
from kahi_impactu_utils.String import title_case


class Kahi_staff_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("names.name")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.required_columns = ["unidad_académica", "subunidad_académica",
                                 "código_unidad_académica", "código_subunidad_académica"]

        self.verbose = config["verbose"] if "verbose" in config else 0

    def _id_creator(self, institution_id, reg, aff, unit):
        """
        Create a unique identifier (_id) based on affiliation and academic unit codes.

        Parameters
        ----------
        reg : dict
            A record containing academic unit and subunit codes.
            Expected keys: "código_unidad_académica", "código_subunidad_académica".
        aff : dict
            An affiliation record containing external IDs.
            Expected key: "external_ids" (a list of dictionaries with "id" and "source").
        unit : str
            Determines the type of identifier to generate.
            Must be either "código_unidad_académica" or "código_subunidad_académica".

        Returns
        -------
        str
            A unique identifier (_id) in the format:
            - For unidad="código_unidad_académica": "RORID_CODIGO_UNIDAD"
            - For other values: "RORID_CODIGO_UNIDAD_CODIGO_SUBUNIDAD"
        """

        _id = ""
        # Generate the identifier based on the provided unit code
        if unit == "código_unidad_académica":
            _id = f"{institution_id}_{reg['código_unidad_académica']}"
        if unit == "código_subunidad_académica":
            _id = f"{institution_id}_{reg['código_unidad_académica']}_{reg['código_subunidad_académica']}"

        return _id

    def staff_affiliation(self, data, institution_id, institution_name, staff_reg):
        # inserting faculties and departments
        for idx, reg in data.iterrows():
            name = title_case(reg["unidad_académica"])
            _id = self._id_creator(institution_id, reg,
                                   staff_reg, "código_unidad_académica")

            if name not in self.facs_inserted.keys():
                is_in_db = self.collection.find_one({"_id": _id})
                if is_in_db:
                    if name not in self.facs_inserted.keys():
                        self.facs_inserted[name] = is_in_db["_id"]
                        print(name, f" already in db {_id}")
                    # continue
                    # may be updatable, check accordingly
                else:
                    entry = self.empty_affiliation()
                    entry["_id"] = _id
                    entry["updated"].append(
                        {"time": int(time()), "source": "staff"})
                    entry["names"].append(
                        {"name": name, "lang": "es", "source": "staff"})
                    entry["types"].append(
                        {"source": "staff", "type": "faculty"})
                    entry["relations"].append(
                        {"id": staff_reg["_id"], "name": institution_name, "types": staff_reg["types"]})
                    if reg["código_unidad_académica"]:
                        entry["external_ids"].append(
                            {"source": "staff", "id": str(reg["código_unidad_académica"])})

                    fac = self.collection.insert_one(entry)
                    self.facs_inserted[name] = fac.inserted_id

            if reg["subunidad_académica"] != "":
                name_dep = title_case(reg["subunidad_académica"])
                name_dep_id = str(reg["código_unidad_académica"]) + \
                    "_" + title_case(reg["subunidad_académica"])
                _id = self._id_creator(
                    institution_id, reg, staff_reg, "código_subunidad_académica")
                if name_dep_id not in self.deps_inserted.keys():
                    is_in_db = self.collection.find_one({"_id": _id})
                    if is_in_db:
                        if name_dep_id not in self.deps_inserted.keys():
                            self.deps_inserted[name_dep_id] = is_in_db["_id"]
                            print(name_dep, f" already in db {_id}")
                        # continue
                        # may be updatable, check accordingly
                    else:
                        entry = self.empty_affiliation()
                        entry["_id"] = _id
                        entry["updated"].append(
                            {"time": int(time()), "source": "staff"})
                        entry["names"].append(
                            {"name": name_dep, "lang": "es", "source": "staff"})
                        entry["types"].append(
                            {"source": "staff", "type": "department"})
                        entry["relations"].append(
                            {"id": staff_reg["_id"], "name": institution_name, "types": staff_reg["types"]})
                        if reg["código_subunidad_académica"]:
                            entry["external_ids"].append(
                                {"source": "staff", "id": str(reg["código_subunidad_académica"])})

                        dep = self.collection.insert_one(entry)
                        self.deps_inserted[name_dep_id] = dep.inserted_id

                if (name, name_dep_id) not in self.fac_dep:
                    self.fac_dep.append((name, name_dep_id))

        # Creating relations between faculties and departments
        for fac, dep in self.fac_dep:
            fac_id = self.facs_inserted[fac]
            dep_id = self.deps_inserted[dep]
            dep_reg = self.collection.find_one({"_id": dep_id})
            fac_reg = self.collection.find_one({"_id": fac_id})
            if dep_reg["_id"] == fac_reg["_id"]:
                # Skip if faculty and department are the same
                continue
            self.collection.update_one(
                {"_id": fac_reg["_id"]},
                {"$addToSet": {
                    "relations": {
                        "id": dep_reg["_id"],
                        "name": title_case(dep_reg["names"][0]["name"]),
                        "types": dep_reg["types"]}}})
            self.collection.update_one(
                {"_id": dep_reg["_id"]},
                {"$addToSet": {
                    "relations": {
                        "id": fac_reg["_id"],
                        "name": title_case(fac_reg["names"][0]["name"]),
                        "types": fac_reg["types"]}}})
        return 0

    def run(self):
        if self.verbose > 4:
            start_time = time()

        for config in self.config["staff_affiliations"]["databases"]:
            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []
            institution_id = config["institution_id"].replace(
                "https://ror.org/", "")
            staff_reg = self.collection.find_one({"_id": institution_id})
            if not staff_reg:
                print("Institution not found in database")
                raise ValueError(
                    f"Institution {institution_id} not found in database")
            else:
                institution_name = ""
                for name in staff_reg["names"]:
                    if name["lang"] == "en":
                        institution_name = name["name"]
                if institution_name == "":  # if en not available take any
                    institution_name = staff_reg["names"][0]["name"]

            if self.verbose > 1:
                print("Processing staff affiliations for institution: ",
                      institution_name)

            file_path = config["file_path"]
            dtype_mapping = {col: str for col in self.required_columns}
            data = read_excel(file_path, dtype=dtype_mapping).fillna("")
            if self.verbose > 4:
                # Print shape of the data
                print("Data shape: ", data.shape)

            # Check if the columns are in the file
            for aff in self.required_columns:
                if aff not in data.columns:
                    print(
                        f"Column {aff} not found in file {file_path}, and it is required.")
                    raise ValueError(f"Column {aff} not found in file")
            self.staff_affiliation(data, institution_id,
                                   institution_name, staff_reg)

        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
