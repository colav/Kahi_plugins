from time import time
from urllib.parse import quote

from joblib import Parallel, delayed
from kahi.progress import ProgressLogger
from kahi.KahiBase import KahiBase
from pymongo import MongoClient


def commons_file_url(filename):
    """Build a valid Wikimedia Commons redirect URL for a file claim."""
    normalized = str(filename).strip().replace(" ", "_")
    return (
        "https://commons.wikimedia.org/w/index.php?title="
        f"Special:Redirect/file/{quote(normalized, safe='')}&width=300"
    )


def claim_file(claims, property_id):
    """Return the first non-empty file value for a Wikidata claim."""
    for claim in claims.get(property_id, []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if value:
            return value
    return None


def process_one(kahi_col, wikid_col, inst, verbose):
    """
    This function processes one institution, and seve the the titles and images from wikidata.
    Images are taken from P154 (logo) or P18 (image) if P154 is not present then P18 is used.

    Parameters:
    ----------
    kahi_col: pymongo.collection.Collection
        collection of institutions
    wikid_col: pymongo.collection.Collection
        collection of wikidata records
    inst: dict
        institution record
    verbose: int
    """
    wikidata_id = None
    for j in inst["external_ids"]:
        if j["source"] == "wikidata":
            wikidata_id = j["id"]
            break
    rec = wikid_col.find_one({"id": wikidata_id},
                             {"labels": 1,
                              "claims.P154.mainsnak.datavalue.value": 1,
                              "claims.P18.mainsnak.datavalue.value": 1})

    if not rec:
        if verbose > 4:
            print(f"WARNING: record with id {wikidata_id} not found")
        return "sin_registro_wikidata"
    names = inst.setdefault("names", [])
    known_names = {
        (name.get("source"), name.get("lang"), name.get("name")) for name in names
    }
    for lang, label in rec.get("labels", {}).items():
        key = ("wikidata", lang, label["value"])
        if key not in known_names:
            names.append({"name": label["value"], "lang": lang,
                          "source": "wikidata", "provenance": "wikidata"})
            known_names.add(key)

    claims = rec.get("claims", {})
    img = claim_file(claims, "P154") or claim_file(claims, "P18")
    if img:
        external_urls = inst.setdefault("external_urls", [])
        external_urls[:] = [
            item for item in external_urls
            if not (
                item.get("provenance") == "wikidata"
                and item.get("source") == "logo"
            )
        ]
        external_urls.append({
            "provenance": "wikidata",
            "source": "logo",
            "url": commons_file_url(img),
        })

    now = int(time())
    updated = inst.setdefault("updated", [])
    wikidata_update = next(
        (item for item in updated if item.get("source") == "wikidata"), None
    )
    if wikidata_update:
        wikidata_update["time"] = now
    else:
        updated.append({"source": "wikidata", "time": now})
    kahi_col.update_one({"_id": inst["_id"]},
                        {"$set": {"names": names,
                                  "external_urls": inst.get("external_urls", []),
                                  "updated": updated}})
    return "actualizados"


class Kahi_wikidata_affiliations(KahiBase):
    """
    Plugin to process wikidata affiliations.
    Information is taken from the wikidata collection and added to the affiliations collection.

    This plugin is not making requests to wikidata api, it is using the data from the wikidata collection.
    please raead https://github.com/colav-playground/wikidata_laod for more information on how to load the data.
    """

    config = {}

    def __init__(self, config):
        """
        Config is a dictionary with the following keys:
        wikidata_affiliations:
            database_name: wikidata
            collection_name: data
            num_jobs: 5
            verbose: 5

        This has to be added in the workflow file in yml.
        """
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.wikidata_db = self.client[config["wikidata_affiliations"]
                                       ["database_name"]]
        self.wikidata_col = self.wikidata_db[config["wikidata_affiliations"]
                                             ["collection_name"]]

        self.n_jobs = config["wikidata_affiliations"]["num_jobs"]

        self.verbose = config["wikidata_affiliations"]["verbose"] if "verbose" in config["wikidata_affiliations"].keys(
        ) else 0

        self.wikidata_col.create_index("id")
        self.wikidata_col.create_index(
            "claims.P31.mainsnak.datavalue.value.id")

    def process_wikidata(self):
        institutions = list(self.collection.find(
            {"external_ids.source": "wikidata"}))

        progress = ProgressLogger(
            "wikidata_affiliations", total=len(institutions)).start(
            f"num_jobs={self.n_jobs}")

        def tracked(kahi_col, wikid_col, inst, verbose):
            progress.step(process_one(kahi_col, wikid_col, inst, verbose))

        Parallel(
            n_jobs=self.n_jobs,
            backend="threading",
            verbose=0
        )(delayed(tracked)(
            self.collection, self.wikidata_col,
            inst, self.verbose) for inst in institutions)
        progress.done()

    def run(self):
        self.process_wikidata()
        return 0
