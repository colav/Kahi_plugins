import mongomock

from kahi_wikidata_affiliations.Kahi_wikidata_affiliations import (
    commons_file_url,
    process_one,
)


def test_commons_file_url_is_valid_and_encodes_filename():
    assert commons_file_url("Logo institución.svg") == (
        "https://commons.wikimedia.org/w/index.php?title="
        "Special:Redirect/file/Logo_instituci%C3%B3n.svg&width=300"
    )


def test_process_one_repairs_existing_logo_without_duplicates():
    client = mongomock.MongoClient()
    affiliations = client.kahi.affiliations
    wikidata = client.wikidata.data
    affiliations.insert_one(
        {
            "_id": "udea",
            "names": [
                {
                    "name": "Universidad de Antioquia",
                    "lang": "es",
                    "source": "wikidata",
                    "provenance": "wikidata",
                }
            ],
            "external_ids": [{"source": "wikidata", "id": "Q1258412"}],
            "external_urls": [
                {"source": "site", "url": "https://www.udea.edu.co"},
                {
                    "provenance": "wikidata",
                    "source": "logo",
                    "url": "https: //commons.wikimedia.org/w/index.php?title="
                    "Special: Redirect/file/Escudo-UdeA.svg&width=300",
                },
            ],
            "updated": [{"source": "wikidata", "time": 1}],
        }
    )
    wikidata.insert_one(
        {
            "id": "Q1258412",
            "labels": {"es": {"value": "Universidad de Antioquia"}},
            "claims": {
                "P154": [
                    {"mainsnak": {"datavalue": {"value": "Escudo-UdeA.svg"}}}
                ]
            },
        }
    )

    process_one(affiliations, wikidata, affiliations.find_one({"_id": "udea"}), 0)
    result = affiliations.find_one({"_id": "udea"})

    logos = [item for item in result["external_urls"] if item.get("source") == "logo"]
    assert logos == [
        {
            "provenance": "wikidata",
            "source": "logo",
            "url": "https://commons.wikimedia.org/w/index.php?title="
            "Special:Redirect/file/Escudo-UdeA.svg&width=300",
        }
    ]
    assert len([name for name in result["names"] if name["source"] == "wikidata"]) == 1
    assert len([item for item in result["updated"] if item["source"] == "wikidata"]) == 1
    assert {"source": "site", "url": "https://www.udea.edu.co"} in result["external_urls"]
