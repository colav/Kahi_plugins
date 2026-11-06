import unittest
from unittest.mock import patch

from kahi_openalex_person.Kahi_openalex_person import (
    process_one,
    process_one_insert,
)


class OpenAlexNameSplitFailureTest(unittest.TestCase):
    def test_obvious_non_person_label_is_skipped(self):
        author = {"display_name": "Español---Francés En"}

        result = process_one(author, {}, "test", {}, None)

        self.assertIsNone(result)

    def test_completed_exact_openalex_author_skips_works_lookup(self):
        openalex_id = "https://openalex.org/A123"
        person = {
            "updated": [{"source": "openalex", "time": 1}],
            "external_ids": [
                {"source": "openalex", "id": openalex_id}
            ],
        }

        class PersonCollection:
            def find(self, _query):
                return self

            def limit(self, _limit):
                return [person]

        class WorksCollection:
            def aggregate(self, _pipeline):
                raise AssertionError("works must not be queried on resume")

        author = {
            "id": openalex_id,
            "ids": {"openalex": openalex_id},
            "display_name": "Ana Pérez",
        }
        client = {"test": {"person": PersonCollection()}}

        result = process_one(
            author, client, "test", {}, WorksCollection(), True
        )

        self.assertIsNone(result)

    def test_insert_stops_instead_of_saving_empty_name_parts(self):
        author = {"display_name": "Catalina Ramirez Diaz"}
        empty_person = {"updated": []}
        client = {"test": {"person": object()}}

        with patch(
            "kahi_openalex_person.Kahi_openalex_person.split_names",
            side_effect=ConnectionError("model unavailable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Catalina Ramirez Diaz"):
                process_one_insert(
                    None, author, client, "test", empty_person, []
                )


if __name__ == "__main__":
    unittest.main()
