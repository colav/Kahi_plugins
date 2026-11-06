"""Real PyMongo close semantics: mongomock.close alone does not catch this bug."""
import mongomock
import pytest
from pymongo import MongoClient
from pymongo.errors import InvalidOperation

from test_snapshot_migration_activation import audit


def test_verification_reads_evidence_after_importer_closes_its_client():
    plugin = audit.Plugin.__new__(audit.Plugin)
    importer_client = MongoClient(connect=False)
    plugin.collection = importer_client.target.affiliations
    plugin.resolution_audits = importer_client.target.custom_resolutions
    plugin.run_id = "completed_import"
    importer_client.close()
    with pytest.raises(InvalidOperation, match="after close"):
        plugin.resolution_audits.find_one({})

    # A separate auditor-owned connection remains open after the import.
    with mongomock.MongoClient() as auditor:
        db = auditor.target
        external_ids = [{"source": "COD_INST", "id": "000000010865"}]
        db.affiliations.insert_one({"_id": "IUA_old", "external_ids": external_ids})
        db.custom_resolutions.insert_many([
            {"_id": "decision", "run_id": plugin.run_id, "kind": "enriched_compound_group",
             "group_id": "COL0094314", "institution_id": "IUA_old", "outcome": "removed",
             "owner_declared": False, "source_declarations": [{"name": "Hospital Alma Máter"}]},
            {"_id": "proof", "kind": "enriched_compound_repair", "institution_id": "IUA_old",
             "committed": True, "registry_external_ids": external_ids, "cod_inst": "000000010865"},
        ])
        reader = audit.verification_reader(db, plugin.run_id, plugin.resolution_audits.name)
        proof = audit.explain_removed_relation(
            reader, {"_id": "COL0094314", "relations": []},
            {"relations": [{"name": "Hospital Alma Máter"}]}, "IUA_old", db.legacy)
        assert proof == {"reason": "verified_owner_not_declared_by_group", "cod_inst": "000000010865"}
        assert reader.collection.database.client is auditor


def test_verification_connection_fix_does_not_accept_changed_source_evidence():
    with mongomock.MongoClient() as auditor:
        db = auditor.target
        db.custom_resolutions.insert_one({
            "_id": "decision", "run_id": "completed_import", "kind": "enriched_compound_group",
            "group_id": "COL0094314", "institution_id": "IUA_old", "outcome": "removed",
            "owner_declared": False, "source_declarations": [{"name": "Other source"}],
        })
        reader = audit.verification_reader(db, "completed_import", "custom_resolutions")
        assert audit.explain_removed_relation(
            reader, {"_id": "COL0094314", "relations": []},
            {"relations": [{"name": "Hospital Alma Máter"}]}, "IUA_old", db.legacy) is None
