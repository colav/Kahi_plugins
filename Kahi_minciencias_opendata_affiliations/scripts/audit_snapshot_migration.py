#!/usr/bin/env python3
"""Compare both import modes in isolated databases, then optionally activate YAML."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
from threading import RLock
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from bson import json_util
from pymongo import MongoClient
import yaml

from kahi.KahiBase import KahiBase
from kahi_minciencias_opendata_affiliations.Kahi_minciencias_opendata_affiliations import (
    Kahi_minciencias_opendata_affiliations as Plugin,
    normalize_scienti_nit,
    split_institution_names,
)


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json_util.dumps(value, indent=2, ensure_ascii=False))
    temporary.replace(path)


def encoded(document):
    return json_util.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def fingerprint(collection):
    digest = hashlib.sha256()
    count = 0
    for document in collection.find().sort("_id", 1).batch_size(500):
        digest.update(encoded(document))
        digest.update(b"\n")
        count += 1
    return {"documents": count, "sha256": digest.hexdigest()}


def copy_collection(source, destinations, observe=None, projection=None):
    batch = []
    for document in source.find({}, projection).sort("_id", 1).batch_size(500):
        if observe:
            observe(document)
        batch.append(document)
        if len(batch) == 500:
            for destination in destinations:
                destination.insert_many(batch)
            batch.clear()
    if batch:
        for destination in destinations:
            destination.insert_many(batch)


def group_map(collection):
    groups = {}
    duplicate_codes = set()
    for document in collection.find({"types.type": "group"}):
        codes = {str(document["_id"])}
        codes.update(str(item.get("id")) for item in document.get("external_ids", []))
        for code in codes:
            if re.fullmatch(r"COL\d{7}", code):
                if code in groups and groups[code]["_id"] != document["_id"]:
                    duplicate_codes.add(code)
                groups[code] = document
    return groups, duplicate_codes


def code_owners(collection):
    owners = {}
    for document in collection.find({"external_ids.source": "COD_INST"}, {"external_ids": 1}):
        for item in document.get("external_ids", []):
            if item.get("source") == "COD_INST" and item.get("id"):
                owners.setdefault(str(item["id"]), set()).add(document["_id"])
    return owners


def provisional_names(plugin, provisional):
    """Only an unregistered fallback whose identifier reproduces its source label."""
    if (not provisional
            or not str(provisional.get("_id") or "").startswith("IUA_")
            or any(item.get("source") != "minciencias" for item in provisional.get("external_ids", []))):
        return []
    source_names = plugin.institution_candidate_names(provisional)
    if not source_names or any(len(split_institution_names(name)) > 1 for name in source_names):
        return []
    if not any(plugin.aval_institution_id(name) == provisional["_id"] for name in source_names):
        return []
    return [re.sub(r"\s*[-–]?\s*\(\s*no avalado\s*\)\s*$", "", name, flags=re.I)
            for name in source_names if not plugin.is_non_institution_fragment(name)]


def canonical_provisional_equivalent(plugin, provisional, canonical):
    """Explain a removed fallback only with an exact name and authoritative target."""
    if not canonical or not plugin.is_authoritative_institution(canonical):
        return False
    source_names = provisional_names(plugin, provisional)
    canonical_names = {plugin.normalize_institution_name(name)
                       for name in plugin.institution_candidate_names(canonical)
                       if len(split_institution_names(name)) == 1}
    return any(plugin.normalize_institution_name(name) in canonical_names for name in source_names)


def compact_institution_name(plugin, name):
    """Audit-only equivalence for spacing/punctuation, never a fuzzy match."""
    return plugin.normalize_institution_name(name).replace(" ", "").replace("_", "")


def catalog_provisional_replacement(plugin, provisional, canonical):
    source_names = provisional_names(plugin, provisional)
    labels = {compact_institution_name(plugin, name) for name in source_names}
    codes = plugin.affiliation_cod_inst_codes(canonical)
    catalog = getattr(plugin, "scienti_catalog", {})
    if len(labels) != 1 or len(codes) != 1:
        return None
    label, code = next(iter(labels)), next(iter(codes))
    record = catalog.get(code)
    if not label or not record:
        return None
    if not hasattr(plugin, "_compact_catalog_index"):
        index = {}
        for candidate_code, candidate in catalog.items():
            for field in ("NME_INST", "NME_INST_FILTRO"):
                name = candidate.get(field)
                if name and len(split_institution_names(name)) == 1:
                    index.setdefault(compact_institution_name(plugin, name), set()).add(candidate_code)
        plugin._compact_catalog_index = index
    if plugin._compact_catalog_index.get(label) != {code}:
        return None
    if not any(compact_institution_name(plugin, name) == label
               for name in plugin.institution_candidate_names(canonical)
               if len(split_institution_names(name)) == 1):
        return None
    if plugin.collection.count_documents({
        "_id": {"$ne": canonical["_id"]},
        "external_ids": {"$elemMatch": {"source": "COD_INST", "id": code}},
    }):
        return None
    corroborated = False
    for external in canonical.get("external_ids", []):
        source = external.get("source")
        if source not in {"nit", "ID_INSTITUCION"}:
            continue
        actual = str(external.get("id") or "")
        expected = str(record.get("TXT_NIT" if source == "nit" else "ID_INSTITUCION") or "")
        if source == "nit":
            actual, expected = normalize_scienti_nit(actual), normalize_scienti_nit(expected)
        if not actual or actual != expected:
            return None
        corroborated = True
    if not corroborated:
        return None
    return {"reason": "catalog_verified_punctuation_replacement", "replacement_id": canonical["_id"],
            "cod_inst": code, "catalog_name": record["NME_INST"]}


def verification_reader(database, run_id, resolutions_collection, catalog=None):
    """Read results using the auditor's connection, not a completed plugin's."""
    reader = Plugin.__new__(Plugin)
    reader.collection = database.affiliations
    reader.resolution_audits = database[resolutions_collection]
    reader.run_id = run_id
    reader.scienti_catalog = catalog or {}
    return reader


def explain_removed_relation(plugin, group, source_group, identifier, legacy_collection):
    """Accept only source-backed repair decisions or proven canonical replacements."""
    decisions = plugin.resolution_audits.find_one({
        "run_id": plugin.run_id, "kind": "enriched_compound_group",
        "group_id": str(group.get("_id")), "institution_id": identifier,
        "outcome": "removed", "owner_declared": False,
    })
    if decisions and decisions.get("source_declarations") == source_group.get("relations", []):
        proof = plugin.resolution_audits.find_one({
            "kind": "enriched_compound_repair", "institution_id": identifier,
            "committed": True,
        })
        institution = plugin.collection.find_one({"_id": identifier}) or {}
        if proof and proof.get("registry_external_ids") == institution.get("external_ids"):
            return {"reason": "verified_owner_not_declared_by_group", "cod_inst": proof["cod_inst"]}
    provisional = legacy_collection.find_one({"_id": identifier})
    for relation in group.get("relations", []):
        canonical = plugin.collection.find_one({"_id": relation.get("id")})
        if canonical_provisional_equivalent(plugin, provisional, canonical):
            return {"reason": "exact_name_authoritative_replacement", "replacement_id": canonical["_id"]}
        if canonical:
            proof = catalog_provisional_replacement(plugin, provisional, canonical)
            if proof:
                label = compact_institution_name(plugin, provisional_names(plugin, provisional)[0])
                declared = {compact_institution_name(plugin, name)
                            for item in source_group.get("relations", [])
                            for name in split_institution_names(item.get("name"))}
                if label in declared:
                    return proof
    return None


def activate_workflow(path, original):
    """Change only this plugin's block, refusing to overwrite concurrent edits."""
    current = path.read_text()
    if current != original:
        raise RuntimeError("workflow changed during validation; activation was not applied")
    match = re.search(
        r"^  minciencias_opendata_affiliations:\n.*?(?=^  [a-zA-Z_]|\Z)",
        original, flags=re.MULTILINE | re.DOTALL,
    )
    if not match:
        raise RuntimeError("affiliations workflow block not found")
    block = match.group()
    changed, replacements = re.subn(
        r"^(    source_mode:) legacy\s*$",
        "    source_mode: snapshot",
        block, flags=re.MULTILINE,
    )
    if replacements != 1:
        raise RuntimeError("expected exactly one legacy affiliations mode")
    changed = re.sub(
        r"^    (?:release_ref|release_name|collection_name|import_revision):[^\n]*\n?",
        "", changed, flags=re.MULTILINE,
    )
    result = original[:match.start()] + changed + original[match.end():]
    before, after = yaml.safe_load(original), yaml.safe_load(result)
    expected = dict(before["workflow"]["minciencias_opendata_affiliations"])
    expected.update(source_mode="snapshot")
    for field in ("release_ref", "release_name", "collection_name", "import_revision"):
        expected.pop(field, None)
    before["workflow"]["minciencias_opendata_affiliations"] = expected
    if before != after:
        raise RuntimeError("unexpected workflow change")
    temporary = path.with_suffix(path.suffix + ".snapshot-migration.tmp")
    temporary.write_text(result)
    temporary.chmod(path.stat().st_mode)
    temporary.replace(path)


def audit(args, stage):
    original_workflow = args.workflow.read_text()
    raw = yaml.safe_load(original_workflow)
    params = raw["workflow"]["minciencias_opendata_affiliations"]
    client = MongoClient(raw["config"]["database_url"], serverSelectionTimeoutMS=10000)
    source_client = MongoClient(params["database_url"], serverSelectionTimeoutMS=10000)
    source = source_client[params["database_name"]]
    pinned = dict(params)
    if params.get("release_ref"):
        reference = raw["config"]["release_references"][params["release_ref"]]
        current = source[reference["publication_collection"]].find_one({
            "_id": reference.get("pointer_id", "current"),
        })
        pinned["release_name"] = (current or {}).get(reference.get("pointer_field", "current_release"))
        if not pinned["release_name"]:
            raise RuntimeError("current release_name not found")
    verifier = Plugin.__new__(Plugin)
    verifier.openadata_db = source
    verifier._configure_snapshot_source(pinned)
    release_name = verifier.snapshot_release_name
    collection_name = verifier.snapshot_collection_name
    materialization_run = verifier.snapshot_materialization_run
    date = datetime.now(ZoneInfo("America/Bogota")).strftime("%Y%m%d")
    prefix = "audit_affiliations_" + date + "_" + str(os.getpid())
    databases = {role: prefix + "_" + role for role in ("input", "legacy", "snapshot")}
    if set(databases.values()) & set(client.list_database_names()):
        raise RuntimeError("isolated database names already exist")
    isolated_source = client[databases["input"]]
    legacy_target = client[databases["legacy"]]
    snapshot_target = client[databases["snapshot"]]
    stage("copying_sources", databases=databases, release=release_name)
    source_before = {
        "snapshot": fingerprint(source[collection_name]),
        "legacy": fingerprint(source.gruplac_groups_data),
    }
    copy_collection(source[collection_name], [isolated_source[collection_name]])
    copy_collection(source.gruplac_groups_data, [isolated_source.gruplac_groups_data])
    if verifier.snapshot_publication_kind == "six_entity_release":
        release = source.scienti_final_release_publications.find_one({"_id": release_name})
        isolated_source.scienti_final_release_publications.insert_one(release)
        isolated_source.scienti_final_release_audits.insert_one(
            source.scienti_final_release_audits.find_one({"_id": release["audit"]}))
    else:
        # Freeze exactly the publication already verified, not a newer pointer.
        publication = source.scienti_affiliation_publications.find_one({"_id": release_name})
        isolated_source.scienti_affiliation_publications.insert_many([
            publication, {"_id": "current", "current_run": release_name,
                          "collection": collection_name, "audit": publication["audit"]},
        ])
        isolated_source.scienti_affiliation_materialization_audits.insert_one(
            source.scienti_affiliation_materialization_audits.find_one({"_id": publication["audit"]}))
        isolated_source.scienti_affiliation_materialization_runs.insert_one(
            source.scienti_affiliation_materialization_runs.find_one({"_id": materialization_run}))
    source_copy = fingerprint(isolated_source[collection_name])
    if source_copy != source_before["snapshot"]:
        raise RuntimeError("snapshot changed during copy")
    if fingerprint(isolated_source.gruplac_groups_data) != source_before["legacy"]:
        raise RuntimeError("legacy input changed during copy")

    yuku_rebuild = None
    if args.check_yuku:
        from yuku.scienti_affiliations import (
            ScientiAffiliationMaterializer, FINGERPRINT_PROJECTIONS,
            split_institution_names as split_yuku_institution_names,
        )
        stage("rebuilding_yuku_in_isolation", databases=databases)
        source_run = source.scienti_affiliation_materialization_runs.find_one({
            "_id": materialization_run,
        }) or {}
        cfg = source_run["config"]
        copy_collection(source[cfg["recognized_collection"]],
                        [isolated_source[cfg["recognized_collection"]]],
                        projection=FINGERPRINT_PROJECTIONS["recognized"])
        copy_collection(source[cfg["gruplac_collection"]],
                        [isolated_source[cfg["gruplac_collection"]]],
                        projection=FINGERPRINT_PROJECTIONS["gruplac"])
        normalized_audit = source.scienti_gruplac_normalization_audits.find_one({
            "_id": cfg["gruplac_audit_name"],
        })
        isolated_source.scienti_gruplac_normalization_audits.insert_one(normalized_audit)
        yuku_rebuild = ScientiAffiliationMaterializer(
            isolated_source, run_name="compound_institutions_v3",
            recognized_collection=cfg["recognized_collection"],
            open_data_collection="gruplac_groups_data",
            gruplac_collection=cfg["gruplac_collection"],
            gruplac_audit_name=cfg["gruplac_audit_name"],
            target_collection="audit_rebuilt_affiliations",
            expected_groups=source_before["snapshot"]["documents"],
            publish_pointer=False,
        ).run()
        old_ids = set(isolated_source[collection_name].distinct("_id"))
        new_ids = set(isolated_source.audit_rebuilt_affiliations.distinct("_id"))
        if old_ids != new_ids:
            raise RuntimeError("rebuilt Yuku graph changed the group universe")
        schema = list(isolated_source[collection_name].find_one())
        for group in isolated_source.audit_rebuilt_affiliations.find():
            if list(group) != schema or any(
                    len(split_yuku_institution_names(item.get("name"))) != 1
                    for item in group.get("relations", [])):
                raise RuntimeError("rebuilt Yuku graph has invalid schema/order or compound institutions")
        yuku_rebuild.update(compound_relations=0, schema_order_preserved=True,
                            group_universe_unchanged=True)

    stage("copying_target", databases=databases)
    baseline_keys = {}
    key_templates = {}
    baseline_campuses = {}
    baseline_owners = {}

    def observe(document):
        keys = tuple(document)
        baseline_keys[document["_id"]] = key_templates.setdefault(keys, keys)
        if "campuses" in document:
            baseline_campuses[document["_id"]] = document["campuses"]
        for item in document.get("external_ids", []):
            if item.get("source") == "COD_INST" and item.get("id"):
                baseline_owners.setdefault(str(item["id"]), set()).add(document["_id"])

    copy_collection(client[raw["config"]["database_name"]].affiliations,
                    [legacy_target.affiliations, snapshot_target.affiliations], observe)
    for target in (legacy_target, snapshot_target):
        target.affiliations.create_index("addresses.country")
        target.affiliations.create_index("addresses.country_code")
        target.affiliations.create_index("campuses.external_ids.id")

    def configuration(mode):
        task = {**params, "database_url": raw["config"]["database_url"],
                "database_name": databases["input"], "source_mode": mode}
        task.pop("release_ref", None)
        task.pop("import_revision", None)
        if verifier.snapshot_publication_kind == "six_entity_release":
            task["release_name"] = release_name
        else:
            task.pop("release_name", None)
        return {**raw["config"], "database_name": databases[mode],
                "log_database": databases[mode], "minciencias_opendata_affiliations": task}

    stage("importing_legacy", databases=databases)
    legacy_plugin = Plugin(configuration("legacy"))
    legacy_required_ids = set()
    legacy_required_codes = {}
    observed_lock = RLock()

    def observe_resolution(institution, *, created, origin, code=None, committed=True):
        if institution:
            with observed_lock:
                legacy_required_ids.add(institution["_id"])
                if code:
                    legacy_required_codes.setdefault(code, set()).add(institution["_id"])

    legacy_plugin.record_institution_resolution = observe_resolution
    legacy_plugin.run()
    stage("importing_snapshot", databases=databases)
    snapshot_plugin = Plugin(configuration("snapshot"))
    summary = snapshot_plugin.run()
    inspection = verification_reader(
        snapshot_target, snapshot_plugin.run_id, snapshot_plugin.resolution_audits.name,
        snapshot_plugin.scienti_catalog)

    stage("verifying", databases=databases)
    legacy_groups, legacy_duplicates = group_map(legacy_target.affiliations)
    snapshot_groups, snapshot_duplicates = group_map(snapshot_target.affiliations)
    source_codes = set(isolated_source[collection_name].distinct("_id"))
    legacy_source_codes = set(isolated_source.gruplac_groups_data.distinct("cod_grupo_gr"))
    missing_groups = source_codes - set(snapshot_groups)
    lost_legacy_groups = legacy_source_codes - source_codes
    relation_differences = []
    explained_relation_replacements = []
    for code in sorted(legacy_source_codes):
        previous = legacy_groups.get(code, {})
        new = snapshot_groups.get(code, {})
        old_ids = {relation.get("id") for relation in previous.get("relations", [])}
        new_ids = {relation.get("id") for relation in new.get("relations", [])}
        missing = old_ids - new_ids
        if missing:
            source_group = isolated_source[collection_name].find_one({"_id": code}) or {}
            for identifier in list(missing):
                proof = explain_removed_relation(
                    inspection, new, source_group, identifier, legacy_target.affiliations)
                if proof:
                    missing.remove(identifier)
                    explained_relation_replacements.append({"group": code, "removed_id": identifier, **proof})
        if missing:
            relation_differences.append({"group": code, "missing_ids": sorted(missing, key=str)})

    snapshot_ids = set(snapshot_target.affiliations.distinct("_id"))
    missing_institutions = legacy_required_ids - snapshot_ids
    explained_institution_replacements = []
    for identifier in list(missing_institutions):
        proof = next((item for item in explained_relation_replacements
                      if item["removed_id"] == identifier and item.get("replacement_id") in snapshot_ids
                      and item["reason"] in {"catalog_verified_punctuation_replacement",
                                             "exact_name_authoritative_replacement"}), None)
        if proof:
            missing_institutions.remove(identifier)
            explained_institution_replacements.append(proof)
    observed_codes = {}
    audits = inspection.resolution_audits
    for document in audits.find({"run_id": snapshot_plugin.run_id, "kind": "institution"}):
        for code in document.get("catalog_codes", []):
            observed_codes.setdefault(code, set()).add(document["institution_id"])
    missing_catalog_mappings = {
        code: sorted(ids - observed_codes.get(code, set()), key=str)
        for code, ids in legacy_required_codes.items()
        if ids - observed_codes.get(code, set())
    }

    template = list(KahiBase().empty_affiliation())
    allowed_new = {"_id", *template}
    touched_ids = set(source_codes)
    touched_ids.update(audits.distinct("institution_id", {
        "run_id": snapshot_plugin.run_id, "kind": "institution"}))
    touched_ids.update(snapshot_groups[code]["_id"] for code in source_codes if code in snapshot_groups)
    violations = []
    ordered_touched_ids = sorted(touched_ids, key=str)
    for start in range(0, len(ordered_touched_ids), 500):
        batch = ordered_touched_ids[start:start + 500]
        for document in snapshot_target.affiliations.find({"_id": {"$in": batch}}):
            identifier = document["_id"]
            previous_keys = set(baseline_keys.get(identifier, ()))
            added = set(document) - allowed_new - previous_keys
            base_keys = [key for key in document if key in template]
            expected_keys = [key for key in template if key in document]
            if added or base_keys != expected_keys:
                violations.append({"id": identifier, "added_keys": sorted(added), "wrong_order": base_keys != expected_keys})
            if identifier in baseline_campuses and document.get("campuses") != baseline_campuses[identifier]:
                violations.append({"id": identifier, "changed_campuses": True})
            if "campuses" in document and identifier not in baseline_campuses:
                violations.append({"id": identifier, "new_campuses": True})

    owners = code_owners(snapshot_target.affiliations)
    new_duplicate_codes = {
        code: sorted(ids, key=str) for code, ids in owners.items()
        if len(ids) > max(1, len(baseline_owners.get(code, set())))
    }
    compound_iua_links = []
    country_only_links = []
    for code in sorted(source_codes):
        for relation in snapshot_groups.get(code, {}).get("relations", []):
            if inspection.is_non_institution_fragment(relation.get("name")):
                country_only_links.append({"group": code, "relation": relation})
            if str(relation.get("id") or "").startswith("IUA_") and len(
                    split_institution_names(relation.get("name"))) > 1:
                compound_iua_links.append({"group": code, "relation": relation})
    enriched_repair_violations = []
    for decision in audits.find({"run_id": snapshot_plugin.run_id, "kind": "enriched_compound_group"}):
        group = snapshot_target.affiliations.find_one({"_id": decision["group_id"]}) or {}
        source_group = isolated_source[collection_name].find_one({"_id": decision["group_id"]}) or {}
        links = [r for r in group.get("relations", []) if r.get("id") == decision["institution_id"]]
        if (bool(links) != decision["owner_declared"]
                or decision.get("source_declarations") != source_group.get("relations", [])
                or any(r.get("name") != decision["owner_name"] for r in links)):
            enriched_repair_violations.append(decision["_id"])
    for repair in audits.find({"run_id": snapshot_plugin.run_id, "kind": "enriched_compound_repair"}):
        institution = snapshot_target.affiliations.find_one({"_id": repair["institution_id"]}) or {}
        if institution.get("external_ids") != repair["registry_external_ids"]:
            enriched_repair_violations.append(repair["_id"])
    before_repeat = fingerprint(snapshot_target.affiliations)
    repeated = Plugin(configuration("snapshot")).run()
    idempotent = repeated == summary and before_repeat == fingerprint(snapshot_target.affiliations)
    isolated_unchanged = source_copy == fingerprint(isolated_source[collection_name])
    original_unchanged = source_before == {
        "snapshot": fingerprint(source[collection_name]),
        "legacy": fingerprint(source.gruplac_groups_data),
    }
    checks = {
        "all_snapshot_groups_present": not missing_groups,
        "legacy_groups_preserved_in_source": not lost_legacy_groups,
        "no_new_duplicate_group_codes": snapshot_duplicates <= legacy_duplicates,
        "legacy_relations_preserved": not relation_differences,
        "legacy_institutions_present": not missing_institutions,
        "legacy_catalog_mappings_preserved": not missing_catalog_mappings,
        "schema_order_and_campuses_preserved": not violations,
        "no_new_duplicate_cod_inst": not new_duplicate_codes,
        "no_compound_iua_group_links": not compound_iua_links,
        "no_country_only_group_links": not country_only_links,
        "enriched_compound_repairs_source_backed": not enriched_repair_violations,
        "idempotent": idempotent,
        "isolated_snapshot_unchanged": isolated_unchanged,
        "original_dam_unchanged": original_unchanged,
    }
    if yuku_rebuild is not None:
        checks["yuku_materialization_valid"] = yuku_rebuild["status"] == "complete"
    report = {
        "status": "passed" if all(checks.values()) else "failed",
        "finished_at": now(), "release": release_name, "source_collection": collection_name,
        "databases": databases, "baseline_documents": len(baseline_keys),
        "source_groups": len(source_codes), "legacy_source_groups": len(legacy_source_codes),
        "legacy_required_institutions": len(legacy_required_ids),
        "legacy_required_catalog_codes": len(legacy_required_codes),
        "snapshot_summary": summary, "checks": checks,
        "yuku_materialization": yuku_rebuild,
        "missing_groups": sorted(missing_groups), "lost_legacy_groups": sorted(lost_legacy_groups),
        "relation_differences": relation_differences,
        "explained_relation_replacements": explained_relation_replacements,
        "explained_institution_replacements": explained_institution_replacements,
        "missing_institutions": sorted(missing_institutions, key=str),
        "missing_catalog_mappings": missing_catalog_mappings,
        "schema_violations": violations, "new_duplicate_codes": new_duplicate_codes,
        "compound_iua_group_links": compound_iua_links,
        "country_only_group_links": country_only_links,
        "enriched_repair_violations": enriched_repair_violations,
        "workflow_activated": False,
    }
    write_json(args.output_dir / "report.json", report)
    if report["status"] != "passed":
        raise RuntimeError("isolated audit failed; see report.json; workflow unchanged")
    if args.activate_workflow:
        stage("activating_workflow", databases=databases)
        activate_workflow(args.workflow, original_workflow)
        report["workflow_activated"] = True
        write_json(args.output_dir / "report.json", report)
    client.close()
    source_client.close()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--activate-workflow", action="store_true")
    parser.add_argument("--check-yuku", action="store_true", help="also rebuild affiliations in the isolated source database")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    state = {"status": "running", "pid": os.getpid(), "started_at": now()}

    def stage(name, **extra):
        state.update(stage=name, **extra)
        write_json(args.output_dir / "status.json", state)
        print(json.dumps({"time": now(), "stage": name, **extra}), flush=True)

    try:
        report = audit(args, stage)
        state.update(status="complete", finished_at=now(), workflow_activated=report["workflow_activated"])
        print(json.dumps({"status": "complete", "checks": report["checks"],
                          "institutions": report["snapshot_summary"]["institution_resolution"]}), flush=True)
    except Exception as error:
        state.update(status="failed", finished_at=now(), error=str(error))
        traceback.print_exc()
    write_json(args.output_dir / "status.json", state)
    return 0 if state["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
