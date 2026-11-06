#!/usr/bin/env python3
"""Repair Kahi work ids, rebuild Elasticsearch, resume the configured ETL."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml
from elasticsearch import Elasticsearch, VERSION as ES_VERSION
from pymongo import MongoClient

from kahi_elasticsearch_works.Kahi_elasticsearch_works import Kahi_elasticsearch_works
from migrate_sha256_work_ids import migrate


def es_client(settings):
    auth = (settings["es_user"], settings["es_password"])
    parameter = {"basic_auth": auth} if ES_VERSION[0] >= 8 else {"http_auth": auth}
    return Elasticsearch(settings["es_url"], **parameter)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    workflow_path = Path(args.workflow).resolve()
    workflow = yaml.safe_load(workflow_path.read_text())
    root = workflow["config"]
    active = workflow["workflow"]
    settings = active.get("openalex_works")
    if not settings:
        raise RuntimeError("openalex_works must be active to obtain Elasticsearch settings")

    client = MongoClient(root["database_url"])
    works = client[root["database_name"]]["works"]
    before = works.count_documents({})
    migration = migrate(works, max(1, args.batch_size))
    after = works.count_documents({})
    if before != after:
        raise RuntimeError(f"work count changed during migration: {before} != {after}")

    plugin_config = {
        "database_url": root["database_url"],
        "database_name": root["database_name"],
        "elasticsearch_works": {
            "es_index": settings["es_index"],
            "es_url": settings["es_url"],
            "es_user": settings["es_user"],
            "es_password": settings["es_password"],
            "bulk_size": 500,
            "verbose": 1,
            "progress_interval": 100000,
            "task": "delete",
        },
    }
    Kahi_elasticsearch_works(plugin_config).run()
    plugin_config["elasticsearch_works"]["task"] = "bulk_insert"
    Kahi_elasticsearch_works(plugin_config).run()

    command = [
        "kahi_run", "--workflow", str(workflow_path),
        "--verbose", "1", "--log", "no",
    ]
    etl_status = subprocess.run(command, check=False).returncode

    string_ids = works.count_documents({"_id": {"$type": "string"}})
    object_ids = works.count_documents({"_id": {"$type": "objectId"}})
    total = works.count_documents({})
    indexable = works.count_documents({"titles.0": {"$exists": True}})
    elastic_count = es_client(settings).count(index=settings["es_index"])["count"]
    report = {
        "migration": migration,
        "etl_exit_status": etl_status,
        "works_total": total,
        "works_object_ids": object_ids,
        "works_string_ids": string_ids,
        "mongo_indexable_works": indexable,
        "elasticsearch_works": elastic_count,
        "healthy": (
            etl_status == 0
            and string_ids == 0
            and object_ids == total
            and elastic_count == indexable
        ),
    }
    Path(args.report).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print("INFO: final health report: {}".format(json.dumps(report, sort_keys=True)))
    if not report["healthy"]:
        raise SystemExit(1)


if __name__ == "__main__":
    sys.exit(main())
