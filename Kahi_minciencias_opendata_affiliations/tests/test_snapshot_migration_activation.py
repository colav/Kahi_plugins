import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location(
    "snapshot_migration_audit", Path(__file__).parents[1] / "scripts/audit_snapshot_migration.py"
)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


WORKFLOW = """config:
  database_url: localhost
workflow:
  minciencias_opendata_affiliations:
    scienti_databases:
    - database_name: scienti_udea_2024
    source_mode: legacy
    release_ref: minciencias_open_data_current
  minciencias_opendata_person:
    source_mode: legacy
"""


def test_activation_changes_only_affiliations_and_keeps_other_consumers(tmp_path):
    path = tmp_path / "workflow.yml"
    path.write_text(WORKFLOW)
    audit.activate_workflow(path, WORKFLOW)
    expected = WORKFLOW.replace("    source_mode: legacy", "    source_mode: snapshot", 1)
    expected = expected.replace("    release_ref: minciencias_open_data_current\n", "")
    assert path.read_text() == expected


def test_activation_refuses_to_overwrite_workflow_edited_during_audit(tmp_path):
    path = tmp_path / "workflow.yml"
    changed = WORKFLOW + "# change made during the audit\n"
    path.write_text(changed)
    with pytest.raises(RuntimeError, match="changed during validation"):
        audit.activate_workflow(path, WORKFLOW)
    assert path.read_text() == changed
