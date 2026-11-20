from test_snapshot_ingestion import Plugin
from test_snapshot_migration_activation import audit


def test_no_avalado_fallback_is_equivalent_only_with_exact_authoritative_name():
    plugin = Plugin.__new__(Plugin)
    name = "CORPORACION UNIVERSIDAD DE LA COSTA - (No Avalado)"
    identifier = plugin.aval_institution_id(name)
    provisional = {"_id": identifier, "names": [{"name": name}],
                   "external_ids": [{"source": "minciencias", "id": identifier}]}
    canonical = {"_id": "ror-id", "names": [{"name": "Universidad de la Costa"},
                                            {"name": "CORPORACION UNIVERSIDAD DE LA COSTA"}],
                 "external_ids": [{"source": "ror", "id": "ror-id"}]}
    assert audit.canonical_provisional_equivalent(plugin, provisional, canonical)
    provisional["_id"] = "IUA_unrelated"
    assert not audit.canonical_provisional_equivalent(plugin, provisional, canonical)
    provisional["_id"] = identifier
    canonical["names"] = [{"name": "Universidad de otra Costa"}]
    assert not audit.canonical_provisional_equivalent(plugin, provisional, canonical)
    canonical["names"] = [{"name": "CORPORACION UNIVERSIDAD DE LA COSTA"}]
    canonical["external_ids"] = []
    assert not audit.canonical_provisional_equivalent(plugin, provisional, canonical)


def test_registered_or_compound_identity_cannot_be_explained_by_name_alone():
    plugin = Plugin.__new__(Plugin)
    canonical = {"_id": "ror-id", "names": [{"name": "Institución A"}],
                 "external_ids": [{"source": "ror", "id": "ror-id"}]}
    provisional = {"_id": "IUA_old", "names": [{"name": "Institución A"}],
                   "external_ids": [{"source": "COD_INST", "id": "other-code"}]}
    assert not audit.canonical_provisional_equivalent(plugin, provisional, canonical)
    provisional["external_ids"] = []
    provisional["names"] = [{"name": "Institución A; Institución B"}]
    assert not audit.canonical_provisional_equivalent(plugin, provisional, canonical)
