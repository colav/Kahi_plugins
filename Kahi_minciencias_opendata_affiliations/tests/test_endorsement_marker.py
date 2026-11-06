from test_snapshot_ingestion import Plugin, config, plugin_module, prepare_databases


strip = plugin_module.strip_endorsement_marker
split = plugin_module.split_endorsement_declarations
non_endorsing = plugin_module.is_non_endorsing_declaration


def _exact_candidates_only(monkeypatch):
    """mongomock has no $text/textScore; keep the exact-name candidate search."""
    original = Plugin.get_institution_candidates

    def exact_only(self, name):
        try:
            return original(self, name)
        except (TypeError, NotImplementedError):
            return []
    monkeypatch.setattr(Plugin, "get_institution_candidates", exact_only)


def test_strips_gruplac_endorsement_marker_variants():
    assert strip("UNIVERSIDAD SURCOLOMBIANA - (No Avalado)") == "UNIVERSIDAD SURCOLOMBIANA"
    assert strip("UNIVERSIDAD SURCOLOMBIANA (no avalado) ") == "UNIVERSIDAD SURCOLOMBIANA"
    assert strip("UNIVERSIDAD DE LA AMAZONIA - (Avalado)") == "UNIVERSIDAD DE LA AMAZONIA"
    assert strip("UNIVERSIDAD DE LA AMAZONIA") == "UNIVERSIDAD DE LA AMAZONIA"


def test_detects_only_non_endorsing_declarations():
    assert non_endorsing("UNIVERSIDAD SURCOLOMBIANA - (No Avalado)")
    assert non_endorsing("UNIVERSIDAD SURCOLOMBIANA (NO AVALADO)")
    assert not non_endorsing("UNIVERSIDAD DE LA AMAZONIA - (Avalado)")
    assert not non_endorsing("UNIVERSIDAD DE LA AMAZONIA")
    assert not non_endorsing("Sede Cúcuta (UNIVERSIDAD SANTO TOMAS)")


def test_keeps_campus_and_other_parentheticals():
    campus = "Sede Cúcuta (UNIVERSIDAD SANTO TOMAS)"
    assert strip(campus) == campus
    assert strip("Avalados SAS") == "Avalados SAS"


def test_excludes_non_endorsing_declarations_and_parts_without_mutating_source():
    reg = {"_id": "COL0000001", "relations": [
        {"id": "", "name": "UNIVERSIDAD DE LA AMAZONIA", "types": []},
        {"id": "", "name": "UNIVERSIDAD SURCOLOMBIANA - (No Avalado)", "types": []},
        {"id": "", "name": "A - (No Avalado); B - (Avalado)", "types": []},
        {"id": "X1", "name": "C - (Avalado)", "types": []},
    ]}
    cleaned, excluded = split(reg)
    assert [item["name"] for item in cleaned["relations"]] == [
        "UNIVERSIDAD DE LA AMAZONIA", "B", "C"]
    assert cleaned["relations"][2]["id"] == "X1"
    assert [item["name"] for item in excluded] == [
        "UNIVERSIDAD SURCOLOMBIANA - (No Avalado)", "A - (No Avalado)"]
    assert reg["relations"][1]["name"] == "UNIVERSIDAD SURCOLOMBIANA - (No Avalado)"


def test_non_endorsing_institution_is_not_related_or_created(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    _exact_candidates_only(monkeypatch)
    source.affiliations_final.update_one(
        {"_id": "COL0000001"},
        {"$set": {"relations": [
            {"id": "", "name": "CEMENTOS ARGOS SA - (Avalado)", "types": []},
            {"id": "", "name": "UNIVERSIDAD SURCOLOMBIANA - (No Avalado)", "types": []}]}},
    )

    summary = Plugin(config()).run()

    group = target.affiliations.find_one({"_id": "COL0000001"})
    assert [relation["id"] for relation in group["relations"]] == ["canonical-argos"]
    assert target.affiliations.count_documents({"_id": {"$regex": "^IUA"}}) == 0
    assert target.affiliations.count_documents({"names.name": {"$regex": "(?i)avalad"}}) == 0
    resolution = summary["institution_resolution"]
    assert resolution["relations_excluded_non_endorsing"] == 1
    assert resolution["relations_unresolved"] == 0


def test_group_with_only_non_endorsing_declarations_has_no_institution(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    source.affiliations_final.update_one(
        {"_id": "COL0000001"},
        {"$set": {"relations": [
            {"id": "", "name": "CEMENTOS ARGOS SA - (No Avalado)", "types": []}]}},
    )

    Plugin(config()).run()

    group = target.affiliations.find_one({"_id": "COL0000001"})
    assert group["relations"] == []
    assert target.affiliations.count_documents({"_id": {"$regex": "^IUA"}}) == 0


def _prior_scienti_link(target):
    institution = dict(target.affiliations.find_one({"_id": "canonical-argos"}))
    institution.update({
        "_id": "canonical-upb",
        "names": [{"source": "ror", "lang": "es", "name": "UNIVERSIDAD PONTIFICIA BOLIVARIANA"}],
        "types": [{"source": "ror", "type": "education"}],
        "external_ids": [{"source": "ror", "id": "canonical-upb"}],
    })
    target.affiliations.insert_one(institution)
    group = Plugin.empty_affiliation(None)
    group.update({
        "_id": "COL0000001",
        "updated": [{"source": "scienti", "time": 1}],
        "names": [{"source": "scienti", "lang": "es", "name": "Grupo Uno"}],
        "types": [{"source": "scienti", "type": "group"}],
        "external_ids": [{"source": "scienti", "id": "COL0000001"}],
        "relations": [{"id": "canonical-upb", "name": "UNIVERSIDAD PONTIFICIA BOLIVARIANA",
                       "types": institution["types"]}],
    })
    target.affiliations.insert_one(group)


def test_prior_link_to_non_endorsing_institution_is_removed(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    _prior_scienti_link(target)
    source.affiliations_final.update_one(
        {"_id": "COL0000001"},
        {"$set": {"relations": [
            {"id": "", "name": "CEMENTOS ARGOS SA", "types": []},
            {"id": "", "name": "UNIVERSIDAD PONTIFICIA BOLIVARIANA - (No Avalado)", "types": []}]}},
    )

    summary = Plugin(config()).run()

    group = target.affiliations.find_one({"_id": "COL0000001"})
    assert [relation["id"] for relation in group["relations"]] == ["canonical-argos"]
    resolution = summary["institution_resolution"]
    assert resolution["relations_excluded_non_endorsing"] == 1
    assert resolution["non_endorsing_prior_links_removed"] == 1
    assert target.affiliations.count_documents({"_id": "canonical-upb"}) == 1


def test_prior_link_is_kept_when_the_institution_also_endorses(monkeypatch):
    target, source = prepare_databases(monkeypatch)
    _prior_scienti_link(target)
    source.affiliations_final.update_one(
        {"_id": "COL0000001"},
        {"$set": {"relations": [
            {"id": "", "name": "UNIVERSIDAD PONTIFICIA BOLIVARIANA", "types": []},
            {"id": "", "name": "UNIVERSIDAD PONTIFICIA BOLIVARIANA - (No Avalado)", "types": []}]}},
    )

    summary = Plugin(config()).run()

    group = target.affiliations.find_one({"_id": "COL0000001"})
    assert [relation["id"] for relation in group["relations"]] == ["canonical-upb"]
    assert summary["institution_resolution"]["non_endorsing_prior_links_removed"] == 0
