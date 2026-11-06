import pytest

from kahi_impactu_utils.Utils import compare_author
from kahi_unicity_person_graph.Kahi_unicity_person_graph import (
    CandidateEdgeBuilder,
    ComponentResolver,
)
from kahi_unicity_person_graph.graph import (
    CandidateGroup,
    build_evidence_components,
)


DOI = "https://doi.org/10.1093/ofid/ofad553"
ORCID_1 = "https://orcid.org/0000-0003-2895-3084"
ORCID_2 = "https://orcid.org/0000-0003-4072-6140"


def profile(pid, name="Diana Andrea Arias Fernandez", source="staff",
            ids=None, affiliations=None, aliases=None):
    tokens = name.split()
    return {
        "_id": pid, "full_name": name,
        "first_names": tokens[:-2], "last_names": tokens[-2:],
        "initials": "".join(token[0] for token in tokens[:-2]),
        "updated": [{"source": source, "time": 1}],
        "external_ids": ids or [], "affiliations": affiliations or [],
        "aliases": aliases or [],
    }


def discover(left, right, count=50, valid_affiliations=None):
    return CandidateEdgeBuilder(
        compare_author, max_authors_threshold=20,
        single_doi_exact_name_max_authors=20,
    ).add_groups(
        [CandidateGroup("doi", DOI, (left["_id"], right["_id"]), 0, count)],
        {left["_id"]: left, right["_id"]: right},
        valid_affiliation_ids=(
            {"institution"} if valid_affiliations is None
            else valid_affiliations
        ),
    ).finish()


@pytest.mark.parametrize("count", [21, 50, 2000])
def test_long_exact_name_with_valid_shared_affiliation_exceeds_limit(count):
    affiliation = [{"id": "institution"}]
    left = profile("a", name="Diana Andrea Arias Fernández",
                   affiliations=affiliation)
    right = profile("b", source="openalex", affiliations=affiliation)
    discovery = discover(left, right, count)

    assert discovery.edges[0].confidence == "high"
    detail = discovery.edges[0].match_details[0]
    assert detail["exact_name_substantive_token_count"] == 4
    assert "author_count_limit_exception" in detail["confidence_reasons"]
    component = build_evidence_components(discovery.edges)[0]
    result = ComponentResolver(compare_author, timestamp=10).resolve(
        component, {"a": left, "b": right}
    )
    assert len(result.plans) == 1
    assert result.plans[0].target_id == "a"
    assert result.plans[0].absorbed_ids == ("b",)


@pytest.mark.parametrize("name", [
    "Juan Perez", "Juan de la Cruz", "Juan de Cruz",
    "Juan A Perez", "Juan A. Perez", "Juan J.A. Perez",
])
def test_particles_and_initials_do_not_supply_three_complete_words(name):
    affiliation = [{"id": "institution"}]
    discovery = discover(
        profile("a", name=name, affiliations=affiliation),
        profile("b", name=name, source="openalex", affiliations=affiliation),
    )
    assert discovery.edges[0].confidence == "medium"


def test_three_complete_words_suffice():
    affiliation = [{"id": "institution"}]
    discovery = discover(
        profile("a", name="Juan Perez Garcia", affiliations=affiliation),
        profile("b", name="Juan Perez Garcia", source="openalex",
                affiliations=affiliation),
    )
    assert discovery.edges[0].confidence == "high"


@pytest.mark.parametrize("count", [None, 20])
def test_missing_count_stays_medium_and_small_work_keeps_original_rule(count):
    affiliation = [{"id": "institution"}]
    discovery = discover(
        profile("a", affiliations=affiliation),
        profile("b", source="openalex", affiliations=affiliation), count,
    )
    assert discovery.edges[0].confidence == ("medium" if count is None else "high")
    assert "author_count_limit_exception" not in discovery.edges[0].match_details[0]["confidence_reasons"]


def test_long_exact_name_without_corroboration_stays_medium():
    discovery = discover(profile("a"), profile("b", source="openalex"))
    assert discovery.edges[0].confidence == "medium"


def test_dangling_shared_affiliation_does_not_enable_exception():
    affiliation = [{"id": "missing-institution"}]
    discovery = discover(
        profile("a", affiliations=affiliation),
        profile("b", source="openalex", affiliations=affiliation),
        valid_affiliations=set(),
    )
    assert discovery.edges[0].confidence == "medium"
    assert discovery.rejected_invalid_affiliation_references == 2


@pytest.mark.parametrize("left_ids,right_ids", [
    ([{"source": "orcid", "id": ORCID_1}],
     [{"source": "orcid", "id": ORCID_1.removeprefix("https://orcid.org/")}]),
    ([{"source": "Cédula de Ciudadanía", "id": "75103968"}],
     [{"source": "Cédula de Ciudadanía", "id": "75.103.968"}]),
    ([{"source": "scienti", "id": {"COD_RH": "0001448821"}}],
     [{"source": "scienti", "id": {"COD_RH": "0001448821"}}]),
])
def test_valid_shared_personal_identity_supports_exception_without_affiliation(
        left_ids, right_ids):
    discovery = discover(
        profile("a", ids=left_ids),
        profile("b", source="openalex", ids=right_ids),
    )
    assert discovery.edges[0].confidence == "high"
    assert "shared_authoritative_identity_support" in discovery.edges[0].match_details[0]["confidence_reasons"]


def test_same_invalid_orcid_does_not_support_exception():
    ids = [{"source": "orcid", "id": "0000-0003-2895-3085"}]
    discovery = discover(profile("a", ids=ids),
                         profile("b", source="openalex", ids=ids))
    assert discovery.edges[0].confidence == "medium"


@pytest.mark.parametrize("left_ids,right_ids", [
    ([{"source": "orcid", "id": ORCID_1}],
     [{"source": "orcid", "id": ORCID_2}]),
    ([{"source": "Cédula de Ciudadanía", "id": "75103968"}],
     [{"source": "Cédula de Ciudadanía", "id": "75103969"}]),
    ([{"source": "scienti", "id": {"COD_RH": "0001448821"}}],
     [{"source": "scienti", "id": {"COD_RH": "0001448822"}}]),
])
def test_conflicting_identity_blocks_exact_name_despite_shared_affiliation(
        left_ids, right_ids):
    affiliation = [{"id": "institution"}]
    discovery = discover(
        profile("a", ids=left_ids, affiliations=affiliation),
        profile("b", source="openalex", ids=right_ids, affiliations=affiliation),
    )
    assert discovery.edges == ()
    assert discovery.rejected_identity_conflicts == 1


def test_same_long_alias_does_not_replace_exact_main_name():
    affiliation = [{"id": "institution"}]
    alias = ["Christian Andres Rojas Ceron"]
    discovery = discover(
        profile("a", name="Christian Andres Rojas Ceron",
                affiliations=affiliation, aliases=alias),
        profile("b", name="Christian Rojas Ceron", source="openalex",
                affiliations=affiliation, aliases=alias),
    )
    assert discovery.edges[0].confidence == "medium"


def test_long_alias_does_not_make_short_exact_main_name_eligible():
    affiliation = [{"id": "institution"}]
    discovery = discover(
        profile("a", name="Maria Torres", affiliations=affiliation,
                aliases=["Maria Elena Torres"]),
        profile("b", name="Maria Torres", source="openalex",
                affiliations=affiliation, aliases=["Maria Elena Torres"]),
    )
    assert discovery.edges[0].confidence == "medium"


def test_two_openalex_profiles_without_trusted_source_remain_rejected():
    affiliation = [{"id": "institution"}]
    discovery = discover(
        profile("a", source="openalex", affiliations=affiliation),
        profile("b", source="openalex", affiliations=affiliation),
    )
    assert discovery.edges == ()
    assert discovery.rejected_untrusted_doi_pairs == 1


def test_exception_does_not_bridge_conflicting_orcids_transitively():
    affiliation = [{"id": "institution"}]
    snapshot = {
        "a": profile("a", ids=[{"source": "orcid", "id": ORCID_1}],
                     affiliations=affiliation),
        "b": profile("b", source="openalex", affiliations=affiliation),
        "c": profile("c", source="scholar",
                     ids=[{"source": "orcid", "id": ORCID_2}],
                     affiliations=affiliation),
    }
    discovery = CandidateEdgeBuilder(
        compare_author, max_authors_threshold=20,
        single_doi_exact_name_max_authors=20,
    ).add_groups([
        CandidateGroup("doi", DOI, ("a", "b"), 0, 50),
        CandidateGroup("doi", "https://doi.org/10.1000/other", ("b", "c"), 1, 50),
    ], snapshot, valid_affiliation_ids={"institution"}).finish()
    assert len(discovery.edges) == 2
    assert all(edge.confidence == "high" for edge in discovery.edges)
    component = build_evidence_components(discovery.edges)[0]
    result = ComponentResolver(compare_author, timestamp=10).resolve(component, snapshot)
    assert len(result.plans) == 1
    assert len(result.plans[0].absorbed_ids) == 1
    assert result.rejected_orcid_conflicts == 1
