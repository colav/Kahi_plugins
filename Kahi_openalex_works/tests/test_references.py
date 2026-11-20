import pytest
from kahi.KahiBase import KahiBase
from kahi_openalex_works.parser import (
    cited_by_api_url, openalex_references, openalex_work_key, parse_openalex)
from kahi_openalex_works.process_one import merge_openalex_citation_links


WORK_ID = "https://openalex.org/W1000367409"
REFS = ["https://openalex.org/W164800462", "https://openalex.org/W1492048727"]


def minimal_reg(**extra):
    reg = {
        "id": WORK_ID, "title": "Avulsión dental", "ids": {"openalex": WORK_ID},
        "doi": "https://doi.org/10.22519/21455333.211", "publication_year": 2012,
        "type": "article", "counts_by_year": [], "primary_location": None,
        "cited_by_count": 1, "biblio": {}, "authorships": [], "concepts": [],
        "referenced_works": REFS, "referenced_works_count": 2,
    }
    reg.update(extra)
    return reg


def empty_work():
    return KahiBase.empty_work(None)


def test_openalex_work_key_and_url():
    assert openalex_work_key(WORK_ID) == "W1000367409"
    assert openalex_work_key("W12") == "W12"
    assert cited_by_api_url(WORK_ID) == (
        "https://api.openalex.org/works?filter=cites:W1000367409")
    assert cited_by_api_url(None) == ""


def test_openalex_references_shape():
    assert openalex_references({"referenced_works": REFS + [None]}) == [
        {"provenance": "openalex", "source": "openalex", "id": REFS[0]},
        {"provenance": "openalex", "source": "openalex", "id": REFS[1]},
    ]
    assert openalex_references({}) == []


def test_parse_sets_references_and_cited_by_url():
    entry = parse_openalex(minimal_reg(), empty_work())
    assert [r["id"] for r in entry["references"]] == REFS
    assert entry["references_count"] == 2
    assert entry["cited_by_api_url"].endswith("cites:W1000367409")


def test_parse_without_references():
    entry = parse_openalex(
        minimal_reg(referenced_works=[], referenced_works_count=0), empty_work())
    assert entry["references"] == []
    assert entry["references_count"] == 0
    assert entry["cited_by_api_url"].endswith("cites:W1000367409")


def test_parse_count_falls_back_to_list_length():
    entry = parse_openalex(minimal_reg(referenced_works_count=None), empty_work())
    assert entry["references_count"] == 2


def test_merge_sets_fields_on_update():
    entry = parse_openalex(minimal_reg(), empty_work())
    colav_reg = empty_work()
    changed = merge_openalex_citation_links(colav_reg, entry)
    assert set(changed) == {"references", "references_count", "cited_by_api_url"}
    assert colav_reg["references_count"] == 2
    assert colav_reg["cited_by_api_url"] == entry["cited_by_api_url"]


def test_merge_only_missing_keeps_existing():
    entry = parse_openalex(minimal_reg(), empty_work())
    colav_reg = empty_work()
    colav_reg["references"] = [{"provenance": "openalex", "source": "openalex", "id": "W1"}]
    colav_reg["cited_by_api_url"] = "kept"
    assert merge_openalex_citation_links(colav_reg, entry, only_missing=True) == {}
    assert colav_reg["cited_by_api_url"] == "kept"
    colav_reg["cited_by_api_url"] = ""
    changed = merge_openalex_citation_links(colav_reg, entry, only_missing=True)
    assert set(changed) == {"cited_by_api_url"}


@pytest.mark.parametrize("oa_url, label", [
    ("http://hdl.handle.net/10495/6956", "handle"),
    ("https://doi.org/10.14201/8946", "doi"),
    ("https://www.scielo.org.co/pdf/x.pdf", "scielo"),
    ("https://revistas.udea.edu.co/index.php/vitae/article/view/1", "open_access"),
])
def test_open_access_url_is_labelled_by_site_kind(oa_url, label):
    reg = minimal_reg(open_access={"is_oa": True, "oa_status": "green", "oa_url": oa_url})
    entry = parse_openalex(reg, empty_work())
    assert entry["external_urls"] == [
        {"provenance": "openalex", "source": label, "url": oa_url}]


def test_repeated_authorships_of_the_same_author_are_joined():
    def authorship(author_id, name, institution, orcid=None):
        return {"author": {"id": author_id, "display_name": name, "orcid": orcid},
                "institutions": [{"id": f"https://openalex.org/I{institution}",
                                  "display_name": institution}],
                "author_position": "middle", "raw_affiliation_strings": []}
    reg = minimal_reg(authorships=[
        authorship("https://openalex.org/A1", "S Bansal", "Universiteit Antwerpen"),
        authorship("https://openalex.org/A2", "J P Gomez", "Universidad de Los Andes"),
        authorship("https://openalex.org/A1", "S Bansal", "Panjab University", "0000-0001-0000-0001"),
        authorship(None, "Anónimo", "DESY"),
        authorship(None, "Anónimo", "DESY"),
    ])

    entry = parse_openalex(reg, empty_work())

    assert [a["full_name"] for a in entry["authors"]] == ["S Bansal", "J P Gomez", "Anónimo", "Anónimo"]
    bansal = entry["authors"][0]
    assert [aff["name"] for aff in bansal["affiliations"]] == ["Universiteit Antwerpen", "Panjab University"]
    assert bansal["external_ids"] == [
        {"source": "openalex", "id": "https://openalex.org/A1"},
        {"source": "orcid", "id": "https://orcid.org/0000-0001-0000-0001"}]
