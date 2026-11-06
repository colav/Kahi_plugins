import pytest

from kahi_snies_affiliations.Kahi_snies_affiliations import snies_external_urls


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("www.udea.edu.co", "https://www.udea.edu.co"),
        ("http://litoral.edu.co/", "http://litoral.edu.co"),
        ("https: //FUNCATC.org/", "https://FUNCATC.org"),
        ("https://https: //FUNCATC.org/", "https://FUNCATC.org"),
    ],
)
def test_snies_external_urls_normalizes_valid_sites(value, expected):
    assert snies_external_urls({"PÁGINA_WEB": value}) == [
        {"provenance": "snies", "source": "site", "url": expected}
    ]


@pytest.mark.parametrize("value", ["", "No disponible", "1", "unir"])
def test_snies_external_urls_rejects_placeholders(value):
    assert snies_external_urls({"PÁGINA_WEB": value}) == []
