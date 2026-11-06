import pytest

from kahi_openalex_works.parser import canonical_orcid_url


@pytest.mark.parametrize(
    "value",
    [
        "0000-0002-1825-0097",
        "https://orcid.org/0000-0002-1825-0097",
        "http://orcid.org/0000-0002-1825-0097",
        "https: //orcid.org/0000-0002-1825-0097",
    ],
)
def test_canonical_orcid_url(value):
    assert canonical_orcid_url(value) == (
        "https://orcid.org/0000-0002-1825-0097"
    )
