from kahi_scholar_person.parser import display_tokens, parse_author_candidate


def test_display_tokens_split_every_dash():
    assert display_tokens("Alonso‐Herrero") == ["Alonso", "Herrero"]
    assert display_tokens("AL–LEHIBI") == ["AL", "Lehibi"]  # short uppercase tokens are kept
    assert display_tokens("Saenz-ludlow") == ["Saenz", "Ludlow"]


def test_parse_author_candidate_has_no_dashes():
    parsed = parse_author_candidate("Alonso‐Herrero, A")
    assert parsed["full_name"] == "A Alonso Herrero"
    assert parsed["last_names"] == ["Alonso", "Herrero"]
