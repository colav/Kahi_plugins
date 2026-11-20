"""Match Kahi patents with Lens patent records.

Pure functions (no database access) so the rules can be tested and audited
in isolation. Rules, applied in order and stopping at the first that yields
a candidate:

1. ``number``: a registration number of the patent equals the publication,
   application or priority number of a Lens record (``family`` when it only
   equals a member of the record's simple family).
2. ``title_exact``: a normalized title equals a Lens title in any language.
3. ``title_fuzzy``: ``token_set_ratio >= 92`` with year +-3.
4. ``inventor_title``: an author shares two name tokens with a Lens inventor,
   ``token_set_ratio >= 85`` and year +-3.

Fuzzy rules also require both titles to be usable (not generic, at least
four words) and to share at least half of their content words: without it
``token_set_ratio`` scores 95 for "sistema para monitoreo ... de medidores"
against "sistema de infraestructura de medicion avanzada ...".

When several Lens records qualify, the one in the patent's jurisdiction is
preferred, then the Colombian one, then one with an abstract and the closest
year. Title rules give up when the qualifying records belong to more than one
simple family (ambiguous).
"""

import re
from collections import defaultdict

from rapidfuzz import fuzz, process
from unidecode import unidecode

FUZZY_THRESHOLD = 92
INVENTOR_TITLE_THRESHOLD = 85
MIN_CONTENT_OVERLAP = 0.5
MIN_CONTENT_WORDS = 3
YEAR_WINDOW = 3
MIN_TITLE_CHARS = 15
MIN_TITLE_TOKENS = 3
MIN_FUZZY_TOKENS = 4

COUNTRY_CODES = {
    "colombia": "CO", "estados unidos": "US", "brasil": "BR", "espana": "ES",
    "mexico": "MX", "francia": "FR", "suiza": "CH", "chile": "CL", "china": "CN",
    "alemania": "DE", "japon": "JP", "peru": "PE", "cuba": "CU", "argentina": "AR",
    "canada": "CA", "corea del sur": "KR", "costa rica": "CR", "reino unido": "GB",
    "inglaterra": "GB", "australia": "AU", "paises bajos": "NL", "rusia": "RU",
    "ecuador": "EC", "panama": "PA", "venezuela": "VE", "guatemala": "GT",
    "polonia": "PL", "singapur": "SG", "india": "IN", "italia": "IT",
    "sudafrica": "ZA", "honduras": "HN", "suecia": "SE", "belgica": "BE",
    "noruega": "NO", "dinamarca": "DK", "filipinas": "PH", "vietnam": "VN",
    "republica checa": "CZ", "bulgaria": "BG", "serbia": "RS", "nigeria": "NG",
    "puerto rico": "US", "emiratos arabes unidos": "AE",
}

GENERIC_TITLES = {
    "patente", "patente de invencion", "patente de modelo de utilidad",
    "modelo de utilidad", "certificado de obtentor", "patente en el exterior",
    "otra patente", "sin titulo", "no aplica",
}


def normalize_text(value):
    """Lowercase ASCII text with single spaces and no punctuation."""
    text = unidecode(str(value or "")).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def usable_title(normalized, min_tokens=MIN_TITLE_TOKENS):
    """Reject generic, junk or too short titles."""
    if len(normalized) < MIN_TITLE_CHARS or normalized in GENERIC_TITLES:
        return False
    tokens = normalized.split()
    if len(tokens) < min_tokens:
        return False
    # "aaaaaa aaaaa aaa": almost no distinct letters
    return len(set(normalized.replace(" ", ""))) >= 6


STOPWORDS = set(
    """a al con de del el en la las los para por que se su sus un una uno y o e an and as
    at by for from in into of on or the to with using its it metodo metodos method methods
    sistema sistemas system systems dispositivo dispositivos device devices equipo equipment
    aparato apparatus proceso procesos process processes composicion composiciones
    composition compositions procedimiento procedure producto product uso usos use thereof
    mismo misma same dicho dicha obtencion obtaining obtener""".split()
)


def content_overlap(left, right):
    """Share of content words (5-letter stems) of the shorter title found in the other."""
    left_words = {word[:5] for word in left.split() if word not in STOPWORDS and len(word) > 2}
    right_words = {word[:5] for word in right.split() if word not in STOPWORDS and len(word) > 2}
    # Too few content words ("sistema de tratamiento de aguas") contain anything.
    if min(len(left_words), len(right_words)) < MIN_CONTENT_WORDS:
        return 0.0
    return len(left_words & right_words) / min(len(left_words), len(right_words))


def country_code(country):
    return COUNTRY_CODES.get(normalize_text(country))


def number_keys(raw, default_country=None):
    """Canonical lookup keys (``<jurisdiction><digits>``) for a patent number.

    Examples: ``NC2023/0017292`` -> ``CO2023017292``; ``PCT/IB2019/053356`` ->
    ``IB2019053356``; ``US 7,041,647`` -> ``US7041647``; ``US20250127805A1``
    -> ``US20250127805``; ``16146449`` with country US -> ``US16146449``;
    ``13-192338-00000-0000`` with country CO -> ``CO13192338``; ``9-108636``
    with country CO -> ``CO09108636``; ``US008303926B1`` -> ``US8303926``.
    """
    text = unidecode(str(raw or "")).upper().strip()
    # Colombian file numbers come padded with empty sequence groups.
    text = re.sub(r"(-0+)+$", "", text)
    keys = set()
    match = re.fullmatch(r"(\d{1,2})-(\d{5,6})", text)
    if match and default_country == "CO":
        keys.add(f"CO{int(match[1]):02d}{int(match[2]):06d}")
    compact = re.sub(r"[\s.,\-_]", "", text)
    if not compact:
        return set()
    match = re.fullmatch(r"(NC|CO)?(\d{4})/(\d{1,7})", compact) or re.fullmatch(
        r"(NC|CO)(\d{4})(\d{7})", compact
    )
    if match and 1950 <= int(match[2]) <= 2100:
        prefix = "CO" if match[1] else default_country
        if prefix:
            keys.add(f"{prefix}{match[2]}{int(match[3]):06d}")
    match = re.fullmatch(r"PCT/?([A-Z]{2})/?(\d{4})/?(\d{1,6})", compact)
    if match:
        keys.add(f"{match[1]}{match[2]}{int(match[3]):06d}")
    match = re.fullmatch(r"([A-Z]{2})/[A-Z]/(\d{4})/(\d{1,7})", compact)
    if match:
        keys.add(f"{match[1]}{match[2]}{int(match[3]):06d}")
    match = re.fullmatch(r"([A-Z]{2})(\d{4})/(\d{1,6})", compact)
    if match and match[1] not in ("NC", "CO"):
        keys.add(f"{match[1]}{match[2]}{int(match[3]):06d}")
    plain = re.sub(r"[^A-Z0-9]", "", compact)
    match = re.fullmatch(r"([A-Z]{2})(\d{5,})([A-Z]\d?)?", plain)
    if match:
        keys.add(match[1] + match[2])
        keys.add(match[1] + match[2].lstrip("0"))
        if match[1] == "BR":
            keys.add(match[1] + match[2][:-1])  # trailing check digit
    elif plain.isdigit() and len(plain) >= 5 and default_country:
        keys.add(default_country + plain)
        if len(plain) == 11 and default_country == "CO" and plain[:2] in ("19", "20"):
            keys.add(f"CO{plain[:4]}{int(plain[4:]):06d}")
    return keys


def _years(record):
    years = set()
    biblio = record.get("biblio") or {}
    dates = [
        record.get("date_published"),
        (biblio.get("application_reference") or {}).get("date"),
        ((biblio.get("priority_claims") or {}).get("earliest_claim") or {}).get("date"),
    ]
    for date in dates:
        if isinstance(date, str) and re.match(r"\d{4}", date):
            years.add(int(date[:4]))
    return years


def _name_tokens(name):
    return frozenset(token for token in normalize_text(name).split() if len(token) > 1)


def names_compatible(left, right):
    """Two name token sets share at least two tokens."""
    return len(left & right) >= 2


class LensIndex:
    """In-memory lookup structures over the Lens records."""

    def __init__(self, records):
        self.records = {}
        self.direct = defaultdict(set)
        self.family = defaultdict(set)
        self.titles = defaultdict(set)
        self.inventor_tokens = defaultdict(set)
        self.by_family = defaultdict(set)
        self.fuzzy_choices = []
        self.fuzzy_ids = []
        for record in records:
            self._add(record)

    def _add(self, record):
        lens_id = record["lens_id"]
        biblio = record.get("biblio") or {}
        jurisdiction = record.get("jurisdiction")
        family = (record.get("families") or {}).get("simple_family") or {}
        members = family.get("members") or []
        family_ids = sorted(
            {member.get("lens_id") for member in members if member.get("lens_id")} | {lens_id}
        )
        inventor_names = [
            (inventor.get("extracted_name") or {}).get("value")
            for inventor in (biblio.get("parties") or {}).get("inventors") or []
        ]
        inventors = [_name_tokens(name) for name in inventor_names]
        titles = [
            normalize_text(item.get("text"))
            for item in biblio.get("invention_title") or []
            if item.get("text")
        ]
        self.records[lens_id] = {
            "lens_id": lens_id,
            "jurisdiction": jurisdiction,
            "years": _years(record),
            "family": family_ids[0],
            "has_abstract": bool(record.get("abstract")),
            "titles": titles,
            "inventors": [tokens for tokens in inventors if tokens],
            "inventor_names": [name for name in inventor_names if name],
        }
        self.by_family[family_ids[0]].add(lens_id)
        own = {f"{jurisdiction}{record.get('doc_number')}"}
        application = biblio.get("application_reference") or {}
        if application.get("doc_number"):
            own.add(f"{application.get('jurisdiction')}{application['doc_number']}")
        own |= number_keys(biblio.get("application_number"), jurisdiction)
        for claim in (biblio.get("priority_claims") or {}).get("claims") or []:
            if claim.get("doc_number"):
                own.add(f"{claim.get('jurisdiction')}{claim['doc_number']}")
        for key in own:
            self.direct[key].add(lens_id)
        for member in members:
            document = member.get("document_id") or {}
            if document.get("doc_number"):
                key = f"{document.get('jurisdiction')}{document['doc_number']}"
                if key not in own:
                    self.family[key].add(lens_id)
        for title in set(titles):
            if usable_title(title):
                self.titles[title].add(lens_id)
            if usable_title(title, MIN_FUZZY_TOKENS):
                self.fuzzy_choices.append(title)
                self.fuzzy_ids.append(lens_id)
        for tokens in self.records[lens_id]["inventors"]:
            for token in tokens:
                self.inventor_tokens[token].add(lens_id)


def year_compatible(record, year, window=YEAR_WINDOW):
    if year is None or not record["years"]:
        return False
    return any(abs(year - other) <= window for other in record["years"])


def choose(index, candidates, patent):
    """Pick one Lens record among candidates of the same invention."""
    countries = patent.get("countries") or set()
    year = patent.get("year")

    def rank(lens_id):
        record = index.records[lens_id]
        distance = (
            min(abs(year - other) for other in record["years"])
            if year is not None and record["years"]
            else 99
        )
        return (
            record["jurisdiction"] not in countries,
            record["jurisdiction"] != "CO",
            not record["has_abstract"],
            distance,
            lens_id,
        )

    return sorted(candidates, key=rank)[0]


def _single_family(index, candidates, patent):
    families = {index.records[lens_id]["family"] for lens_id in candidates}
    if len(families) > 1 and patent.get("year") is not None:
        candidates = {
            lens_id
            for lens_id in candidates
            if year_compatible(index.records[lens_id], patent["year"])
        }
        families = {index.records[lens_id]["family"] for lens_id in candidates}
    return candidates if len(families) == 1 else set()


def match_patent(index, patent):
    """Return ``{lens_id, matched_by, score, evidence}`` or a no-match reason.

    ``patent`` holds ``titles`` (strings), ``numbers`` (raw registration
    numbers with their country code), ``countries`` (codes), ``year`` and
    ``authors`` (full names).
    """
    keys = set()
    for number, code in patent.get("numbers") or []:
        keys |= number_keys(number, code)
    for rule, lookup in (("number", index.direct), ("family", index.family)):
        found = {key: lookup[key] for key in sorted(keys) if key in lookup}
        if found:
            candidates = set().union(*found.values())
            return {
                "lens_id": choose(index, candidates, patent),
                "matched_by": rule,
                "score": 100,
                "evidence": sorted(found),
                "candidates": len(candidates),
            }

    titles = [normalize_text(title) for title in patent.get("titles") or []]
    exact = {title for title in titles if usable_title(title) and title in index.titles}
    if exact:
        candidates = set().union(*(index.titles[title] for title in exact))
        candidates = _single_family(index, candidates, patent)
        if candidates:
            return {
                "lens_id": choose(index, candidates, patent),
                "matched_by": "title_exact",
                "score": 100,
                "evidence": sorted(exact),
                "candidates": len(candidates),
            }
        return {"lens_id": None, "reason": "ambiguous_title"}

    year = patent.get("year")
    if year is not None and index.fuzzy_choices:
        best = None
        for title in titles:
            if not usable_title(title, MIN_FUZZY_TOKENS):
                continue
            for _, score, position in process.extract(
                title,
                index.fuzzy_choices,
                scorer=fuzz.token_set_ratio,
                score_cutoff=FUZZY_THRESHOLD,
                limit=20,
            ):
                lens_id = index.fuzzy_ids[position]
                other = index.fuzzy_choices[position]
                if (
                    year_compatible(index.records[lens_id], year)
                    and content_overlap(title, other) >= MIN_CONTENT_OVERLAP
                    and (best is None or score > best[0])
                ):
                    best = (score, lens_id, title, other)
        if best:
            score, lens_id, title, other = best
            candidates = index.by_family[index.records[lens_id]["family"]]
            return {
                "lens_id": choose(index, candidates, patent),
                "matched_by": "title_fuzzy",
                "score": round(score, 1),
                "evidence": [title, other],
                "candidates": len(candidates),
            }

    authors = [_name_tokens(name) for name in patent.get("authors") or []]
    authors = [tokens for tokens in authors if len(tokens) >= 2]
    pool = set()
    for tokens in authors:
        for token in tokens:
            pool |= index.inventor_tokens.get(token, set())
    best = None
    for lens_id in sorted(pool):
        record = index.records[lens_id]
        if not year_compatible(record, year) or not any(
            names_compatible(author, inventor)
            for author in authors
            for inventor in record["inventors"]
        ):
            continue
        for title in titles:
            if not usable_title(title, MIN_FUZZY_TOKENS):
                continue
            for other in record["titles"]:
                if not usable_title(other, MIN_FUZZY_TOKENS):
                    continue
                score = fuzz.token_set_ratio(title, other)
                if (
                    score >= INVENTOR_TITLE_THRESHOLD
                    and content_overlap(title, other) >= MIN_CONTENT_OVERLAP
                    and (best is None or score > best[0])
                ):
                    best = (score, lens_id, title, other)
    if best:
        score, lens_id, title, other = best
        candidates = index.by_family[index.records[lens_id]["family"]]
        return {
            "lens_id": choose(index, candidates, patent),
            "matched_by": "inventor_title",
            "score": round(score, 1),
            "evidence": [title, other],
            "candidates": len(candidates),
        }
    return {"lens_id": None, "reason": "no_match"}
