from copy import deepcopy
import pytest

from test_works_snapshot_ingestion import Plugin, config, WORK_1
from test_snapshot_phases import identified, nodoi
from test_snapshot_similarity import setup, TITLE

BOOK = [{'source': 'impactu', 'type': 'Libro', 'level': 0}]
CHAPTER = [{'source': 'impactu', 'type': 'Capítulo de libro', 'level': 0}]


@pytest.mark.parametrize('reverse', [False, True])
@pytest.mark.parametrize('route', ['isbn', 'scienti_pair', 'product', 'exact_title', 'es_title'])
def test_book_and_chapter_never_merge_across_available_routes(monkeypatch, reverse, route):
    target, source, engine, options = setup(monkeypatch, title=TITLE if route == 'exact_title' else 'Effects of regional clinical interventions on patienx outcomes')
    source_type, owner_type = (BOOK, CHAPTER) if reverse else (CHAPTER, BOOK)
    source.filtered.update_one({}, {'$set': {'types': source_type}})
    target.works.update_one({}, {'$set': {'types': owner_type}})
    if route in ['isbn', 'scienti_pair', 'product']:
        if route == 'isbn':
            ids = [{'source': 'isbn', 'id': '9789582011734'}]
        elif route == 'scienti_pair':
            ids = [{'source': 'scienti', 'id': {'COD_RH': '0000000001', 'COD_PRODUCTO': '12'}}]
        else:
            ids = [{'source': 'minciencias', 'id': 'TEST-PRODUCT-12'}]
        source.filtered.update_one({}, {'$set': {'external_ids': ids}})
        target.works.update_one({}, {'$set': {'external_ids': ids}})
    before = deepcopy(target.works.find_one({'_id': WORK_1}))
    result = Plugin(config(**options)).run()
    assert result['inserted'] == 1 and result['updated'] == 0
    assert target.works.count_documents({}) == 2
    assert target.works.find_one({'_id': WORK_1}) == before


@pytest.mark.parametrize('kind', [BOOK, CHAPTER])
@pytest.mark.parametrize('route', ['isbn', 'es_title'])
def test_same_kind_still_enriches_the_existing_work(monkeypatch, kind, route):
    target, source, engine, options = setup(monkeypatch)
    source.filtered.update_one({}, {'$set': {'types': kind}})
    target.works.update_one({}, {'$set': {'types': kind}})
    if route == 'isbn':
        target.works.update_one({}, {'$set': {'external_ids': [{'source': 'isbn', 'id': '9789582011734'}]}})
    result = Plugin(config(**options)).run()
    assert result['updated'] == 1 and result['inserted'] == 0
    assert target.works.count_documents({}) == 1
    assert target.works.find_one()['doi'] == 'https://doi.org/10.1000/retained'


@pytest.mark.parametrize('conflict', [[], BOOK + CHAPTER])
@pytest.mark.parametrize('known', [BOOK, CHAPTER])
def test_missing_or_contradictory_type_does_not_merge_with_known_book_family(monkeypatch, conflict, known):
    target, source, engine, options = setup(monkeypatch)
    source.filtered.update_one({}, {'$set': {'types': known}})
    target.works.update_one({}, {'$set': {'types': conflict, 'external_ids': [{'source': 'isbn', 'id': '9789582011734'}]}})
    assert Plugin(config(**options)).run()['inserted'] == 1
    assert target.works.count_documents({}) == 2


def test_contradictory_types_on_both_sides_are_not_merged(monkeypatch):
    target, source, engine, options = setup(monkeypatch)
    source.filtered.update_one({}, {'$set': {'types': BOOK + CHAPTER}})
    target.works.update_one({}, {'$set': {'types': BOOK + CHAPTER}})
    assert Plugin(config(**options)).run()['inserted'] == 1


def test_source_id_collision_does_not_overwrite_book_with_chapter(monkeypatch):
    row = nodoi(WORK_1)
    row['types'] = CHAPTER
    target, _ = identified(monkeypatch, [row])
    owner = deepcopy(row)
    owner['types'] = BOOK
    owner['authors'][0]['id'] = 'person-canonical'
    target.works.insert_one(owner)
    result = Plugin(config(collection_name='filtered', task='nodoi')).run()
    assert result['identity_conflicts'] == 1
    assert target.works.count_documents({}) == 1
    assert target.works.find_one() == owner


@pytest.mark.parametrize('label,expected', [
    ('book', 'book'), ('Libro', 'book'), ('BOOK', 'book'),
    ('Libro resultado de investigacion por capitulos', 'book'),
    ('Libro de texto por capitulos', 'book'),
    ('book-chapter', 'chapter'), ('book_section', 'chapter'),
    ('Capítulo de libro', 'chapter'),
    ('Capitulo de libro resultado de investigacion', 'chapter'),
    ('Articulo corto publicado en libro de texto', 'chapter'),
])
def test_known_native_and_canonical_labels(label, expected):
    from kahi_minciencias_opendata_works.work_types import book_families
    assert book_families({'types': [{'source': 'example', 'type': label}]}) == {expected}


def test_unknown_types_are_not_guessed_from_isbn_or_title():
    from kahi_minciencias_opendata_works.work_types import book_families, book_types_compatible
    row = {'types': [], 'titles': [{'title': 'Un capítulo de libro'}],
           'external_ids': [{'source': 'isbn', 'id': '9789582011734'}]}
    assert book_families(row) == set()
    assert book_types_compatible(row, row) is True


def test_new_book_and_chapter_have_only_base_model_fields(monkeypatch):
    from kahi.KahiBase import KahiBase
    target, source, engine, options = setup(monkeypatch)
    source.filtered.update_one({}, {'$set': {'types': CHAPTER}})
    target.works.update_one({}, {'$set': {'types': BOOK}})
    Plugin(config(**options)).run()
    rows = [row for row in target.works.find() if row['_id'] != WORK_1]
    assert len(rows) == 1
    assert list(rows[0]) == ['_id', *KahiBase().empty_work()]


SCIENTI_CHAPTER = [
    {'provenance': 'scienti', 'source': 'scienti', 'type': 'Otro capítulo de libro publicado', 'level': 2, 'code': '135'},
    {'provenance': 'scienti', 'source': 'scienti', 'type': 'Libro', 'level': 1, 'code': '13'},
    {'provenance': 'scienti', 'source': 'scienti', 'type': 'Producción bibliográfica', 'level': 0, 'code': '1'},
]
MINCIENCIAS_CHAPTER = [
    {'provenance': 'minciencias', 'source': 'minciencias', 'type': 'Capítulo de libro', 'level': 1, 'parent': 'Producción bibliográfica'},
    {'provenance': 'minciencias', 'source': 'minciencias', 'type': 'Producción bibliográfica', 'level': 0, 'parent': None},
]


def test_parent_category_does_not_turn_a_scienti_chapter_into_a_book():
    from kahi_minciencias_opendata_works.work_types import book_families, book_types_compatible
    # "Intoxicación por metilxantinas" (COD_RH 0000061158 / 94): chapter in both sources.
    assert book_families({'types': SCIENTI_CHAPTER}) == frozenset({'chapter'})
    assert book_types_compatible({'types': MINCIENCIAS_CHAPTER}, {'types': SCIENTI_CHAPTER})
    assert not book_types_compatible({'types': BOOK}, {'types': SCIENTI_CHAPTER})
