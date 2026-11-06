"""Temporal author affiliations are selected identically on insert and update."""
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import MagicMock

import mongomock
import pytest

from kahi.KahiBase import KahiBase
from kahi_ciarp_works import process_one as module
from kahi_ciarp_works.parser import parse_ciarp


def affiliation(kind, identifier, years, parent='institution'):
    return {'id': identifier, 'name': identifier, 'years': years,
            'types': [{'source': 'staff', 'type': kind}],
            'relations': [{'id': parent}]}


@pytest.fixture
def environment():
    db = mongomock.MongoClient().db
    institution = {'_id': 'institution', 'names': [{'name': 'University', 'lang': 'es'}],
                   'types': [{'type': 'education'}]}
    db.affiliations.insert_one(deepcopy(institution))
    for kind, identifier, parent in [
            ('faculty', 'current-faculty', 'institution'),
            ('faculty', 'old-faculty', 'institution'),
            ('department', 'department', 'institution'),
            ('faculty', 'foreign-faculty', 'other-institution')]:
        db.affiliations.insert_one({'_id': identifier,
            'names': [{'name': identifier, 'lang': 'es'}],
            'types': [{'type': kind}], 'relations': [{'id': parent}]})
    person = {'_id': 'person', 'full_name': 'Ana Perez',
        'external_ids': [{'source': 'Cédula de Ciudadanía', 'id': '123'}],
        'affiliations': [affiliation('education', 'institution', [2022, 2023, 2024])],
    }
    return db, institution, person


def record(year=2024):
    return {'título': 'Example work', 'idioma': 'es', 'doi': '', 'issn': '',
        'isbn': '', 'revista': '', 'año': year, 'volumen': '', 'issue': '',
        'primera_página': '', 'última_página': '', 'ranking': '', 'identificación': '123',
        'index': 'row-1', 'source_record_id': 'source-row-1',
        'código_unidad_académica': 'wrong-faculty', 'código_subunidad_académica': 'wrong-department'}


def selected_ids(environment, year=2024):
    db, institution, person = environment
    before = deepcopy(person)
    result = module.select_work_affiliations(person, institution, year, db, current_year=2026)
    assert person == before
    return [aff['id'] for aff in result]


def test_current_units_preferred_over_previous_year(environment):
    person = environment[2]
    person['affiliations'] += [affiliation('faculty', 'old-faculty', [2023]),
        affiliation('faculty', 'current-faculty', [2024]),
        affiliation('department', 'department', [2024])]
    assert selected_ids(environment) == ['institution', 'current-faculty', 'department']


def test_previous_year_units_are_eligible(environment):
    environment[2]['affiliations'] += [affiliation('faculty', 'old-faculty', [2023]),
        affiliation('department', 'department', [2023])]
    assert selected_ids(environment) == ['institution', 'old-faculty', 'department']


def test_parent_only_if_no_eligible_units(environment):
    environment[2]['affiliations'].append(affiliation('faculty', 'old-faculty', [2022]))
    assert selected_ids(environment) == ['institution']


def test_unknown_unit_dates_and_foreign_units_are_not_inferred(environment):
    environment[2]['affiliations'] += [
        {'id': 'current-faculty', 'types': [{'type': 'faculty'}]},
        affiliation('faculty', 'foreign-faculty', [2024], parent='other-institution')]
    assert selected_ids(environment) == ['institution']


def test_staff_year_gaps_are_not_covered_by_envelope(environment):
    parent = environment[2]['affiliations'][0]
    parent.update(years=[2020, 2025], start_date=1577836800, end_date=-1)
    assert selected_ids(environment, 2024) == []
    assert not module.publication_year_in_parent_affiliation(
        environment[2], 'institution', 2024, current_year=2026)


@pytest.mark.parametrize('year,expected', [(2025, ['institution']), (2026, []), (None, []), ('', [])])
def test_parent_tolerance_and_unknown_year(environment, year, expected):
    assert selected_ids(environment, year) == expected


def test_legacy_unit_intervals_and_multiple_concurrent_units(environment):
    environment[2]['affiliations'] += [
        {'id': 'old-faculty', 'types': [{'type': 'faculty'}],
         'start_date': int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
         'end_date': int(datetime(2024, 12, 31, tzinfo=timezone.utc).timestamp())},
        affiliation('faculty', 'current-faculty', [2024])]
    assert selected_ids(environment) == ['institution', 'old-faculty', 'current-faculty']


@pytest.mark.parametrize('mode', ['insert', 'update'])
@pytest.mark.parametrize('with_units', [True, False])
def test_real_write_paths_use_staff_units_not_ciarp_codes(environment, mode, with_units, monkeypatch):
    db, institution, person = environment
    if with_units:
        person['affiliations'] += [affiliation('faculty', 'current-faculty', [2024]),
            affiliation('department', 'department', [2023])]
    db.person.insert_one(deepcopy(person))
    expected = ['institution', 'current-faculty', 'department'] if with_units else ['institution']
    if mode == 'insert':
        # Mongomock lacks $type in the shared insert-only aggregation pipeline.
        # Substitute only that persistence adapter; exercise the real selector
        # and author insertion code. The adapter has its own utility tests.
        monkeypatch.setattr(module, 'insert_only_update', lambda entry: {'$setOnInsert': entry})
        module.process_one_insert(record(), db, db.works, institution,
                                  KahiBase().empty_work(), None)
        result = db.works.find_one()['authors'][0]
        assert [aff['id'] for aff in result['affiliations']] == expected
    else:
        work = KahiBase().empty_work()
        work['authors'] = [{'id': 'person', 'full_name': 'Ana Perez',
                           'affiliations': [{'id': 'other-source'}]}]
        work['_id'] = db.works.insert_one(work).inserted_id
        module.process_one_update(record(), work, db, db.works, institution, KahiBase().empty_work())
        result = db.works.find_one()['authors'][0]
        assert [aff['id'] for aff in result['affiliations']] == ['other-source'] + expected
        module.process_one_update(record(), db.works.find_one(), db, db.works,
                                  institution, KahiBase().empty_work())
        assert db.works.find_one()['authors'][0] == result
    assert result['id'] == 'person'
    assert db.person.find_one() == person


def test_update_selects_units_using_preserved_publication_year(environment):
    db, institution, person = environment
    person['affiliations'] += [affiliation('faculty', 'old-faculty', [2022]),
                             affiliation('faculty', 'current-faculty', [2024])]
    db.person.insert_one(person)
    work = KahiBase().empty_work()
    work.update(year_published=2023, authors=[{'id': 'person', 'full_name': 'Ana Perez', 'affiliations': []}])
    work['_id'] = db.works.insert_one(work).inserted_id
    module.process_one_update(record(2024), work, db, db.works, institution, KahiBase().empty_work())
    result = db.works.find_one()
    assert result['year_published'] == 2023
    assert [aff['id'] for aff in result['authors'][0]['affiliations']] == ['institution', 'old-faculty']


def test_blank_name_cannot_match_unresolved_person():
    people = MagicMock()
    author = {'full_name': '', 'external_ids': []}
    assert module.resolve_ciarp_author({'person': people}, author) is None
    people.find_one.assert_not_called()


def test_parser_does_not_infer_units_from_product_codes(environment):
    document = parse_ciarp(record(), environment[1], KahiBase().empty_work())
    assert [aff['id'] for aff in document['authors'][0]['affiliations']] == ['institution']


@pytest.mark.parametrize('kind,identifier', [('faculty', 'current-faculty'), ('department', 'department')])
def test_unit_and_subunit_are_independently_optional(environment, kind, identifier):
    environment[2]['affiliations'].append(affiliation(kind, identifier, [2024]))
    assert selected_ids(environment) == ['institution', identifier]
