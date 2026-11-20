"""Check that work importers share publication-year bounds without database I/O."""
import importlib
from unittest.mock import MagicMock
from datetime import datetime

import pytest

from kahi.KahiBase import KahiBase
from kahi_impactu_utils import Utils


@pytest.fixture(autouse=True)
def current_year_2026(monkeypatch):
    class Clock(datetime):
        @staticmethod
        def now():
            return datetime(2026, 10, 2)
    monkeypatch.setattr(Utils, 'dt', Clock)


@pytest.mark.parametrize('value, expected', [(1799, None), (1800, 1800),
                                             (2028, 2028), (2029, None), (2030, None), (None, None)])
@pytest.mark.parametrize('source', ['openalex', 'scienti', 'ciarp', 'scholar', 'dspace', 'wos', 'scopus'])
def test_importer_year(source, value, expected, monkeypatch):
    module_name = (f'kahi_{source}_works.Kahi_{source}_works'
                   if source in ('wos', 'scopus') else f'kahi_{source}_works.parser')
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, 'lang_poll', lambda *args, **kwargs: 'es')
    records = {
        'openalex': {'title': 'Example', 'publication_year': value, 'doi': None,
                     'ids': {}, 'type': 'article', 'counts_by_year': [], 'primary_location': None,
                     'biblio': {}, 'cited_by_count': 0, 'authorships': [], 'concepts': []},
        'scienti': {'TXT_NME_PROD': 'Example', 'TXT_RESUMEN_PROD': None,
                    'COD_RH': '1', 'COD_PRODUCTO': '2', 'NRO_ANO_PRESENTA': value,
                    'NRO_MES_PRESENTA': None, 'product_type': [{'TXT_NME_TIPO_PRODUCTO': 'Article',
                                                                'NRO_NIVEL': 0, 'TPO_CLASE': 'A', 'COD_TIPO_PRODUCTO': '111'}],
                    'author': [{'TXT_TOTAL_NAMES': 'Ana Perez', 'COD_RH': '1', 'TPO_DOCUMENTO_IDENT': None}]},
        'ciarp': {'título': 'Example', 'idioma': 'es', 'doi': None, 'issn': None,
                  'isbn': None, 'revista': '', 'año': value, 'volumen': None,
                  'issue': None, 'primera_página': None, 'última_página': None,
                  'ranking': None, 'identificación': '1', 'código_unidad_académica': None,
                  'código_subunidad_académica': None, 'index': '1'},
        'scholar': {'title': 'Example', 'year': value},
        'dspace': {'_id': 'oai:example.edu:123/456', 'OAI-PMH': {'GetRecord': {'record': {'metadata': {
            'dim:dim': {'dim:field': [{'@element': 'date', '@qualifier': 'issued',
                                       '#text': f'{value}-01-01'}]}}}}}},
        'wos': {'PY': value},
        'scopus': {'Title': 'Example', 'Year': value, 'ISBN': None},
    }
    if source == 'ciarp':
        document = module.parse_ciarp(records[source],
                                      {'_id': 'institution', 'names': [{'name': 'Example', 'lang': 'es'}], 'types': []},
                                      KahiBase().empty_work())
    elif source == 'dspace':
        document = module.parse_dspace(records[source], KahiBase().empty_work(), 'https://example.edu')
    else:
        document = getattr(module, f'parse_{source}')(records[source], KahiBase().empty_work())
    assert document['year_published'] == expected


@pytest.mark.parametrize('source', ['openalex', 'scienti', 'ciarp', 'scholar'])
@pytest.mark.parametrize('stored,incoming,expected', [
    (2024, 2030, 2024), (None, 2030, None), (1799, 2028, 2028),
    (2030, 1799, None), ('2024', 2028, 2024)])
def test_merge_keeps_valid_year_and_never_copies_invalid(source, stored, incoming, expected):
    module = importlib.import_module(f'kahi_{source}_works.process_one')
    existing = {'year_published': stored}
    module.merge_missing_core_metadata(existing, {'year_published': incoming}, MagicMock())
    assert existing['year_published'] == expected


@pytest.mark.parametrize('stored,incoming,expected', [
    (None, 2030, None), (1799, 2028, 2028), (2024, 2030, 2024)])
def test_minciencias_snapshot_year(stored, incoming, expected):
    module = importlib.import_module('kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works')
    plugin = object.__new__(module.Kahi_minciencias_opendata_works)
    old = None if stored is None else {'year_published': stored}
    new = {'year_published': incoming}
    result = plugin._merge(old, new)
    assert result['year_published'] == expected
    assert new['year_published'] == incoming


def test_dspace_ignores_bad_year_when_multiple_dates(monkeypatch):
    module = importlib.import_module('kahi_dspace_works.parser')
    reg = {'_id': 'oai:example.edu:123/456', 'OAI-PMH': {'GetRecord': {'record': {'metadata': {
        'dim:dim': {'dim:field': [{'@element': 'date', '@qualifier': 'issued',
                                   '#text': f'{year}-01-01'} for year in [1799, 2029, 2024, 2022]]}}}}}}
    assert module.parse_dspace(reg, KahiBase().empty_work(), 'https://example.edu')['year_published'] == 2022


@pytest.mark.parametrize('year,expected', [(1799, None), (1800, 1800), (2028, 2028), (2029, None)])
def test_minciencias_related_work_year(year, expected, monkeypatch):
    module = importlib.import_module('kahi_minciencias_opendata_works.related_works')
    monkeypatch.setattr(module, 'lang_poll', lambda *args, **kwargs: 'es')
    candidate = {'title': 'Example', 'year': year, 'authors': [], 'keywords': [],
                 'areas': [], 'dois': [], 'identifiers': [], 'isbn': [], 'issn': [],
                 'fingerprint': 'abc', 'oriented_people': []}
    document = module.candidate_to_work(candidate, KahiBase().empty_work())
    assert document['year_published'] == expected


@pytest.mark.parametrize('source', ['wos', 'scopus'])
@pytest.mark.parametrize('stored,incoming,expected', [
    (2024, 2030, 2024), (None, 2030, None), (1799, 2028, 2028),
    (2030, 1799, None), ('2024', 2028, 2024)])
def test_wos_scopus_update_year(source, stored, incoming, expected, monkeypatch):
    module = importlib.import_module(f'kahi_{source}_works.Kahi_{source}_works')
    existing = KahiBase().empty_work()
    existing.update(_id='work-1', year_published=stored)
    entry = KahiBase().empty_work()
    entry.update(year_published=incoming, types=[{'type': 'article'}])
    monkeypatch.setattr(module, f'parse_{source}', lambda *args, **kwargs: entry)
    monkeypatch.setattr(module, 'doi_processor', lambda value: value)
    collection = MagicMock()
    collection.find_one.return_value = existing
    doi_field = 'DI' if source == 'wos' else 'DOI'
    module.process_one({doi_field: 'https://doi.org/10.1000/example'},
                       MagicMock(), collection, KahiBase().empty_work())
    fields = collection.update_one.call_args.args[1]['$set']
    assert fields['year_published'] == expected
