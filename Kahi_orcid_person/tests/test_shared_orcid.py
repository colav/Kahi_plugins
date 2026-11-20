import importlib
from threading import Lock

import mongomock

module = importlib.import_module('kahi_orcid_person.Kahi_orcid_person')
ORCID = 'https://orcid.org/0000-0001-7969-7231'


def record(given, family):
    return {'record:record': {
        'common:orcid-identifier': {'common:uri': ORCID},
        'person:person': {'person:name': {'personal-details:given-names': given,
                                          'personal-details:family-name': family}}}}


def person(_id, name, extra_ids=()):
    return {'_id': _id, 'full_name': name, 'first_names': name.split()[:2], 'last_names': name.split()[2:],
            'initials': '', 'aliases': [], 'updated': [{'source': 'scienti', 'time': 1}],
            'external_ids': [{'provenance': 'scienti', 'source': 'scienti', 'id': {'COD_RH': _id}},
                             {'provenance': 'scienti', 'source': 'orcid', 'id': ORCID}] + list(extra_ids)}


def plugin():
    p = object.__new__(module.Kahi_orcid_person)
    p.verbose = 0
    p.shared_orcids_removed = 0
    p._stats_lock = Lock()
    return p


def run(docs, given='Dalia Xiomara', family='Suárez Pulido'):
    col = mongomock.MongoClient().db.person
    col.insert_many(docs)
    outcome = plugin().process_one(record(given, family), col, {'updated': [], 'aliases': []}, [])
    return col, outcome


def test_name_compatibility():
    assert module.orcid_name_compatible('Dalia Xiomara Suarez Pulido', 'Dalia Xiomara Suárez Pulido')
    assert module.orcid_name_compatible('Carlos Andres Gonzalez Restrepo', 'Carlos Andres González')
    assert module.orcid_name_compatible('Miguel Urra Canales', 'Miguel Urra')
    assert not module.orcid_name_compatible('miguel angel avila diaz', 'Dalia Xiomara Suárez Pulido')
    assert module.orcid_name_compatible('J Perez', 'Juan Perez')
    assert not module.orcid_name_compatible('Ana Perez', 'Juan Perez')


def test_orcid_is_removed_from_the_incompatible_person():
    wrong = person('1', 'miguel angel avila diaz', [{'provenance': 'openalex', 'source': 'observed_orcids', 'id': ORCID}])
    wrong['aliases'] = ['Dalia Xiomara Suárez Pulido', 'avila diaz miguel']
    wrong['updated'].append({'source': 'orcid', 'time': 2})
    col, outcome = run([wrong, person('2', 'Dalia Xiomara Suarez Pulido')])
    assert outcome == 'actualizados'
    w = col.find_one({'_id': '1'})
    assert all(e['id'] != ORCID for e in w['external_ids'])
    assert 'Dalia Xiomara Suárez Pulido' not in w['aliases'] and 'avila diaz miguel' in w['aliases']
    assert [u['source'] for u in w['updated']] == ['scienti']
    owner = col.find_one({'_id': '2'})
    assert {'provenance': 'orcid', 'source': 'orcid', 'id': ORCID} in owner['external_ids']
    assert any(u['source'] == 'orcid' for u in owner['updated'])
    audit = col.database['orcid_person_shared_orcids'].find_one({'orcid': ORCID})
    assert audit['decision'] == 'reassigned' and audit['removed'] == ['1'] and audit['kept'] == ['2']


def test_both_compatible_keeps_both():
    col, _ = run([person('1', 'Dalia Suarez Pulido'), person('2', 'Dalia Xiomara Suarez')])
    assert col.count_documents({'external_ids.id': ORCID}) == 2
    assert col.database['orcid_person_shared_orcids'].find_one()['decision'] == 'ambiguous'


def test_no_compatible_keeps_both():
    col, _ = run([person('1', 'Pedro Gomez Lopez'), person('2', 'Ana Ruiz Diaz')])
    assert col.count_documents({'external_ids.id': ORCID}) == 2
    assert col.database['orcid_person_shared_orcids'].find_one()['decision'] == 'no_match'


def test_single_person_is_updated_and_marked():
    col, outcome = run([person('1', 'Dalia Xiomara Suarez Pulido')])
    assert outcome == 'actualizados'
    doc = col.find_one()
    assert {'provenance': 'orcid', 'source': 'orcid', 'id': ORCID} in doc['external_ids']
    assert col.database['orcid_person_shared_orcids'].count_documents({}) == 0
