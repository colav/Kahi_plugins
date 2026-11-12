from copy import deepcopy
from hashlib import sha256

from bson import BSON, ObjectId
from kahi.KahiBase import KahiBase
import pytest

from test_works_snapshot_ingestion import Plugin, config, prepare, work_snapshot, WORK_1, WORK_2
from kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works import snapshot_doi


def identified(monkeypatch, works=None):
    target, source = prepare(monkeypatch, works=works)
    source.filtered.insert_many(list(source.works_final.find()))
    release = source.scienti_final_release_publications.find_one({'_id': 'release_six'})
    audit = source.scienti_final_release_audits.find_one({'_id': 'release_six_audit'})
    count = source.filtered.count_documents({})
    selected_config = {
        'source': {'release_name': 'release_six', 'collections': release['collections'],
                   'counts': {'works': count},
                   'release_digest': sha256(BSON.encode(release)).hexdigest(),
                   'audit_digest': sha256(BSON.encode(audit)).hexdigest()},
        'target': 'filtered', 'count_only': False,
        'policy_version': 'scienti-identified-works-v1', 'policy': 'validated-policy',
    }
    source.scienti_identified_work_publications.insert_one({
        '_id': 'filtered', 'release_name': 'release_six', 'run_name': 'selection',
        'audit': 'selection', 'documents': count,
    })
    source.scienti_identified_work_audits.insert_one({
        '_id': 'selection', 'status': 'passed', 'critical_anomalies': 0,
        'config': selected_config, 'documents': count, 'documents_digest': 'a'*64,
    })
    source.scienti_identified_work_runs.insert_one({
        '_id': 'selection', 'status': 'complete', 'config': selected_config,
        'stats': {'selected': count}, 'documents_digest': 'a'*64,
    })
    return target, source


def nodoi(identifier=WORK_2, title='Libro con identificador y título específico', year=2024):
    row = work_snapshot(identifier)
    row['doi'] = ''
    row['titles'] = [{'title': title, 'source': 'minciencias'}]
    row['year_published'] = year
    row['external_ids'] = [{'source': 'isbn', 'id': '9789582011734'}]
    return row


def test_doi_and_nodoi_partition_have_independent_checkpoints(monkeypatch, capsys):
    target, source = identified(monkeypatch, [work_snapshot(), nodoi()])
    first = Plugin(config(collection_name='filtered', task='doi')).run()
    output = capsys.readouterr().out
    assert '[minciencias_opendata_works:doi] START total=2' in output
    assert '[minciencias_opendata_works:doi] DONE 2 reg en' in output
    assert 'nuevos=1 omitidos=1' in output
    assert first['processed'] == 2 and first['selected'] == 1
    assert first['skipped_other_phase'] == 1
    assert target.works.count_documents({}) == 1
    assert Plugin(config(collection_name='filtered', task='doi')).run() == first
    second = Plugin(config(collection_name='filtered', task='nodoi')).run()
    assert second['selected'] == 1 and second['skipped_other_phase'] == 1
    assert target.works.count_documents({}) == 2
    assert target.minciencias_opendata_works_runs.count_documents({'status':'complete'}) == 2


def test_sparse_filtered_documents_receive_only_model_defaults(monkeypatch):
    target, source = identified(monkeypatch)
    source.filtered.update_one({}, {'$unset': {field: '' for field in [
        'abstracts','open_access','apc','references','citations','date_published']}})
    Plugin(config(collection_name='filtered', task='doi')).run()
    row=target.works.find_one()
    assert list(row) == ['_id', *KahiBase().empty_work()]
    assert row['abstracts'] == [] and row['apc'] == {'paid': {}}
    assert row['open_access'] == {} and row['date_published'] is None
    assert not any(key.startswith('_doi') for key in row)


@pytest.mark.parametrize('change', ['missing_publication','failed_audit','wrong_release',
                                    'changed_digest','wrong_target','changed_count'])
def test_identified_snapshot_requires_matching_publication_and_audit(monkeypatch,change):
    target, source = identified(monkeypatch)
    if change == 'missing_publication': source.scienti_identified_work_publications.delete_many({})
    if change == 'failed_audit': source.scienti_identified_work_audits.update_one({}, {'$set':{'status':'failed'}})
    if change == 'wrong_release': source.scienti_identified_work_publications.update_one({}, {'$set':{'release_name':'other'}})
    if change == 'changed_digest': source.scienti_identified_work_runs.update_one({}, {'$set':{'documents_digest':'bad'}})
    if change == 'wrong_target': source.scienti_identified_work_audits.update_one({}, {'$set':{'config.target':'other'}})
    if change == 'changed_count': source.filtered.insert_one({'_id':WORK_2})
    with pytest.raises(RuntimeError,match='identified works snapshot'):
        Plugin(config(collection_name='filtered',task='doi'))
    assert target.works.count_documents({}) == 0


def test_doi_can_come_only_from_external_ids(monkeypatch):
    row=work_snapshot();row['doi']='invalid'
    row['external_ids'].append({'source':'doi','id':'10.1000/EXAMPLE'})
    target,_=identified(monkeypatch,[row])
    assert Plugin(config(collection_name='filtered',task='doi')).run()['selected']==1
    assert target.works.find_one()['doi']=='https://doi.org/10.1000/example'
    assert Plugin(config(collection_name='filtered',task='nodoi')).run()['selected']==0


def test_invalid_doi_belongs_to_nodoi_and_is_not_stored_as_primary(monkeypatch):
    row=nodoi();row['doi']='not-a-doi'
    target,_=identified(monkeypatch,[row])
    assert Plugin(config(collection_name='filtered',task='doi')).run()['selected']==0
    assert Plugin(config(collection_name='filtered',task='nodoi')).run()['selected']==1
    assert target.works.find_one()['doi']==''


def test_conflicting_external_dois_without_primary_are_not_guessed():
    with pytest.raises(RuntimeError,match='multiple DOI'):
        snapshot_doi({'external_ids':[{'source':'doi','id':'10.1000/a'},
                                     {'source':'doi','id':'10.1000/b'}]})


def test_same_isbn_and_title_under_different_ids_is_not_inserted_twice(monkeypatch):
    first=nodoi(WORK_1);second=nodoi(WORK_2)
    target,_=identified(monkeypatch,[first,second])
    result=Plugin(config(collection_name='filtered',task='nodoi')).run()
    assert result['inserted']==1 and result['updated']==1
    assert target.works.count_documents({})==1


def test_nodoi_enriches_existing_doi_work_without_removing_its_doi(monkeypatch):
    target,_=identified(monkeypatch,[nodoi()])
    owner=nodoi(WORK_1);owner['authors'][0]['id']='person-canonical'
    owner['doi']='https://doi.org/10.1000/book';target.works.insert_one(owner)
    result=Plugin(config(collection_name='filtered',task='nodoi')).run()
    assert result['updated']==1 and result['inserted']==0
    assert target.works.count_documents({})==1
    assert target.works.find_one()['doi']==owner['doi']


def test_formatted_isbn_variant_uses_title_fallback(monkeypatch):
    target,_=identified(monkeypatch,[nodoi()])
    owner=nodoi(WORK_1);owner['authors'][0]['id']='person-canonical'
    owner['external_ids'][0]['id']='978-958-20-1173-4';target.works.insert_one(owner)
    assert Plugin(config(collection_name='filtered',task='nodoi')).run()['updated']==1
    assert target.works.count_documents({})==1


@pytest.mark.parametrize('difference',['chapter_title','edition_year'])
def test_isbn_does_not_merge_other_chapters_or_editions(monkeypatch,difference):
    a,b=nodoi(WORK_1),nodoi(WORK_2)
    if difference=='chapter_title':b['titles'][0]['title']='Capítulo totalmente distinto'
    else:b['year_published']=2025
    target,_=identified(monkeypatch,[a,b])
    Plugin(config(collection_name='filtered',task='nodoi')).run()
    assert target.works.count_documents({})==2


def test_issn_alone_never_causes_a_merge(monkeypatch):
    a,b=nodoi(WORK_1),nodoi(WORK_2)
    for row in [a,b]:row['external_ids']=[{'source':'issn','id':'1234-5678'}]
    target,_=prepare(monkeypatch,[a,b])
    Plugin(config(task='nodoi')).run()
    assert target.works.count_documents({})==2


def test_scienti_pair_lookup_ignores_dictionary_key_order(monkeypatch):
    row=nodoi();row['external_ids']=[{'source':'scienti','id':{'COD_RH':'0000000001','COD_PRODUCTO':'12'}}]
    target,_=identified(monkeypatch,[row])
    owner=deepcopy(row);owner['_id']=WORK_1
    owner['external_ids'][0]['id']={'COD_PRODUCTO':'12','COD_RH':'0000000001'}
    target.works.insert_one(owner)
    assert Plugin(config(collection_name='filtered',task='nodoi')).run()['updated']==1
    assert target.works.count_documents({})==1


def test_ambiguous_compatible_identifier_candidates_are_reported_without_a_third_work(monkeypatch):
    target,_=identified(monkeypatch,[nodoi(ObjectId('000000000000000000000003'))])
    owners=[nodoi(WORK_1),nodoi(WORK_2)]
    for owner in owners:owner['authors'][0]['id']='person-canonical'
    target.works.insert_many(owners)
    result=Plugin(config(collection_name='filtered',task='nodoi')).run()
    assert result['identity_conflicts']==1
    assert target.works.count_documents({})==2


def test_nodoi_source_id_collision_does_not_overwrite_other_work(monkeypatch):
    target,_=identified(monkeypatch,[nodoi(WORK_1)])
    other=nodoi(WORK_1,'Un documento enteramente diferente');target.works.insert_one(other)
    result=Plugin(config(collection_name='filtered',task='nodoi')).run()
    assert result['identity_conflicts']==1
    assert target.works.find_one()==other


def test_isbn_and_generic_chapter_title_do_not_merge_disjoint_authors(monkeypatch):
    target,_=identified(monkeypatch,[nodoi()])
    owner=nodoi(WORK_1)
    owner['authors'][0]['id']='different-person'
    target.works.insert_one(owner)
    result=Plugin(config(collection_name='filtered',task='nodoi')).run()
    assert result['inserted']==1
    assert target.works.count_documents({})==2


def _concurrent_nodoi_insert(arguments):
    from pymongo import MongoClient
    url, database, identifier = arguments
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    try:
        plugin = Plugin.__new__(Plugin)
        plugin.collection = client[database].works
        counters = {'inserted': 0, 'updated': 0}
        incoming = KahiBase().empty_work()
        incoming.update(nodoi(ObjectId(identifier)))
        incoming.pop('source_metadata', None)
        for field in ['authorship_status', 'authorship_candidates', 'authorship_evidence',
                      'authorship_authority', 'identity_collision_of']:
            incoming.pop(field, None)
        plugin._upsert_without_doi(incoming, counters)
        return counters
    finally:
        client.close()


def test_real_multiprocess_nodoi_insert_is_unique():
    import os
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    from uuid import uuid4
    from pymongo import MongoClient
    from kahi_impactu_utils.Work import ensure_work_doi_indexes
    url = os.environ.get('KAHI_TEST_MONGODB_URL')
    if not url:
        pytest.skip('Set KAHI_TEST_MONGODB_URL for isolated MongoDB tests')
    name = 'test_mc_work_' + uuid4().hex
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    try:
        ensure_work_doi_indexes(client[name].works)
        arguments = [(url, name, str(ObjectId())) for _ in range(8)]
        with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('spawn')) as pool:
            results = list(pool.map(_concurrent_nodoi_insert, arguments))
        assert sum(result['inserted'] for result in results) == 1
        assert sum(result['updated'] for result in results) == 7
        assert client[name].works.count_documents({}) == 1
        assert '_doi_key' not in client[name].works.find_one()
    finally:
        client.drop_database(name)
        client.close()


def pointer(source, **changes):
    document = {
        '_id': 'current', 'release_name': 'release_six', 'full_graph': 'works_final',
        'collection': 'filtered', 'run_name': 'selection', 'audit': 'selection',
    }
    document.update(changes)
    source.scienti_identified_work_publications.insert_one(document)


def test_current_pointer_selects_the_filtered_collection_without_collection_name(monkeypatch):
    target, source = identified(monkeypatch, [work_snapshot(), nodoi()])
    pointer(source)
    plugin = Plugin(config(task='doi'))
    assert plugin.identified_snapshot and plugin.source_collection_name == 'filtered'
    assert plugin.run()['selected'] == 1
    assert target.works.count_documents({}) == 1


def test_current_pointer_of_another_release_is_rejected(monkeypatch):
    _, source = prepare(monkeypatch)
    pointer(source, release_name='release_other')
    with pytest.raises(RuntimeError, match='identified works pointer'):
        Plugin(config(task='doi'))


def test_identified_false_uses_the_full_graph(monkeypatch):
    _, source = identified(monkeypatch)
    pointer(source)
    plugin = Plugin(config(task='doi', identified=False))
    assert not plugin.identified_snapshot and plugin.source_collection_name == 'works_final'


def test_type_audit_is_discarded_on_import(monkeypatch):
    row = work_snapshot()
    row['type_audit'] = {'findings': [{'reason': 'bold_text_is_not_a_catalog_type',
                                       'raw_type': 'Endocrine Society'}]}
    target, _ = prepare(monkeypatch, works=[row])
    assert Plugin(config(task='doi')).run()['selected'] == 1
    stored = target.works.find_one()
    assert stored and 'type_audit' not in stored
