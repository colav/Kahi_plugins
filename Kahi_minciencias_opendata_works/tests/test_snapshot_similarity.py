from copy import deepcopy
from unittest.mock import MagicMock
from bson import ObjectId
import pytest

from test_works_snapshot_ingestion import Plugin, config, WORK_1, WORK_2
from test_snapshot_phases import identified, nodoi
from kahi_minciencias_opendata_works import snapshot_similarity as module

TITLE='Effects of regional clinical interventions on patient outcomes'
VARIANT='Effects of regional clinical interventions on patienx outcomes'


class Engine:
    def __init__(self):
        self.handler=MagicMock()
        self.handler.es.count.return_value={'count':1}
        self.hits=[];self.searches=[];self.synced=[]
        self.search_error=None;self.sync_error=None

    def candidates(self,document):
        self.searches.append(deepcopy(document))
        if self.search_error:raise self.search_error
        return list(self.hits),1

    def sync(self,document):
        if self.sync_error:raise self.sync_error
        self.synced.append(deepcopy(document))


def setup(monkeypatch,with_owner=True,title=VARIANT):
    incoming=nodoi(WORK_2,title)
    target,source=identified(monkeypatch,[incoming])
    engine=Engine()
    monkeypatch.setattr(module,'SnapshotSimilarity',lambda params:engine)
    if with_owner:
        owner=nodoi(WORK_1,TITLE)
        owner['authors'][0]['id']='person-canonical'
        owner['external_ids']=[{'source':'openalex','id':'W123'}]
        owner['doi']='https://doi.org/10.1000/retained'
        owner['citations_count']=[{'source':'openalex','count':42}]
        target.works.insert_one(owner)
        engine.hits=[str(WORK_1)]
    options={'collection_name':'filtered','task':'nodoi','es_index':'works-index','es_url':'http://test:9200'}
    return target,source,engine,options


def test_similarity_updates_other_identifiers_and_preserves_doi_and_metadata(monkeypatch):
    target,_,engine,options=setup(monkeypatch)
    result=Plugin(config(**options)).run()
    work=target.works.find_one()
    assert result['similarity_matches']==1 and result['updated']==1
    assert target.works.count_documents({})==1
    assert work['_id']==WORK_1 and work['doi']=='https://doi.org/10.1000/retained'
    assert work['citations_count']==[{'source':'openalex','count':42}]
    assert len(work['titles'])==2
    assert engine.synced[0]['_id']==WORK_1


def test_exact_live_title_fast_path_does_not_need_an_es_query(monkeypatch):
    target,_,engine,options=setup(monkeypatch,title=TITLE)
    engine.search_error=RuntimeError('search should not be called')
    result=Plugin(config(**options)).run()
    assert result['updated']==1 and result['similarity_matches']==1
    assert engine.searches==[] and len(engine.synced)==1


def test_identifier_lookup_precedes_similarity(monkeypatch):
    target,_,engine,options=setup(monkeypatch)
    target.works.update_one({'_id':WORK_1},{'$set':{'external_ids':[{'source':'isbn','id':'9789582011734'}]}})
    engine.search_error=RuntimeError('search should not be called')
    assert Plugin(config(**options)).run()['updated']==1
    assert engine.searches==[]


@pytest.mark.parametrize('difference',['title','year','authors','volume','issue','start_page'])
def test_es_hit_cannot_override_incompatible_live_metadata(monkeypatch,difference):
    target,source,engine,options=setup(monkeypatch)
    if difference=='title':target.works.update_one({}, {'$set':{'titles':[{'title':'Unrelated publication'}]}})
    elif difference=='year':target.works.update_one({}, {'$set':{'year_published':2023}})
    elif difference=='authors':target.works.update_one({}, {'$set':{'authors':[{'id':'other','full_name':'Otra Persona'}]}})
    else:
        target.works.update_one({}, {'$set':{'bibliographic_info.'+difference:'other'}})
        source.filtered.update_one({}, {'$set':{'bibliographic_info.'+difference:'123'}})
    result=Plugin(config(**options)).run()
    assert result['inserted']==1 and target.works.count_documents({})==2


def test_current_bibliography_is_verified(monkeypatch):
    target,source,engine,options=setup(monkeypatch)
    source.filtered.update_one({}, {'$set':{'bibliographic_info.start_page':'12'}})
    target.works.update_one({}, {'$set':{'bibliographic_info.start_page':'45'}})
    assert Plugin(config(**options)).run()['inserted']==1


def test_stale_es_hit_is_reported_and_does_not_drop_source(monkeypatch):
    target,_,engine,options=setup(monkeypatch,with_owner=False)
    engine.hits=[str(WORK_1)]
    result=Plugin(config(**options)).run()
    assert result['stale_es_hits']==1 and result['inserted']==1
    assert target.works.find_one()['_id']==WORK_2


def test_multiple_live_candidates_do_not_create_a_third_work(monkeypatch):
    target,_,engine,options=setup(monkeypatch)
    second=target.works.find_one();second['_id']=ObjectId();target.works.insert_one(second)
    engine.hits=[str(WORK_1),str(second['_id'])]
    result=Plugin(config(**options)).run()
    assert result['identity_conflicts']==1 and target.works.count_documents({})==2


def test_es_search_failure_stops_before_inserting_blindly(monkeypatch):
    target,_,engine,options=setup(monkeypatch,with_owner=False)
    engine.search_error=ConnectionError('ES unavailable')
    with pytest.raises(ConnectionError):Plugin(config(**options)).run()
    assert target.works.count_documents({})==0
    assert target.minciencias_opendata_works_runs.find_one()['status']=='failed'


def test_index_write_failure_retries_without_duplicate_mongo_work(monkeypatch):
    target,_,engine,options=setup(monkeypatch,with_owner=False)
    engine.sync_error=ConnectionError('write unavailable')
    with pytest.raises(ConnectionError):Plugin(config(**options)).run()
    assert target.works.count_documents({})==1
    engine.sync_error=None
    result=Plugin(config(**options)).run()
    assert result['updated']==1 and target.works.count_documents({})==1
    assert len(engine.synced)==1


def test_similarity_changes_checkpoint_identity(monkeypatch):
    _,_,engine,options=setup(monkeypatch,with_owner=False)
    no_es=Plugin(config(collection_name='filtered',task='nodoi'))
    with_es=Plugin(config(**options))
    assert no_es.run_id!=with_es.run_id
    assert with_es.run()['similarity_policy']=='live-title-author-year-v1'


@pytest.mark.parametrize('changes',[{'task':'doi'},{'es_url':''},{'es_user':'user'}])
def test_invalid_es_config_is_rejected(monkeypatch,changes):
    _,_,engine,options=setup(monkeypatch,with_owner=False)
    options.update(changes)
    with pytest.raises(ValueError):Plugin(config(**options))


def test_empty_index_with_existing_works_is_rejected(monkeypatch):
    _,_,engine,options=setup(monkeypatch)
    engine.handler.es.count.return_value={'count':0}
    with pytest.raises(RuntimeError,match='populated'):Plugin(config(**options))


def test_doi_phase_does_not_initialize_or_query_es(monkeypatch):
    target,source,engine,options=setup(monkeypatch,with_owner=False)
    options={key:value for key,value in options.items() if not key.startswith('es_')}
    options['task']='doi'
    monkeypatch.setattr(module,'SnapshotSimilarity',lambda params:pytest.fail('DOI initialized ES'))
    assert Plugin(config(**options)).run()['selected']==0


def test_author_names_allow_match_when_ids_are_missing(monkeypatch):
    target,_,engine,options=setup(monkeypatch)
    target.works.update_one({}, {'$set':{'authors':[{'id':'','full_name':'Nombre canónico'}]}})
    assert Plugin(config(**options)).run()['updated']==1


def test_authorless_matching_requires_exact_title_and_bibliographic_evidence(monkeypatch):
    target,source,engine,options=setup(monkeypatch,title=TITLE)
    source.filtered.update_one({}, {'$set':{'authors':[],'bibliographic_info.start_page':'12'}})
    target.works.update_one({}, {'$set':{'authors':[],'bibliographic_info.start_page':'12'}})
    assert Plugin(config(**options)).run()['updated']==1


def test_authorless_title_and_year_alone_do_not_merge(monkeypatch):
    target,source,engine,options=setup(monkeypatch,title=TITLE)
    source.filtered.update_one({}, {'$set':{'authors':[]}})
    target.works.update_one({}, {'$set':{'authors':[]}})
    assert Plugin(config(**options)).run()['inserted']==1


def test_adapter_uses_installed_mohan_signature_and_indexes_with_bounded_cache(monkeypatch):
    handler=MagicMock();handler.search_work.return_value=[{'_id':str(WORK_1)}]
    constructor=MagicMock(return_value=handler);monkeypatch.setattr(module,'Similarity',constructor)
    adapter=module.SnapshotSimilarity({'es_index':'idx','es_url':'http://test:9200'})
    row=nodoi();row['bibliographic_info']['start_page']='12';hits,queries=adapter.candidates(row)
    assert hits==[str(WORK_1)] and queries==1
    kwargs=handler.search_work.call_args.kwargs
    assert kwargs['use_es_thold'] is True and kwargs['es_thold']==0 and kwargs['hits']==20
    assert 'page_start' in kwargs and 'page_end' in kwargs
    adapter.sync(row);adapter.sync(row)
    assert handler.insert_work.call_count==1
    payload=handler.insert_work.call_args.kwargs['work']
    assert payload['page_start']=='12' and 'first_page' not in payload
    assert 'page_end' in payload and 'last_page' not in payload
    row['year_published']=2025;adapter.sync(row)
    assert handler.insert_work.call_count==2
    handler.insert_work.side_effect=ConnectionError('ES unavailable')
    row['year_published']=2026
    with pytest.raises(ConnectionError):adapter.sync(row)
    handler.insert_work.side_effect=None;adapter.sync(row)
    assert handler.insert_work.call_count==4


def _concurrent_title_insert(arguments):
    from time import sleep
    from pymongo import MongoClient
    from kahi.KahiBase import KahiBase
    from kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works import SOURCE_ONLY_FIELDS
    class FakeIndex:
        def candidates(self,document):
            sleep(0.05)
            return [],1
        def sync(self,document):
            return None
    url,database,identifier=arguments
    client=MongoClient(url,serverSelectionTimeoutMS=5000)
    try:
        plugin=Plugin.__new__(Plugin);plugin.collection=client[database].works;plugin.similarity=FakeIndex()
        row=KahiBase().empty_work();row.update(nodoi(ObjectId(identifier),TITLE))
        row['external_ids']=[{'source':'minciencias','id':'TEST-'+identifier}]
        for field in SOURCE_ONLY_FIELDS:row.pop(field,None)
        counters={'inserted':0,'updated':0}
        plugin._upsert_without_doi(row,counters)
        return counters
    finally:client.close()


def test_real_multiprocess_exact_title_with_other_identifiers_is_unique():
    import os,multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    from uuid import uuid4
    from pymongo import MongoClient
    from kahi_impactu_utils.Work import ensure_work_doi_indexes
    url=os.environ.get('KAHI_TEST_MONGODB_URL')
    if not url:pytest.skip('Set KAHI_TEST_MONGODB_URL for isolated MongoDB tests')
    name='test_mc_title_'+uuid4().hex;client=MongoClient(url,serverSelectionTimeoutMS=5000)
    try:
        ensure_work_doi_indexes(client[name].works)
        client[name].works.create_index('titles.title',collation={'locale':'en','strength':2})
        args=[(url,name,str(ObjectId())) for _ in range(8)]
        with ProcessPoolExecutor(max_workers=4,mp_context=multiprocessing.get_context('spawn')) as pool:
            results=list(pool.map(_concurrent_title_insert,args))
        assert sum(result['inserted'] for result in results)==1
        assert sum(result['updated'] for result in results)==7
        assert client[name].works.count_documents({})==1
    finally:client.drop_database(name);client.close()


def test_slow_lookup_cannot_insert_after_its_identity_lease_expires(monkeypatch):
    from datetime import datetime,timedelta,timezone
    import importlib
    main=importlib.import_module('kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works')
    target,_,engine,options=setup(monkeypatch,with_owner=False)
    monkeypatch.setattr(main,'monotonic',MagicMock(side_effect=[0,121]))
    def expire(document):
        target['_kahi_work_identity'].update_many({}, {'$set':{'expires_at':datetime.now(timezone.utc)-timedelta(seconds=1)}})
        return [],1
    engine.candidates=expire
    with pytest.raises(RuntimeError,match='identity lease expired'):
        Plugin(config(**options)).run()
    assert target.works.count_documents({})==0


def test_workers_process_batch_rows_concurrently_with_the_same_totals(monkeypatch):
    import threading
    rows=[nodoi(ObjectId(),f'Titulo distinto numero {i} para la fila paralela') for i in range(12)]
    for i,row in enumerate(rows):row['external_ids']=[{'source':'isbn','id':f'97895820117{i:02d}'}]
    target,source=identified(monkeypatch,rows)
    engine=Engine();monkeypatch.setattr(module,'SnapshotSimilarity',lambda params:engine)
    seen=set();lock=threading.Lock()
    original=Plugin._process_row
    def spy(self,*args,**kwargs):
        with lock:seen.add(threading.get_ident())
        return original(self,*args,**kwargs)
    monkeypatch.setattr(Plugin,'_process_row',spy)
    plugin=Plugin(config(collection_name='filtered',task='nodoi',workers=4,es_index='idx',es_url='http://test:9200'))
    assert plugin.workers==4
    summary=plugin.run()
    assert summary['inserted']==12 and summary['updated']==0 and summary['selected']==12
    assert target.works.count_documents({})==12
    assert len(seen)>1


def test_workers_default_is_sequential_and_counters_merge_partial_keys(monkeypatch):
    rows=[nodoi(ObjectId(),f'Otro titulo {i}') for i in range(3)]
    target,source=identified(monkeypatch,rows)
    engine=Engine();monkeypatch.setattr(module,'SnapshotSimilarity',lambda params:engine)
    plugin=Plugin(config(collection_name='filtered',task='nodoi',es_index='idx',es_url='http://test:9200'))
    assert plugin.workers==1
    counters={'inserted':0,'updated':0}
    plugin._merge_counters(counters,{'inserted':2,'similarity_queries':5})
    assert counters=={'inserted':2,'updated':0,'similarity_queries':5}


def test_real_threads_same_title_rows_in_one_batch_insert_once():
    import os
    from uuid import uuid4
    from pymongo import MongoClient
    from kahi.KahiBase import KahiBase
    from kahi_impactu_utils.Work import ensure_work_doi_indexes
    from kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works import SOURCE_ONLY_FIELDS
    url=os.environ.get('KAHI_TEST_MONGODB_URL')
    if not url:pytest.skip('Set KAHI_TEST_MONGODB_URL for isolated MongoDB tests')
    name='test_mt_title_'+uuid4().hex;client=MongoClient(url,serverSelectionTimeoutMS=5000)
    try:
        ensure_work_doi_indexes(client[name].works)
        client[name].works.create_index('titles.title',collation={'locale':'en','strength':2})
        class FakeIndex:
            def candidates(self,document):
                from time import sleep;sleep(0.05);return [],1
            def sync(self,document):return None
        plugin=Plugin.__new__(Plugin);plugin.collection=client[name].works;plugin.similarity=FakeIndex()
        plugin.workers=4;plugin._counters_lock=__import__('threading').Lock()
        rows=[]
        for _ in range(8):
            identifier=str(ObjectId());row=KahiBase().empty_work();row.update(nodoi(ObjectId(identifier),TITLE))
            row['external_ids']=[{'source':'minciencias','id':'TEST-'+identifier}]
            for field in SOURCE_ONLY_FIELDS:row.pop(field,None)
            rows.append(row)
        plugin._project=lambda source,people,groups,institutions,counters:deepcopy(source)
        plugin._reference_maps=lambda batch:({},{},{})
        counters={'inserted':0,'updated':0}
        plugin._process_batch(rows,counters)
        assert counters['inserted']==1 and counters['updated']==7
        assert client[name].works.count_documents({})==1
    finally:client.drop_database(name);client.close()


def test_sync_cache_is_thread_safe(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    handler=MagicMock();monkeypatch.setattr(module,'Similarity',MagicMock(return_value=handler))
    adapter=module.SnapshotSimilarity({'es_index':'idx','es_url':'http://test:9200'})
    docs=[nodoi(ObjectId(),f'Titulo {i}') for i in range(300)]
    with ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(adapter.sync,docs))
    assert handler.insert_work.call_count==300 and len(adapter._indexed)==300


def test_adapter_sizes_the_es_pool_to_the_workers(monkeypatch):
    handler=MagicMock();monkeypatch.setattr(module,'Similarity',MagicMock(return_value=handler))
    sizer=MagicMock();monkeypatch.setattr(module,'size_similarity_pool',sizer)
    module.SnapshotSimilarity({'es_index':'idx','es_url':'http://test:9200','es_user':'u','es_password':'p','workers':16})
    sizer.assert_called_once_with(handler,'http://test:9200',('u','p'),16)


def _work(author_id,name,**extra):
    work={'titles':[{'title':'Variaciones diarias de parámetros ruminales y sanguíneos en vacas lecheras'}],
          'year_published':2012,'types':[],'bibliographic_info':{},
          'authors':[{'id':author_id,'full_name':name}]}
    work.update(extra);return work


def test_disjoint_person_ids_fall_back_to_a_strict_name_match():
    # Clarissa Strieder Barboza has two person records (split OpenAlex profiles).
    incoming=_work('6aefdfba5e00d0d45e885ba8','Clarissa Strieder Barboza')
    owner=_work('6af029695e00d0d45ea46c0b','Clarissa Strieder Barboza')
    assert Plugin._similarity_compatible(incoming,owner)
    stranger=_work('6af0000000000000000000ff','Fernando Wittwer')
    assert not Plugin._similarity_compatible(incoming,stranger)
    shared=_work('6aefdfba5e00d0d45e885ba8','C Strieder')
    assert Plugin._similarity_compatible(incoming,shared)
