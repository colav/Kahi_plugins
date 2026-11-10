import importlib

from pandas import DataFrame

from test_optional_units import instance, staff_record

module = importlib.import_module('kahi_staff_person.Kahi_staff_person')


def test_regular_columns_build_name_parts():
    names = module.staff_name_fields(staff_record(nombres='Ana María', primer_apellido='Pérez-Gómez',
                                                  segundo_apellido='Ruiz', nombre_completo='ignorado'))
    assert names == {'first_names': ['Ana', 'María'], 'last_names': ['Pérez Gómez', 'Ruiz'],
                     'initials': 'AM', 'full_name': 'Ana María Pérez Gómez Ruiz',
                     'alias': 'ana maría pérez-gómez ruiz'}


def test_nombre_completo_is_used_without_splitting():
    plugin = instance([staff_record(nombres='', primer_apellido='', segundo_apellido='',
                                    nombre_completo='Andrés Felipe Perez')])
    plugin.process_staff()
    person = plugin.collection.find_one()
    assert person['full_name'] == 'Andrés Felipe Perez'
    assert person['first_names'] == [] and person['last_names'] == [] and person['initials'] == ''
    assert person['aliases'] == ['andrés felipe perez']
    assert [aff['id'] for aff in person['affiliations']] == ['institution']


def test_nombre_completo_dashes_become_spaces():
    names = module.staff_name_fields({'nombres': '', 'primer_apellido': '', 'segundo_apellido': '',
                                      'nombre_completo': 'Jaiberth Cardona‐Arias'})
    assert names['full_name'] == 'Jaiberth Cardona Arias'
    assert names['alias'] == 'jaiberth cardona‐arias'


def test_record_without_any_name_is_skipped():
    plugin = instance([staff_record(nombres='', primer_apellido='', segundo_apellido='')])
    plugin.process_staff()
    assert plugin.collection.count_documents({}) == 0


def test_missing_name_columns_are_skipped_too():
    record = {k: v for k, v in staff_record().items() if k not in ('nombres', 'primer_apellido', 'segundo_apellido')}
    plugin = instance([record])
    plugin.process_staff()
    assert plugin.collection.count_documents({}) == 0


def test_nameless_record_still_enriches_existing_person():
    plugin = instance([staff_record(nombres='', primer_apellido='', segundo_apellido='')])
    plugin.collection.insert_one({'_id': 'p1', 'full_name': 'Ana Perez', 'first_names': ['Ana'],
                                  'last_names': ['Perez'], 'initials': 'A', 'aliases': [], 'updated': [],
                                  'external_ids': [{'source': 'Cédula de Ciudadanía', 'id': '123'}],
                                  'affiliations': [], 'degrees': [], 'ranking': [], 'related_works': []})
    plugin.process_staff()
    person = plugin.collection.find_one({'_id': 'p1'})
    assert person['full_name'] == 'Ana Perez'
    assert 'institution' in [aff['id'] for aff in person['affiliations']]
    assert '' not in person['aliases']


def test_staff_full_name_uses_same_rule():
    assert module.staff_full_name([{'nombres': '', 'primer_apellido': '', 'segundo_apellido': '',
                                    'nombre_completo': 'Luz Dary Ríos'}]) == 'Luz Dary Ríos'
    assert module.staff_full_name([{'nombres': 'Luz', 'primer_apellido': 'Ríos'}]) == 'Luz Ríos'
    assert module.staff_full_name([{}]) == ''


def test_optional_full_name_column_is_filled_when_absent(monkeypatch):
    plugin = instance([staff_record()])
    plugin.staff_required_columns = list(staff_record()) + ['nombre_completo'] + sorted(module.STAFF_UNIT_COLUMNS)
    monkeypatch.setattr(module, 'read_excel', lambda *args, **kwargs: DataFrame([staff_record()]))
    data = plugin._load_staff_data({'staff_file_path': 'example.xlsx'}, 'institution')
    assert data.iloc[0]['nombre_completo'] == ''


def test_file_with_only_nombre_completo_loads(monkeypatch):
    plugin = instance([staff_record()])
    plugin.staff_required_columns = list(staff_record()) + ['nombre_completo'] + sorted(module.STAFF_UNIT_COLUMNS)
    record = {k: v for k, v in staff_record().items() if k not in ('nombres', 'primer_apellido', 'segundo_apellido')}
    record['nombre_completo'] = 'Andrés Felipe Perez'
    monkeypatch.setattr(module, 'read_excel', lambda *args, **kwargs: DataFrame([record]))
    data = plugin._load_staff_data({'staff_file_path': 'example.xlsx'}, 'institution')
    assert data.iloc[0]['nombres'] == '' and data.iloc[0]['nombre_completo'] == 'Andrés Felipe Perez'


def test_rerun_does_not_duplicate_staff_updated_entry():
    plugin = instance([staff_record()])
    plugin.process_staff()
    plugin.process_staff()
    person = plugin.collection.find_one()
    assert [u['source'] for u in person['updated']] == ['staff']
    assert [aff['id'] for aff in person['affiliations']] == ['institution']


def test_merge_keeps_entries_of_other_sources():
    merged = module.merge_staff_person(
        {'updated': [{'time': 1, 'source': 'scienti', 'provenance': 'scienti'}]},
        {'updated': [{'time': 2, 'source': 'staff', 'provenance': 'staff'}]})
    assert [u['source'] for u in merged['updated']] == ['scienti', 'staff']
