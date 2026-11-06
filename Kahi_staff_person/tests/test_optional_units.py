from copy import deepcopy
import importlib

import mongomock
from pandas import DataFrame
import pytest

module = importlib.import_module('kahi_staff_person.Kahi_staff_person')


def staff_record(**changes):
    record = {'identificación': '123', 'tipo_documento': 'cédula de ciudadanía',
        'nombres': 'Ana', 'primer_apellido': 'Perez', 'segundo_apellido': '',
        'fecha_inicial_vinculación': '01/01/2020', 'fecha_final_vinculación': '',
        'fecha_nacimiento': '', 'sexo': '', 'nivel_académico': '',
        'tipo_contrato': '', 'jornada_laboral': '', 'categoría_laboral': ''}
    record.update(changes)
    return record


def instance(records):
    plugin = object.__new__(module.Kahi_staff_person)
    plugin.db = mongomock.MongoClient().db
    plugin.collection = plugin.db.person
    plugin.staff_source_institution_id = 'institution'
    plugin.staff_reg = {'_id': 'institution', 'names': [{'name': 'University', 'lang': 'es'}],
                        'types': [{'type': 'education'}]}
    plugin.staff_data = DataFrame(records).fillna('')
    plugin.cedula_dep = {'123': ''}
    plugin.allowed_categories = []
    return plugin


def test_person_without_unit_fields_is_inserted_with_parent():
    plugin = instance([staff_record()])
    plugin.process_staff()
    person = plugin.collection.find_one()
    assert person['full_name'] == 'Ana Perez'
    assert [aff['id'] for aff in person['affiliations']] == ['institution']
    assert 2024 in person['affiliations'][0]['years']


def test_optional_excel_columns_are_filled_without_relaxing_other_requirements(monkeypatch):
    plugin = instance([staff_record()])
    plugin.staff_required_columns = list(staff_record()) + sorted(module.STAFF_UNIT_COLUMNS)
    monkeypatch.setattr(module, 'read_excel', lambda *args, **kwargs: DataFrame([staff_record()]))
    data = plugin._load_staff_data({'staff_file_path': 'example.xlsx'}, 'institution')
    assert all(data.iloc[0][col] == '' for col in module.STAFF_UNIT_COLUMNS)
    monkeypatch.setattr(module, 'read_excel', lambda *args, **kwargs: DataFrame([{'nombres': 'Ana'}]))
    with pytest.raises(ValueError):
        plugin._load_staff_data({'staff_file_path': 'example.xlsx'}, 'institution')


def test_each_unit_retains_multiple_periods_and_gaps_without_names():
    rows = [staff_record(**{'fecha_inicial_vinculación': start, 'fecha_final_vinculación': end,
            'código_unidad_académica': faculty, 'código_subunidad_académica': department})
        for start, end, faculty, department in [
            ('01/01/2018', '31/12/2020', 'F', 'D'),
            ('01/01/2022', '31/12/2023', 'F', 'D'),
            ('01/01/2024', '', 'G', '')]]
    plugin = instance(rows)
    for unit, kind in [('institution_F', 'faculty'), ('institution_F_D', 'department'),
                       ('institution_G', 'faculty')]:
        plugin.db.affiliations.insert_one({'_id': unit,
            'names': [{'name': unit, 'lang': 'es'}],
            'types': [{'source': 'staff', 'type': kind}], 'relations': [{'id': 'institution'}]})
    plugin.process_staff()
    person = plugin.collection.find_one()
    affs = {aff['id']: aff for aff in person['affiliations']}
    assert affs['institution_F']['years'] == [2018, 2019, 2020, 2022, 2023]
    assert affs['institution_F_D']['years'] == [2018, 2019, 2020, 2022, 2023]
    assert 2021 not in affs['institution']['years']
    assert 2024 in affs['institution_G']['years']
    plugin.process_staff()
    assert plugin.collection.count_documents({}) == 1
    assert len(plugin.collection.find_one()['affiliations']) == len(affs)


def test_existing_person_without_new_units_keeps_other_source_affiliations():
    plugin = instance([staff_record()])
    old = {'_id': 'person', 'full_name': 'Ana Perez',
        'external_ids': [{'source': 'Cédula de Ciudadanía', 'id': '123'}],
        'affiliations': [{'id': 'foreign', 'years': [2019]}]}
    plugin.collection.insert_one(deepcopy(old))
    plugin.process_staff()
    assert plugin.collection.find_one()['affiliations'][0] == old['affiliations'][0]
    assert len(plugin.collection.find_one()['affiliations']) == 2


def test_missing_unit_placeholders_do_not_generate_phantom_ids():
    record = {'unidad_académica': float('nan'), 'subunidad_académica': None}
    assert module.staff_faculty_id('institution', record) == ''
    assert module.staff_department_id('institution', record) == ''


def test_real_excel_without_unit_columns_can_be_loaded(tmp_path):
    plugin = instance([staff_record()])
    plugin.staff_required_columns = list(staff_record()) + sorted(module.STAFF_UNIT_COLUMNS)
    excel = tmp_path / 'staff_without_units.xlsx'
    DataFrame([staff_record()]).to_excel(excel, index=False)
    loaded = plugin._load_staff_data({'staff_file_path': str(excel)}, 'institution')
    assert loaded.iloc[0]['identificación'] == '123'
    assert all(loaded.iloc[0][col] == '' for col in module.STAFF_UNIT_COLUMNS)
