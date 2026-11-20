from copy import deepcopy
import mongomock
from kahi.KahiBase import KahiBase
from kahi_siiu_projects.parser import parse_siiu
from kahi_siiu_projects.process_one import process_one


def empty():
    return deepcopy(object.__new__(KahiBase).empty_project())


def record():
    return {
        "CODIGO": "P-1", "NOMBRE_COMPLETO": "Proyecto SIIU",
        "project_participant": [{
            "PERSONA_NATURAL": None,
            "project_participant_role": [{"IDENTIFICADOR": 307}],
            "INSTITUCION": "890980040",
        }],
    }


def test_null_principal_is_ignored():
    entry = parse_siiu(record(), empty())
    assert entry["authors"] == []


def test_retry_is_idempotent_by_code(monkeypatch):
    from importlib import import_module
    # The ordered pipeline is verified separately against a real MongoDB.
    monkeypatch.setattr(
        import_module("kahi_siiu_projects.process_one"), "insert_only_update",
        lambda document: {"$setOnInsert": document},
    )
    db = mongomock.MongoClient().db
    for _ in range(2):
        process_one(record(), db, db.projects, empty(), None)
    assert db.projects.count_documents({}) == 1


def text(label, value, requested=True):
    item = {"TITULO": None if requested else label, "TEXTO_INGRESADO": value}
    if requested:
        item["requested_text"] = [{"TITULO": label}]
    return item


def test_abstract_is_the_text_labelled_resumen_without_markup():
    from kahi_siiu_projects.parser import project_texts
    reg = {"descriptive_text": [
        text("Objetivo general", "<p>Evaluar la conectividad</p>"),
        text("Resumen ejecutivo", "<p class=\"MsoNormal\">La conectividad&nbsp;funcional "
             "se asocia con <b>relaciones</b> entre circuitos cerebrales.</p>"),
        text("Resumen", "Corto"),
    ]}
    assert project_texts(reg) == (
        "La conectividad funcional se asocia con relaciones entre circuitos cerebrales.", "")


def test_without_resumen_the_topic_text_is_objective_and_problem_statement():
    from kahi_siiu_projects.parser import MAX_TOPIC_TEXT_CHARS, project_texts
    reg = {"descriptive_text": [
        text("Metodología", "No se usa"),
        text("Planteamiento del problema", "<p>" + "Existen muchos estudios. " * 400 + "</p>"),
        text("OBJETIVO GENERAL", "Caracterizar la biomasa residual.", requested=False),
    ]}
    abstract, topic_text = project_texts(reg)
    assert abstract == ""
    assert topic_text.startswith("Caracterizar la biomasa residual. Existen muchos estudios.")
    assert len(topic_text) <= MAX_TOPIC_TEXT_CHARS
    assert project_texts({}) == ("", "") and project_texts({"descriptive_text": None}) == ("", "")


def test_parse_fills_abstract_and_topic_text_when_the_template_has_it():
    reg = record()
    reg["descriptive_text"] = [text("Objetivo general", "Caracterizar la biomasa residual.")]
    old_template = empty()
    old_template.pop("topic_text", None)
    assert "topic_text" not in parse_siiu(reg, old_template)
    template = dict(empty(), topic_text="")
    entry = parse_siiu(reg, template)
    assert entry["abstract"] == "" and entry["topic_text"] == "Caracterizar la biomasa residual."
    reg["descriptive_text"].append(text("Resumen", "Un resumen del proyecto de biomasa."))
    entry = parse_siiu(reg, dict(empty(), topic_text=""))
    assert entry["abstract"] == "Un resumen del proyecto de biomasa." and entry["topic_text"] == ""
