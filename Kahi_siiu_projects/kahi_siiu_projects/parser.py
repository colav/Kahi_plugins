from kahi_impactu_utils.Utils import lang_poll
from time import time
import html
import re
import unicodedata

TAG_RE = re.compile(r"<[^>]+>")
# Labels of the SIIU descriptive texts, normalized (see ``_label``).
ABSTRACT_LABEL = "resumen"
TOPIC_TEXT_LABELS = ("objetivo general", "planteamiento del problema")
MAX_TOPIC_TEXT_CHARS = 3000


def _label(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z ]+", " ", text.lower()).split())


def clean_text(value):
    """Plain text of a SIIU rich-text field (HTML tags and entities removed)."""
    text = html.unescape(TAG_RE.sub(" ", str(value or "")))
    return " ".join(text.replace("\xa0", " ").split())


def descriptive_texts(reg):
    """Return ``{label: text}`` with the longest text of each label of the project."""
    texts = {}
    for item in reg.get("descriptive_text") or []:
        if not isinstance(item, dict):
            continue
        requested = item.get("requested_text")
        requested = requested[0] if isinstance(requested, list) and requested else {}
        label = _label(requested.get("TITULO") or item.get("TITULO"))
        text = clean_text(item.get("TEXTO_INGRESADO"))
        if label and text and len(text) > len(texts.get(label, "")):
            texts[label] = text
    return texts


def project_texts(reg):
    """
    Abstract and topic text of a SIIU project.

    The abstract is the text labelled "Resumen" (also "Resumen ejecutivo").
    Without one, ``topic_text`` joins the general objective and the problem
    statement: enough to infer topics, but not shown as the abstract.
    """
    texts = descriptive_texts(reg)
    abstracts = [text for label, text in texts.items() if label.startswith(ABSTRACT_LABEL)]
    if abstracts:
        return max(abstracts, key=len), ""
    parts = [texts[label] for label in TOPIC_TEXT_LABELS if label in texts]
    return "", " ".join(parts)[:MAX_TOPIC_TEXT_CHARS].strip()


def parse_siiu(reg, empty_project, verbose=0):
    """
    Parse a record from the SIIU database into a project entry, using the empty_project as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from siiu
    empty_project : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = empty_project.copy()
    entry["updated"] = [{"source": "siiu", "time": int(time())}]
    title = reg.get("NOMBRE_COMPLETO")
    code = reg.get("CODIGO")
    if not isinstance(title, str) or not title.strip() or code in (None, ""):
        return None
    lang = lang_poll(title, verbose=verbose)
    entry["titles"].append(
        {"title": title.strip(), "lang": lang, "source": "siiu"})
    entry["external_ids"].append(
        {"provenance": "siiu", "source": "codigo", "id": code})
    entry["abstract"], topic_text = project_texts(reg)
    if "topic_text" in entry:
        entry["topic_text"] = topic_text
    if "project_participant" in reg:
        for author in reg["project_participant"]:
            # solo investigador principar de momento
            roles = author.get("project_participant_role") or []
            person_id = author.get("PERSONA_NATURAL")
            if roles and roles[0].get("IDENTIFICADOR") == 307 and person_id:

                affiliations = []
                for group in author.get("group") or []:
                    if group.get("CODIGO_COLCIENCIAS"):
                        grec = {
                            "external_ids": [{"provenance": "siiu", "source": "scienti", "id": group["CODIGO_COLCIENCIAS"]}],
                            "name": group["NOMBRE_COMPLETO"]
                        }

                        affiliations.append(grec)
                        # hay que hacer un siiu affiliations y crozar los grupos apra ver si obtenemos mas NRO_ID_GRUPO
                        # affiliations.append(
                        #     {
                        #         "external_ids": [{"provenance": "siiu", "source": "scienti", "id": group["NRO_ID_GRUPO"]}],
                        #         "name": group["NOMBRE_COMPLETO"]
                        #     }
                        # )
                if author.get("INSTITUCION"):
                    affiliations.append({
                        "external_ids": [{"provenance": "siiu", "source": "nit", "id": author["INSTITUCION"]}]
                    })
                # type
                subtypes = reg.get("project_subtype") or []
                project_types = subtypes[0].get("project_type") or [] if subtypes else []
                if subtypes and project_types:

                    entry["types"].append(
                        {
                            "provenance": 'siiu',
                            "source": 'siiu',
                            "type": project_types[0].get("NOMBRE", ""),
                            "level": 0,
                            "code": str(project_types[0].get("IDENTIFICADOR", ""))
                        }
                    )
                    entry["types"].append(
                        {
                            "provenance": 'siiu',
                            "source": 'siiu',
                            "type": subtypes[0].get("NOMBRE", ""),
                            "level": 1,
                            "code": str(project_types[0].get("IDENTIFICADOR", "")) + str(subtypes[0].get("IDENTIFICADOR", ""))
                        }
                    )

                author_entry = {
                    "full_name": "",
                    "affiliations": affiliations,
                    "external_ids": [{"provenance": 'siiu', "source": 'Cédula de Ciudadanía', "id": person_id}]
                }
                entry["authors"].append(author_entry)
    return entry
