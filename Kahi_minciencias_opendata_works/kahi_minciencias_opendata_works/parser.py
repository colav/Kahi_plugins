from kahi_impactu_utils.Utils import lang_poll, check_date_format
from time import time


def parse_minciencias_opendata(reg, empty_work, verbose=0):
    """
    Parse a record from the minciencias opendata database into a work entry, using the empty_work as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from minciencias opendata.
    empty_work : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = empty_work.copy()
    entry["updated"] = [{"source": "minciencias", "time": int(time())}]
    if 'nme_producto_pd' in reg.keys():
        if reg["nme_producto_pd"]:
            lang = lang_poll(reg["nme_producto_pd"], verbose=verbose)
    entry["titles"].append(
        {"title": reg["nme_producto_pd"], "lang": lang, "source": "minciencias"})
    if "id_producto_pd" in reg.keys():
        if reg["id_producto_pd"]:
            entry["external_ids"].append(
                {"provenance": "minciencias", "source": "minciencias", "id": reg["id_producto_pd"]})
    if "id_tipo_pd_med" in reg.keys():
        date = ""
        if "ano_convo" in reg.keys():
            if reg["ano_convo"]:
                date = check_date_format(reg["ano_convo"])
        if reg["id_tipo_pd_med"]:
            entry["ranking"].append(
                {"provenance": "minciencias", "date": date, "rank": reg["id_tipo_pd_med"], "source": "minciencias"})
    if "nme_tipologia_pd" in reg.keys():
        if reg["nme_tipologia_pd"]:
            typ = reg["nme_tipologia_pd"]
            entry["types"].append(
                {"provenance": "minciencias", "source": "minciencias", "type": typ})
    # authors section
    if 'id_persona_pd' in reg.keys():
        if reg["id_persona_pd"]:
            minciencias_id = reg["id_persona_pd"]
        affiliation = []
        group_name = ""
        if "cod_grupo_gr" in reg.keys():
            if reg["cod_grupo_gr"]:
                if "nme_grupo_gr" in reg.keys():
                    if reg["nme_grupo_gr"]:
                        group_name = reg["nme_grupo_gr"]
            affiliation.append(
                {
                    "external_ids": [{"provenance": "minciencias", "source": "minciencias", "id": reg["cod_grupo_gr"]}],
                    "name": group_name
                }
            )
        author_entry = {
            "full_name": "",
            "types": [],
            "affiliations": [affiliation[0]] if affiliation else [],
            "external_ids": [{"provenance": "minciencias", "source": "minciencias", "id": minciencias_id}]
        }
        entry["authors"] = [author_entry]
    return entry
