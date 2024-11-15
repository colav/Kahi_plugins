from kahi_impactu_utils.Utils import lang_poll, check_date_format
from time import time
from re import search


def parse_ids(product_id, regex, values):
    """
    depending of the product type, the id is parsed in different ways. This function is used to parse the id of the product
    to extract the different ids that are used in the scienti database.

    Parameters
    ----------
    product_id : str
        The id of the product.
    regex : str
        The regex to be used to parse the id.
    values : list
        The values to be extracted from the id.
    """
    match = search(regex, product_id)
    ids = {}
    if match:
        for i, value in enumerate(values):
            ids[value] = match.group(i + 1)
    return ids


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
            # Extracting COD_RH and other ids depending on the format of the id and entity in scienti
            # the products with 3 - are the next ones:
            # yuku> db.gruplac_production_data.distinct("nme_tipologia_pd",{id_producto_pd: {$regex:/-\d{9,11}-\d{1,7}-\d{1,7}/}})
            # [
            # 'Obras o productos de arte, arquitectura y diseño',
            # 'Patente de invención',
            # 'Patente modelo de utilidad',
            # 'Registro general',
            # 'Registros de acuerdos de licencia para la explotación de obras',
            # 'Secreto empresarial'
            # ]

            if reg["nme_tipologia_pd"] in ['Obras o productos de arte, arquitectura y diseño']:
                ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})', [
                                "COD_RH", "COD_PRODUCTO", "SEQ_PRODUCTO"])
                if ids:
                    entry["external_ids"].append(
                        {"provenance": "minciencias", "source": "scienti", "id": ids})
            elif reg["nme_tipologia_pd"] in ['Registro general', 'Registros de acuerdos de licencia para la explotación de obras']:
                ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})', [
                                "COD_RH", "COD_PRODUCTO", "COD_REGISTRO"])
                if ids:
                    entry["external_ids"].append(
                        {"provenance": "minciencias", "source": "scienti", "id": ids})
            elif reg["nme_tipologia_pd"] in ['Secreto empresarial']:
                ids = parse_ids(reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})-(\d{1,7})', [
                                "COD_RH", "COD_PRODUCTO", "COD_SECRETO_INDUSTRIAL"])
                if ids:
                    entry["external_ids"].append(
                        {"provenance": "minciencias", "source": "scienti", "id": ids})
            else:
                ids = parse_ids(
                    reg["id_producto_pd"], r'(\d{9,11})-(\d{1,7})', ["COD_RH", "COD_PRODUCTO"])
                if ids:
                    entry["external_ids"].append(
                        {"provenance": "minciencias", "source": "scienti", "id": ids})
    date = ""
    if "ano_convo" in reg.keys():
        if reg["ano_convo"]:
            date = check_date_format(reg["ano_convo"])

    if "id_tipo_pd_med" in reg.keys():
        if reg["id_tipo_pd_med"]:
            entry["ranking"].append(
                {"provenance": "minciencias", "date": date, "rank": reg["id_tipo_pd_med"], "source": "minciencias"})
    if "nme_tipo_medicion_pd" in reg.keys():
        if reg["nme_tipo_medicion_pd"]:
            entry["ranking"].append(
                {"provenance": "minciencias", "date": date, "rank": reg["nme_tipo_medicion_pd"], "level": 0, "source": "minciencias"})
    if "nme_categoria_pd" in reg.keys():
        if reg["nme_categoria_pd"]:
            entry["ranking"].append(
                {"provenance": "minciencias", "date": date, "rank": reg["nme_categoria_pd"], "level": 1, "source": "minciencias"})

    if "nme_tipologia_pd" in reg.keys():
        if reg["nme_tipologia_pd"]:
            typ = reg["nme_tipologia_pd"]
            entry["types"].append(
                {"provenance": "minciencias", "source": "minciencias", "type": typ, "level": 1, "parent": reg["nme_clase_pd"]})
    if "nme_clase_pd" in reg.keys():
        if reg["nme_clase_pd"]:
            typ = reg["nme_clase_pd"]
            entry["types"].append(
                {"provenance": "minciencias", "source": "minciencias", "type": typ, "level": 0, "parent": None
                 })

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
            "affiliations": [affiliation[0]] if affiliation else [],
            "external_ids": [{"provenance": "minciencias", "source": "scienti", "id": {"COD_RH": minciencias_id}}]
        }
        entry["authors"] = [author_entry]
    return entry
