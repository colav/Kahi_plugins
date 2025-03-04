

def get_scienti_string(work):
    """
    Get the scienti string of the work types

    Parameters:
    ----------
    work: dict
        The work dictionary with the types

    Returns:
    -------
    str
        The scienti string of the work types
    """
    t = ""
    for i, _t in enumerate(work["types"]):
        t += _t["code"] + ": " + _t["type"] + " "
    return t.strip()


def process_scienti(work, types, verbose=False):
    """
    Get the scienti type from the work and return the colav type

    Parameters:
    ----------
    work: dict
        The work from kahi
    types: pandas.DataFrame
        The types dataframe
    verbose: bool
        If True, print warnings

    Returns:
    -------
    dict
        The colav type or an empty dictionary if type is not found
    """
    t = get_scienti_string(work)
    colav_type = types[types["Tipo"] == t]["Tipo Colav"].values
    if len(colav_type) > 1 and verbose:
        print(f"WARNING: more than one type found for {t} = {colav_type}")
    if len(colav_type) == 1:
        return {"provenance": "scienti", "source": "impactu", "type": colav_type[0]}
    return {}


def process_minciencias(work, types):
    """
    Get the minciencias type from the work and return the colav type

    Parameters:
    ----------
    work: dict
        The work from kahi
    types: pandas.DataFrame
        The impactu types

    Returns:
    -------
    dict
        The colav type or an empty dictionary if type is not found
    """
    t = work["types"][0]["type"] + ": " + work["types"][1]["type"]
    colav_type = types[types["Tipo"] == t]["Tipo Colav"].values
    if len(colav_type) > 1:
        print(f"WARNING: more than one type found for {t} = {colav_type}")
    if len(colav_type) == 1:
        return {"provenance": "minciencias", "source": "impactu", "type": colav_type[0]}
    return {}


def process_others(source, types):
    """
    Allows to process other type sources such as ciarp, openalex and scholar

    Parameters:
    ----------
    source: str
        The source of the types ex: ciarp, openalex, scholar
    types: pandas.DataFrame
        The impactu types

    Returns:
    -------
    function
        The function to process the type source
    """
    def process_source(work, types):
        t = work["types"][0]["type"]
        colav_type = types[types["Tipo"] == t]["Tipo Colav"].values
        if len(colav_type) > 1:
            print(f"WARNING: more than one type found for {t} = {colav_type}")
        if len(colav_type) == 1:
            return {"provenance": source, "source": "impactu", "type": colav_type[0]}
        return {}
    return process_source


functors = {}
functors["minciencias"] = process_minciencias
functors["scienti"] = process_scienti
functors["ciarp"] = process_others("ciarp")
functors["openalex"] = process_others("openalex")
functors["scholar"] = process_others("scholar")


def process_one(db, work, source, types, verbose=True):
    """
    Process one work to get the colav type

    Parameters:
    ----------
    db: pymongo.database.Database
        The database
    work: dict
        The work from kahi
    source: str
        The source of the types ex: minciencias, scienti, ciarp, openalex, scholar
    types: pandas.DataFrame
        The impactu types
    verbose: bool
        If True, print warnings

    """
    df_filtered = types[types["Fuente"].str.contains(
        source, case=False, na=False)]
    if len(work["types"]) > 1 and (source != "minciencias" or source != "scienti") and verbose:
        print(f"WARNING: more than one type found for {source} = {work}")

    colav_type = functors[source](work, df_filtered)
    if colav_type:
        db["works"].update_one({"_id": work["_id"]},
                               {"$push": {"types": colav_type}})
    elif verbose:
        print(f"WARNING: colav type not found for {work}")
