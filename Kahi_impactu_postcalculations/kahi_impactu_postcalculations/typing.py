
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
    Get the scienti type from the work and return the impactu type

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
        The impactu type or an empty dictionary if type is not found
    """
    t = get_scienti_string(work)
    impactu_type = types[types["Tipo"] == t]["Tipo ImpactU"].values
    if len(impactu_type) > 1 and verbose:
        print(f"WARNING: more than one type found for {t} = {impactu_type}")
    if len(impactu_type) == 1:
        return {"provenance": "scienti", "source": "impactu", "type": impactu_type[0]}
    return {}


def process_minciencias(work, types):
    """
    Get the minciencias type from the work and return the impactu type

    Parameters:
    ----------
    work: dict
        The work from kahi
    types: pandas.DataFrame
        The impactu types

    Returns:
    -------
    dict
        The impactu type or an empty dictionary if type is not found
    """
    t = work["types"][0]["type"] + ": " + work["types"][1]["type"]
    impactu_type = types[types["Tipo"] == t]["Tipo ImpactU"].values
    if len(impactu_type) > 1:
        print(f"WARNING: more than one type found for {t} = {impactu_type}")
    if len(impactu_type) == 1:
        return {"provenance": "minciencias", "source": "impactu", "type": impactu_type[0]}
    return {}


def process_others(source):
    """
    Allows to process other type sources such as ciarp, openalex and scholar

    Parameters:
    ----------
    source: str
        The source of the types ex: ciarp, openalex, scholar

    Returns:
    -------
    function
        The function to process the type source
    """
    def process_source(work, types):
        """
        Functor to process the type source

        Parameters:
        ----------
        work: dict
            The work from kahi
        types: pandas.DataFrame
            The impactu types

        Returns:
        -------
        dict
            The impactu type or an empty dictionary if type is not found
        """
        t = work["types"][0]["type"]
        impactu_type = types[types["Tipo"] == t]["Tipo ImpactU"].values
        if len(impactu_type) > 1:
            print(
                f"WARNING: more than one type found for {t} = {impactu_type}")
        if len(impactu_type) == 1:
            return {"provenance": source, "source": "impactu", "type": impactu_type[0]}
        return {}
    return process_source


functors = {}
functors["minciencias"] = process_minciencias
functors["scienti"] = process_scienti
functors["ciarp"] = process_others("ciarp")
functors["coar"] = process_others("coar")
functors["redcol"] = process_others("redcol")
functors["eu-repo"] = process_others("eu-repo")
functors["openalex"] = process_others("openalex")
functors["scholar"] = process_others("scholar")


def process_type(db, work, source, types, verbose=True):
    """
    Process one work to get the impactu type

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

    impactu_type = functors[source](work, df_filtered)
    if impactu_type:
        db["works"].update_one({"_id": work["_id"]},
                               {"$push": {"types": impactu_type}})
    elif verbose:
        print(f"WARNING: impactu type not found for {work}")
