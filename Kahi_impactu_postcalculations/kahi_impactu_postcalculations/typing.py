
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
    for _t in work.get("types", []):
        code = _t.get("code")
        type_name = _t.get("type")
        if not code or not type_name:
            continue
        t += code + ": " + type_name + " "
    return t.strip()


def build_type_lookup(types, source):
    """Build immutable O(1) mappings for one source."""
    if hasattr(types, "payload"):
        records = types.payload.get("mappings", [])
    elif isinstance(types, dict) and "mappings" in types:
        records = types["mappings"]
    elif hasattr(types, "to_dict"):
        records = types.to_dict("records")
    else:
        records = types

    by_type = {}
    impactu_types = set()
    source_key = str(source).casefold()
    for record in records:
        record_source = record.get("source", record.get("Fuente", ""))
        if source_key not in str(record_source).casefold():
            continue
        if record.get("entity", record.get("Entidad", "works")) != "works":
            continue
        type_name = record.get("type", record.get("Tipo"))
        impactu_type = record.get("type_impactu", record.get("Tipo ImpactU"))
        if not isinstance(type_name, str) or not impactu_type:
            continue
        values = by_type.setdefault(type_name, [])
        if impactu_type not in values:
            values.append(impactu_type)
        impactu_types.add(impactu_type)
    return {
        "by_type": {
            type_name: tuple(values) for type_name, values in by_type.items()
        },
        "impactu_types": frozenset(impactu_types),
    }


def _type_values(types, type_name):
    if isinstance(types, dict) and "by_type" in types:
        return types["by_type"].get(type_name, ())
    return types[types["Tipo"] == type_name]["Tipo ImpactU"].dropna().unique()


def _normalized_type(types, type_name):
    if isinstance(types, dict) and "impactu_types" in types:
        return (type_name,) if type_name in types["impactu_types"] else ()
    return types[
        types["Tipo ImpactU"] == type_name
    ]["Tipo ImpactU"].dropna().unique()


def process_scienti(work, types, verbose=False):
    """
    Get the scienti type from the work and return the impactu type

    Parameters:
    ----------
    work: dict
        The work from kahi
    types: dict or sequence
        The precompiled lookup or catalog mappings
    verbose: bool
        If True, print warnings

    Returns:
    -------
    dict
        The impactu type or an empty dictionary if type is not found
    """
    t = get_scienti_string(work)
    impactu_type = _type_values(types, t)
    if len(impactu_type) > 1 and verbose:
        print(f"WARNING: more than one type found for {t} = {impactu_type}")
    if len(impactu_type) == 1:
        return {
            "provenance": "scienti",
            "source": "impactu",
            "type": impactu_type[0],
        }
    return {}


def process_minciencias(work, types, verbose=False):
    """
    Get the minciencias type from the work and return the impactu type

    Parameters:
    ----------
    work: dict
        The work from kahi
    types: dict or sequence
        The precompiled lookup or catalog mappings

    Returns:
    -------
    dict
        The impactu type or an empty dictionary if type is not found
    """
    type_names = [
        item.get("type")
        for item in work.get("types", [])
        if item.get("type")
    ]
    if not type_names:
        return {}

    # Minciencias commonly provides a hierarchy with two levels, but some
    # valid products contain only the most specific type. Try the original
    # two-level representation first and then each available type.
    candidates = []
    if len(type_names) >= 2:
        candidates.append(f"{type_names[0]}: {type_names[1]}")
    candidates.extend(reversed(type_names))

    impactu_type = []
    t = candidates[0]
    for candidate in dict.fromkeys(candidates):
        matches = _type_values(types, candidate)
        if len(matches):
            t = candidate
            impactu_type = matches
            break
    if not len(impactu_type):
        # Some one-level records already contain the normalized ImpactU type
        # rather than the original two-level Minciencias hierarchy.
        for candidate in dict.fromkeys(candidates):
            normalized_matches = _normalized_type(types, candidate)
            if len(normalized_matches) == 1:
                t = candidate
                impactu_type = normalized_matches
                break
    if len(impactu_type) > 1 and verbose:
        print(f"WARNING: more than one type found for {t} = {impactu_type}")
    if len(impactu_type) == 1:
        return {
            "provenance": "minciencias",
            "source": "impactu",
            "type": impactu_type[0],
        }
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
    def process_source(work, types, verbose=False):
        """
        Functor to process the type source

        Parameters:
        ----------
        work: dict
            The work from kahi
        types: dict or sequence
            The precompiled lookup or catalog mappings

        Returns:
        -------
        dict
            The impactu type or an empty dictionary if type is not found
        """
        type_names = [
            item.get("type")
            for item in work.get("types", [])
            if item.get("type")
        ]
        if not type_names:
            return {}
        t = type_names[0]
        impactu_type = _type_values(types, t)
        if len(impactu_type) > 1 and verbose:
            print(
                f"WARNING: more than one type found for {t} = {impactu_type}")
        if len(impactu_type) == 1:
            return {
                "provenance": source,
                "source": "impactu",
                "type": impactu_type[0],
            }
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
functors["crossref"] = process_others("crossref")


def process_type(db, work, source, types, verbose=False):
    """
    Process one work to get the impactu type

    Parameters:
    ----------
    db: pymongo.database.Database
        The database
    work: dict
        The work from kahi
    source: str
        Type source, for example minciencias, scienti, ciarp, or openalex
    types: dict or sequence
        The precompiled lookup or catalog mappings
    verbose: bool
        If True, print warnings

    """
    if isinstance(types, dict) and "by_type" in types:
        source_types = types
    else:
        source_types = build_type_lookup(types, source)
    if (
        len(work["types"]) > 1
        and source not in ("minciencias", "scienti")
        and verbose
    ):
        print(f"WARNING: more than one type found for {source} = {work}")

    impactu_type = functors[source](work, source_types, verbose=verbose)
    if impactu_type:
        db["works"].update_one(
            {"_id": work["_id"]},
            {"$addToSet": {"types": impactu_type}},
        )
    elif verbose:
        print(f"WARNING: impactu type not found for {work}")
