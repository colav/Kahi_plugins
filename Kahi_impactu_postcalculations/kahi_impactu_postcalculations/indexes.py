

def create_indexes(db):
    """
    Indexes creation required for Backend to work properly.

    Parameters
    ----------
    db : pymongo.database.Database
        Database object to create indexes on. (kahi)
    """
    db["works"].create_index({"groups.id": 1})
    db["works"].create_index({"source.id": 1})
    db["works"].create_index(
        {"citations_count.source": 1, "citations_count.count": 1})
    db["works"].create_index({"titles.source": 1, "titles.title": 1})
    db["affilliations"].create_index({"products_count": -1})
    db["person"].create_index({"products_count": -1})
    db["works"].create_index(
        {"types.source": 1, "types.type": 1, "types.code": 1})
    db["works"].create_index({"open_access.open_access_status": 1})
    # https://github.com/colav/impactu/issues/418
    db["works"].create_index(
        {"subjects.subjects.level": 1, "subjects.subjects.name": 1})
