

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
    db["affiliations"].create_index({"products_count": -1})
    db["person"].create_index({"products_count": -1})
    db["works"].create_index(
        {"types.source": 1, "types.type": 1, "types.code": 1})
    db["works"].create_index({"open_access.open_access_status": 1})
    # https://github.com/colav/impactu/issues/418
    db["works"].create_index(
        {"subjects.subjects.level": 1, "subjects.subjects.name": 1})
    # https://github.com/colav/impactu/issues/595
    db["works"].create_index({"citations_count_openalex": 1})
    db["affiliations"].create_index({"citations_count_openalex": 1})
    db["works"].create_index({"open_access.is_open_access": 1})
    db["sources"].create_index({
        "citations_count.source": 1,
        "citations_count.count": 1
    })
    db["sources"].create_index({"products_count": -1, "_id": 1})
    db["sources"].create_index({"global_products_count": -1, "_id": 1})
    db["sources"].create_index({"global_citations_count": -1, "_id": 1})
    db["sources"].create_index({"names.name": 1, "_id": 1})
    db["sources"].create_index({"names.name": "text"})
    db["sources"].create_index({"types.type": 1})
    db["sources"].create_index({"publisher.country_code": 1})
    db["sources"].create_index({
        "external_ids.source": 1,
        "external_ids.id": 1
    })
    db["sources"].create_index({"subjects.name": 1})
    db["sources"].create_index({
        "ranking.source": 1,
        "ranking.to_date": -1,
        "ranking.rank": 1
    })
    db["sources"].create_index({"_id": 1, "ranking": 1})
    db["sources"].create_index({"open_access_status": 1})
    db["sources"].create_index({"scimago_best_quartile": 1})
    db["sources"].create_index({"apc.apc_usd": 1})
    db["sources"].create_index({"open_access_start_year": 1})
    db["sources"].create_index({"apc.charges": 1})
    db["sources"].create_index({"publication_time_weeks": 1})
    db["sources"].create_index({"licenses.type": 1})
    db["sources"].create_index({"topics.id": 1})
