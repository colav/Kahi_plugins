from joblib import Parallel, delayed


PARALLEL_SAFE_COLLECTIONS = {"sources", "person", "affiliations"}
WORKS_DATES_CHUNK_BUCKETS = 16
WORKS_DATES_CHUNK_WORKERS = 4
WORKS_DATES_MIN_DOCS_TO_CHUNK = 100000


def _build_objectid_ranges(collection, match_query, buckets):
    """
    Build contiguous _id ranges using bucketAuto over matching documents.
    """
    return list(collection.aggregate(
        [
            {"$match": match_query},
            {
                "$bucketAuto": {
                    "groupBy": "$_id",
                    "buckets": buckets,
                    "output": {
                        "min_id": {"$min": "$_id"},
                        "max_id": {"$max": "$_id"},
                        "count": {"$sum": 1},
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "min_id": 1,
                    "max_id": 1,
                    "count": 1,
                }
            },
            {"$sort": {"min_id": 1}},
        ],
        allowDiskUse=True,
    ))


def _run_chunked_aggregate_by_id(
    collection,
    base_match,
    pipeline_tail,
    pipeline_name,
    chunk_buckets,
    chunk_workers,
    min_docs_to_chunk,
):
    """
    Run one aggregation pipeline in parallel chunks over non-overlapping _id ranges.
    """
    docs_to_process = collection.count_documents(base_match)
    if docs_to_process == 0:
        print(f"INFO: {pipeline_name}: no matching docs")
        return

    full_pipeline = [{"$match": base_match}] + pipeline_tail
    if docs_to_process < min_docs_to_chunk or chunk_buckets <= 1 or chunk_workers <= 1:
        print(
            f"INFO: {pipeline_name}: running sequentially "
            f"for {docs_to_process} docs"
        )
        collection.aggregate(full_pipeline, allowDiskUse=True)
        return

    ranges = _build_objectid_ranges(collection, base_match, chunk_buckets)
    if len(ranges) <= 1:
        print(
            f"INFO: {pipeline_name}: single range detected, "
            "running sequentially"
        )
        collection.aggregate(full_pipeline, allowDiskUse=True)
        return

    workers = min(chunk_workers, len(ranges))
    print(
        f"INFO: {pipeline_name}: running in {len(ranges)} chunks "
        f"with {workers} workers ({docs_to_process} docs)"
    )

    def _run_one_range(range_info):
        range_match = {
            "$and": [
                base_match,
                {
                    "_id": {
                        "$gte": range_info["min_id"],
                        "$lte": range_info["max_id"],
                    }
                },
            ]
        }
        collection.aggregate(
            [{"$match": range_match}] + pipeline_tail,
            allowDiskUse=True,
        )

    Parallel(n_jobs=workers, prefer="threads", backend="threading")(
        delayed(_run_one_range)(range_info)
        for range_info in ranges
    )


def set_works_authors_affiliations_country(collection) -> None:
    """
    Method to set the country of the affiliations of the authors of the works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "authors.affiliations.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "authors.affiliations.id",
                "foreignField": "_id",
                "as": "affiliations_data",
                "pipeline": [{"$project": {"_id": 1, "addresses.country": 1}}],
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": "$$author.affiliations",
                                            "as": "affiliation",
                                            "in": {
                                                "$mergeObjects": [
                                                    "$$affiliation",
                                                    {
                                                        "country": {
                                                            "$let": {
                                                                "vars": {
                                                                    "matchedAffiliation": {
                                                                        "$arrayElemAt": [
                                                                            {
                                                                                "$filter": {
                                                                                    "input": "$affiliations_data",
                                                                                    "as": "affiliation_data",
                                                                                    "cond": {
                                                                                        "$eq": [
                                                                                            "$$affiliation.id",
                                                                                            "$$affiliation_data._id",
                                                                                        ]
                                                                                    },
                                                                                }
                                                                            },
                                                                            0,
                                                                        ]
                                                                    }
                                                                },
                                                                "in": {
                                                                    "$ifNull": [
                                                                        {
                                                                            "$arrayElemAt": [
                                                                                "$$matchedAffiliation.addresses.country",
                                                                                0,
                                                                            ]
                                                                        },
                                                                        None,
                                                                    ]
                                                                },
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"affiliations_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)  # works collections


def set_works_authors_affiliations_country_code(collection) -> None:
    """
    Method to set the country code of the affiliations of the authors of the works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "authors.affiliations.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "authors.affiliations.id",
                "foreignField": "_id",
                "as": "affiliations_data",
                "pipeline": [{"$project": {"_id": 1, "addresses.country_code": 1}}],
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": "$$author.affiliations",
                                            "as": "affiliation",
                                            "in": {
                                                "$mergeObjects": [
                                                    "$$affiliation",
                                                    {
                                                        "country_code": {
                                                            "$let": {
                                                                "vars": {
                                                                    "matchedAffiliation": {
                                                                        "$arrayElemAt": [
                                                                            {
                                                                                "$filter": {
                                                                                    "input": "$affiliations_data",
                                                                                    "as": "affiliation_data",
                                                                                    "cond": {
                                                                                        "$eq": [
                                                                                            "$$affiliation.id",
                                                                                            "$$affiliation_data._id",
                                                                                        ]
                                                                                    },
                                                                                }
                                                                            },
                                                                            0,
                                                                        ]
                                                                    }
                                                                },
                                                                "in": {
                                                                    "$ifNull": [
                                                                        {
                                                                            "$arrayElemAt": [
                                                                                "$$matchedAffiliation.addresses.country_code",
                                                                                0,
                                                                            ]
                                                                        },
                                                                        None,
                                                                    ]
                                                                },
                                                            }
                                                        }
                                                    },
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"affiliations_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)


def set_works_groups_ranking(collection) -> None:
    """
    Function to set the ranking of the groups of the works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "groups.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "groups.id",
                "foreignField": "_id",
                "as": "groups_data",
                "pipeline": [{"$match": {"ranking.source": "minciencias"}}, {"$project": {"_id": 1, "ranking": 1}}],
            }
        },
        {
            "$addFields": {
                "groups": {
                    "$map": {
                        "input": "$groups",
                        "as": "group",
                        "in": {
                            "$mergeObjects": [
                                "$$group",
                                {
                                    "ranking": {
                                        "$let": {
                                            "vars": {
                                                "matchedGroup": {
                                                    "$arrayElemAt": [
                                                        {
                                                            "$filter": {
                                                                "input": "$groups_data",
                                                                "as": "group_data",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$group.id",
                                                                        "$$group_data._id",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        0,
                                                    ]
                                                }
                                            },
                                            "in": {
                                                "$ifNull": [
                                                    {
                                                        "$arrayElemAt": [
                                                            {
                                                                "$map": {
                                                                    "input": {
                                                                        "$filter": {
                                                                            "input": "$$matchedGroup.ranking",
                                                                            "as": "rankData",
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$rankData.source",
                                                                                    "minciencias",
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    "as": "filteredRank",
                                                                    "in": "$$filteredRank.rank",
                                                                }
                                                            },
                                                            0,
                                                        ]
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"groups_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)


def set_works_authors_ranking(collection) -> None:
    """
    Function to set the ranking of the authors

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "authors.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "person",
                "localField": "authors.id",
                "foreignField": "_id",
                "as": "authors_data",
                "pipeline": [{"$match": {"ranking.source": "minciencias"}}, {"$project": {"_id": 1, "ranking": 1}}],
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "ranking": {
                                        "$let": {
                                            "vars": {
                                                "matchedAuthor": {
                                                    "$arrayElemAt": [
                                                        {
                                                            "$filter": {
                                                                "input": "$authors_data",
                                                                "as": "author_data",
                                                                "cond": {
                                                                    "$eq": [
                                                                        "$$author.id",
                                                                        "$$author_data._id",
                                                                    ]
                                                                },
                                                            }
                                                        },
                                                        0,
                                                    ]
                                                }
                                            },
                                            "in": {
                                                "$ifNull": [
                                                    {
                                                        "$arrayElemAt": [
                                                            {
                                                                "$map": {
                                                                    "input": {
                                                                        "$filter": {
                                                                            "input": "$$matchedAuthor.ranking",
                                                                            "as": "rankData",
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$rankData.source",
                                                                                    "minciencias",
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    "as": "filteredRank",
                                                                    "in": "$$filteredRank.rank",
                                                                }
                                                            },
                                                            0,
                                                        ]
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$project": {"authors_data": 0}},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "fail",
            }
        },
    ]
    collection.aggregate(pipeline)


def set_works_citations_count_openalex(collection) -> None:
    """
    Function to set the OpenAlex citations count in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$set": {
                "citations_count_openalex": {
                    "$ifNull": [
                        {
                            "$getField": {
                                "field": "count",
                                "input": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$citations_count",
                                            "as": "c",
                                            "cond": {
                                                "$eq": ["$$c.source", "openalex"]
                                            },
                                        }
                                    }
                                },
                            }
                        },
                        0,
                    ]
                }
            }
        }
    ]

    collection.update_many(
        {
            "$or": [
                {"citations_count.source": "openalex"},
                {"citations_count_openalex": {"$exists": False}},
            ]
        },
        pipeline,
    )


def set_works_authors_full_data(collection) -> None:
    """
    Function to enrich works authors with full person data

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "authors.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "person",
                "localField": "authors.id",
                "foreignField": "_id",
                "as": "authors_data",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "id": "$_id",
                            "sex": 1,
                            "full_name": 1,
                            "first_names": 1,
                            "last_names": 1,
                            "ranking": 1,
                            "external_ids": 1,
                        }
                    }
                ],
            }
        },
        {
            "$addFields": {
                "authors_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$authors_data",
                            "as": "a",
                            "in": {"k": "$$a.id", "v": "$$a"},
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "$ifNull": [
                                        {
                                            "$getField": {
                                                "field": "$$author.id",
                                                "input": "$authors_map",
                                            }
                                        },
                                        {},
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$unset": ["authors_data", "authors_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_authors_affiliations_dates(collection) -> None:
    """
    Function to set authors affiliations start and end dates in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    base_match = {
        "authors.affiliations.id": {"$exists": True}
    }
    pipeline_tail = [
        {
            "$lookup": {
                "from": "person",
                "let": {"authorIds": "$authors.id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$_id", "$$authorIds"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id": "$_id",
                            "affiliations.id": 1,
                            "affiliations.start_date": 1,
                            "affiliations.end_date": 1,
                        }
                    },
                ],
                "as": "authors_data",
            }
        },
        {
            "$addFields": {
                "authors_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$authors_data",
                            "as": "a",
                            "in": {"k": "$$a.id", "v": "$$a"},
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$let": {
                                "vars": {
                                    "personData": {
                                        "$getField": {
                                            "field": "$$author.id",
                                            "input": "$authors_map",
                                        }
                                    }
                                },
                                "in": {
                                    "$mergeObjects": [
                                        "$$author",
                                        {
                                            "affiliations": {
                                                "$map": {
                                                    "input": {
                                                        "$ifNull": [
                                                            "$$author.affiliations",
                                                            [],
                                                        ]
                                                    },
                                                    "as": "aff",
                                                    "in": {
                                                        "$let": {
                                                            "vars": {
                                                                "matchAff": {
                                                                    "$first": {
                                                                        "$filter": {
                                                                            "input": {
                                                                                "$ifNull": [
                                                                                    "$$personData.affiliations",
                                                                                    [],
                                                                                ]
                                                                            },
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$this.id",
                                                                                    "$$aff.id",
                                                                                ]
                                                                            },
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "in": {
                                                                "$mergeObjects": [
                                                                    "$$aff",
                                                                    {
                                                                        "start_date": {
                                                                            "$ifNull": [
                                                                                "$$matchAff.start_date",
                                                                                "$$aff.start_date",
                                                                            ]
                                                                        },
                                                                        "end_date": {
                                                                            "$ifNull": [
                                                                                "$$matchAff.end_date",
                                                                                "$$aff.end_date",
                                                                            ]
                                                                        },
                                                                    },
                                                                ]
                                                            },
                                                        }
                                                    },
                                                }
                                            }
                                        },
                                    ]
                                },
                            }
                        },
                    }
                }
            }
        },
        {"$unset": ["authors_data", "authors_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    _run_chunked_aggregate_by_id(
        collection=collection,
        base_match=base_match,
        pipeline_tail=pipeline_tail,
        pipeline_name="set_works_authors_affiliations_dates",
        chunk_buckets=WORKS_DATES_CHUNK_BUCKETS,
        chunk_workers=WORKS_DATES_CHUNK_WORKERS,
        min_docs_to_chunk=WORKS_DATES_MIN_DOCS_TO_CHUNK,
    )


def set_works_source_full_data(collection) -> None:
    """
    Function to enrich works source with full source data

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "source.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "sources",
                "localField": "source.id",
                "foreignField": "_id",
                "as": "src",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "names": 1,
                            "types": 1,
                            "external_ids": 1,
                            "updated": 1,
                            "publisher": 1,
                            "ranking": 1,
                            "apc": 1,
                            "external_urls": 1,
                        }
                    }
                ],
            }
        },
        {"$unwind": "$src"},
        {
            "$set": {
                "source": {
                    "$mergeObjects": ["$source", "$src"]
                }
            }
        },
        {"$unset": "src"},
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_person_affiliations_relations(collection) -> None:
    """
    Function to set relations inside affiliations for person collection

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the persons are stored
    """
    pipeline = [
        {
            "$unwind": {
                "path": "$affiliations",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "localField": "affiliations.id",
                "foreignField": "_id",
                "as": "aff_doc",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "relations.id": 1,
                        }
                    }
                ],
            }
        },
        {
            "$unwind": {
                "path": "$aff_doc",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$addFields": {
                "affiliations.relations": "$aff_doc.relations"
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "doc": {"$first": "$$ROOT"},
                "affiliations": {"$push": "$affiliations"},
            }
        },
        {
            "$addFields": {
                "affiliations": {
                    "$filter": {
                        "input": "$affiliations",
                        "as": "a",
                        "cond": {
                            "$and": [
                                {"$ne": ["$$a", None]},
                                {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$objectToArray": "$$a"
                                            }
                                        },
                                        0,
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        "$doc",
                        {"affiliations": "$affiliations"},
                    ]
                }
            }
        },
        {
            "$project": {
                "aff_doc": 0,
                "doc": 0,
            }
        },
        {
            "$merge": {
                "into": "person",
                "whenMatched": "replace",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_authors_affiliations_external_data(collection) -> None:
    """
    Function to enrich works authors affiliations with external_ids and addresses data

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "authors.affiliations.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "let": {
                    "aff_ids": {
                        "$reduce": {
                            "input": "$authors",
                            "initialValue": [],
                            "in": {
                                "$setUnion": [
                                    "$$value",
                                    "$$this.affiliations.id",
                                ]
                            },
                        }
                    }
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$_id", "$$aff_ids"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "id": "$_id",
                            "_id": 0,
                            "external_ids": 1,
                            "addresses": {
                                "$map": {
                                    "input": {
                                        "$ifNull": ["$addresses", []]
                                    },
                                    "as": "addr",
                                    "in": {
                                        "latitude": "$$addr.lat",
                                        "longitude": "$$addr.lng",
                                        "city": "$$addr.city",
                                        "country": "$$addr.country",
                                        "country_code": "$$addr.country_code",
                                    },
                                }
                            },
                        }
                    },
                ],
                "as": "aff_data",
            }
        },
        {
            "$addFields": {
                "aff_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$aff_data",
                            "as": "a",
                            "in": {
                                "k": "$$a.id",
                                "v": "$$a",
                            },
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": {
                                                "$ifNull": [
                                                    "$$author.affiliations",
                                                    [],
                                                ]
                                            },
                                            "as": "aff",
                                            "in": {
                                                "$let": {
                                                    "vars": {
                                                        "affData": {
                                                            "$getField": {
                                                                "field": "$$aff.id",
                                                                "input": "$aff_map",
                                                            }
                                                        }
                                                    },
                                                    "in": {
                                                        "$mergeObjects": [
                                                            "$$aff",
                                                            {
                                                                "external_ids": {
                                                                    "$ifNull": [
                                                                        "$$affData.external_ids",
                                                                        "$$aff.external_ids",
                                                                    ]
                                                                },
                                                                "addresses": {
                                                                    "$ifNull": [
                                                                        "$$affData.addresses",
                                                                        "$$aff.addresses",
                                                                    ]
                                                                },
                                                            },
                                                        ]
                                                    },
                                                }
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$unset": ["aff_data", "aff_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_groups_citations_count(collection) -> None:
    """
    Function to set citations count for groups inside works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "groups": {
                    "$exists": True,
                    "$ne": [],
                }
            }
        },
        {"$unwind": "$groups"},
        {
            "$lookup": {
                "from": "affiliations",
                "let": {
                    "groupId": "$groups.id",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$_id", "$$groupId"],
                            }
                        }
                    },
                    {
                        "$match": {
                            "types.type": "group",
                        }
                    },
                    {
                        "$project": {
                            "citations_count": 1,
                            "_id": 0,
                        }
                    },
                ],
                "as": "affiliation",
            }
        },
        {
            "$unwind": {
                "path": "$affiliation",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$addFields": {
                "groups.citations_count": "$affiliation.citations_count"
            }
        },
        {
            "$project": {
                "affiliation": 0
            }
        },
        {
            "$group": {
                "_id": "$_id",
                "groups": {"$push": "$groups"},
                "doc": {"$first": "$$ROOT"},
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [
                        "$doc",
                        {"groups": "$groups"},
                    ]
                }
            }
        },
        {
            "$merge": {
                "into": "works",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def set_works_groups_ranking_to_works_collection(collection) -> None:
    """
    Function to set ranking data for groups inside works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$match": {
                "groups.id": {"$exists": True}
            }
        },
        {
            "$lookup": {
                "from": "affiliations",
                "let": {
                    "group_ids": {
                        "$map": {
                            "input": "$groups",
                            "as": "g",
                            "in": "$$g.id",
                        }
                    }
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$in": ["$_id", "$$group_ids"]
                            }
                        }
                    },
                    {
                        "$project": {
                            "id": "$_id",
                            "_id": 0,
                            "ranking": 1,
                        }
                    },
                ],
                "as": "group_rankings",
            }
        },
        {
            "$addFields": {
                "group_rank_map": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$group_rankings",
                            "as": "gr",
                            "in": {
                                "k": "$$gr.id",
                                "v": {
                                    "$ifNull": [
                                        "$$gr.ranking",
                                        [],
                                    ]
                                },
                            },
                        }
                    }
                }
            }
        },
        {
            "$addFields": {
                "groups": {
                    "$map": {
                        "input": "$groups",
                        "as": "g",
                        "in": {
                            "$mergeObjects": [
                                "$$g",
                                {
                                    "ranking": {
                                        "$ifNull": [
                                            {
                                                "$getField": {
                                                    "field": "$$g.id",
                                                    "input": "$group_rank_map",
                                                }
                                            },
                                            [],
                                        ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$unset": ["group_rankings", "group_rank_map"]},
        {
            "$merge": {
                "into": "works",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    collection.aggregate(pipeline)


def clean_works_authors_affiliations_country_fields(collection) -> None:
    """
    Function to remove country and country_code fields from authors affiliations in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$set": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "affiliations": {
                                        "$map": {
                                            "input": {
                                                "$ifNull": [
                                                    "$$author.affiliations",
                                                    [],
                                                ]
                                            },
                                            "as": "aff",
                                            "in": {
                                                "$arrayToObject": {
                                                    "$filter": {
                                                        "input": {
                                                            "$objectToArray": "$$aff"
                                                        },
                                                        "as": "field",
                                                        "cond": {
                                                            "$not": {
                                                                "$in": [
                                                                    "$$field.k",
                                                                    [
                                                                        "country",
                                                                        "country_code",
                                                                    ],
                                                                ]
                                                            }
                                                        },
                                                    }
                                                }
                                            },
                                        }
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    ]

    collection.update_many(
        {
            "$or": [
                {"authors.affiliations.country": {"$exists": True}},
                {"authors.affiliations.country_code": {"$exists": True}},
            ]
        },
        pipeline,
    )


def normalize_works_authors_ranking_empty_list(collection) -> None:
    """
    Function to replace null authors ranking with empty list in works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the works are stored
    """
    pipeline = [
        {
            "$set": {
                "authors": {
                    "$map": {
                        "input": "$authors",
                        "as": "author",
                        "in": {
                            "$mergeObjects": [
                                "$$author",
                                {
                                    "ranking": {
                                        "$cond": [
                                            {
                                                "$eq": [
                                                    "$$author.ranking",
                                                    None,
                                                ]
                                            },
                                            [],
                                            "$$author.ranking",
                                        ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    ]

    collection.update_many(
        {"authors.ranking": None},
        pipeline,
    )


def set_affiliations_citations_count_openalex(collection) -> None:
    """
    Function to set the OpenAlex citations count in affiliations

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the affiliations are stored
    """
    pipeline = [
        {
            "$set": {
                "citations_count_openalex": {
                    "$ifNull": [
                        {
                            "$getField": {
                                "field": "count",
                                "input": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$citations_count",
                                            "as": "c",
                                            "cond": {
                                                "$eq": [
                                                    "$$c.source",
                                                    "openalex",
                                                ]
                                            },
                                        }
                                    }
                                },
                            }
                        },
                        0,
                    ]
                }
            }
        }
    ]

    collection.update_many({}, pipeline)


def set_sources_products_count(collection) -> None:
    """
    Function to set products count in sources from works

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    works_collection = (
        collection.database["works"]
        if collection.name == "sources"
        else collection
    )
    pipeline = [
        {
            "$set": {
                "_source_ids": {
                    "$cond": [
                        {"$isArray": "$source.id"},
                        "$source.id",
                        ["$source.id"],
                    ]
                }
            }
        },
        {"$unwind": "$_source_ids"},
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$ne": ["$_source_ids", None]},
                        {"$not": [{"$isArray": "$_source_ids"}]},
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$_source_ids",
                "products_count": {"$sum": 1},
            }
        },
        {
            "$merge": {
                "into": "sources",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    works_collection.aggregate(pipeline)


def normalize_sources_products_count(collection) -> None:
    """
    Function to set default products_count to 0 in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    collection.update_many(
        {"products_count": {"$exists": False}},
        {"$set": {"products_count": 0}},
    )


def set_sources_citations_count_openalex(collection) -> None:
    """
    Function to set total OpenAlex citations count in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    works_collection = (
        collection.database["works"]
        if collection.name == "sources"
        else collection
    )
    pipeline = [
        {
            "$match": {
                "citations_count": {
                    "$elemMatch": {"source": "openalex"}
                }
            }
        },
        {
            "$project": {
                "source.id": 1,
                "citations_count": 1,
            }
        },
        {"$unwind": "$citations_count"},
        {
            "$match": {
                "citations_count.source": "openalex"
            }
        },
        {
            "$set": {
                "_source_ids": {
                    "$cond": [
                        {"$isArray": "$source.id"},
                        "$source.id",
                        ["$source.id"],
                    ]
                }
            }
        },
        {"$unwind": "$_source_ids"},
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$ne": ["$_source_ids", None]},
                        {"$not": [{"$isArray": "$_source_ids"}]},
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$_source_ids",
                "total_citations": {
                    "$sum": "$citations_count.count"
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "citations_count": [
                    {
                        "source": "openalex",
                        "count": "$total_citations",
                    }
                ],
            }
        },
        {
            "$merge": {
                "into": "sources",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }
        },
    ]

    works_collection.aggregate(pipeline)


def normalize_sources_citations_count(collection) -> None:
    """
    Function to set default OpenAlex citations_count in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    collection.update_many(
        {"citations_count": {"$exists": False}},
        {
            "$set": {
                "citations_count": [
                    {
                        "source": "openalex",
                        "count": 0,
                    }
                ]
            }
        },
    )


def normalize_sources_global_counts(collection) -> None:
    """
    Function to set default global counts in sources

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection
    """
    collection.update_many(
        {"global_products_count": {"$exists": False}},
        {"$set": {"global_products_count": 0}},
    )
    collection.update_many(
        {"global_citations_count": {"$exists": False}},
        {"$set": {"global_citations_count": 0}},
    )


def clean_person_empty_affiliations_array(collection) -> None:
    """
    Function to replace affiliations equal to [{}]
    with an empty list in person collection

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the persons are stored
    """
    collection.update_many(
        {"affiliations": [{}]},
        {"$set": {"affiliations": []}},
    )


def normalize_source_apc_usd(collection) -> None:
    """
    Function to normalize APC charges to USD in the sources collection.

    Uses a MongoDB aggregation pipeline to compute APC values in USD
    based on existing APC charges and currencies for each source.

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Sources collection where APC information is stored.
    """
    collection.aggregate([
        {
            "$addFields": {
                "apc.apc_usd": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$ne": [
                                        {"$type": "$apc.charges"},
                                        "missing"
                                    ]
                                },
                                {
                                    "$ne": [
                                        {"$type": "$apc.currency"},
                                        "missing"
                                    ]
                                },
                                {"$ne": ["$apc.charges", None]},
                                {"$ne": ["$apc.currency", None]}
                            ]
                        },
                        "then": {
                            "$round": [
                                {
                                    "$switch": {
                                        "branches": [
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "ARS"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        1484.75
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "AUD"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        1.540433
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "BDT"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        122.041105
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "BRL"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        5.384517
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "CAD"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        1.403056
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "CHF"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        0.795967
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "CNY"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        7.093
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "CZK"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        20.94248
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "EGP"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        47.544911
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "EUR"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        0.861265
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "GBP"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        0.747696
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "IDR"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        16582.079612
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "INR"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        88.029318
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "IQD"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        1310.124452
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "IRR"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        42443.577735
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "JPY"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        151.513219
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "KRW"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        1430.007464
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "MXN"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        18.436318
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "NGN"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        1464.430761
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "PEN"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        3.382431
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "PKR"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        283.341893
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "PLN"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        3.653606
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "RON"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        4.377858
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "RSD"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        100.941411
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "RUB"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        81.334585
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "TRY"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        41.977679
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "UAH"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        41.762775
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "USD"
                                                    ]
                                                },
                                                "then": "$apc.charges"
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "ZAR"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        17.380682
                                                    ]
                                                }
                                            },
                                            {
                                                "case": {
                                                    "$eq": [
                                                        "$apc.currency",
                                                        "XOF"
                                                    ]
                                                },
                                                "then": {
                                                    "$divide": [
                                                        "$apc.charges",
                                                        558.165538
                                                    ]
                                                }
                                            }
                                        ],
                                        "default": None
                                    }
                                },
                                2
                            ]
                        },
                        "else": "$$REMOVE"
                    }
                }
            }
        },
        {
            "$merge": {
                "into": "sources",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ])


def normalize_source_scimago_best_quartile(collection) -> None:
    """
    Function to normalize the Scimago Best Quartile ranking for sources.

    This aggregation selects, for each source, the best available Scimago
    Best Quartile value (prioritizing Q1 over lower quartiles and other
    values) and stores it in the ``scimago_best_quartile`` field of the
    ``sources`` collection.

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the sources are stored.
    """
    collection.aggregate([
        {
            "$project": {
                "_id": 1,
                "ranking": 1
            }
        },
        {
            "$match": {
                "ranking.source": {
                    "$in": ["scimago Best Quartile", "Scimago Best Quartile"]
                }
            }
        },
        {
            "$unwind": "$ranking"
        },
        {
            "$match": {
                "ranking.source": {
                    "$in": ["scimago Best Quartile", "Scimago Best Quartile"]
                },
                "ranking.rank": {"$exists": True, "$nin": [None, ""]}
            }
        },
        {
            "$addFields": {
                "ranking_priority": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$eq": ["$ranking.rank", "Q1"]
                                },
                                "then": 1
                            },
                            {
                                "case": {
                                    "$eq": ["$ranking.rank", "Q2"]
                                },
                                "then": 2
                            },
                            {
                                "case": {
                                    "$eq": ["$ranking.rank", "Q3"]
                                },
                                "then": 3
                            },
                            {
                                "case": {
                                    "$eq": ["$ranking.rank", "Q4"]
                                },
                                "then": 4
                            },
                            {
                                "case": {
                                    "$eq": ["$ranking.rank", "-"]
                                },
                                "then": 5
                            }
                        ],
                        "default": 6
                    }
                }
            }
        },
        {
            "$sort": {"_id": 1, "ranking_priority": 1}
        },
        {
            "$group": {
                "_id": "$_id",
                "scimago_best_quartile": {"$first": "$ranking.rank"}
            }
        },
        {
            "$merge": {
                "into": "sources",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ], allowDiskUse=True)


def normalize_source_open_access_status(collection) -> None:
    """
    Function to categorize sources as diamond, gold, hybrid, or closed based on
    open access start year and APC charges, and store this status on the source.

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Collection where the sources are stored
    """
    collection.aggregate([
        {
            "$project": {
                "_id": 1,
                "open_access_start_year": 1,
                "apc": 1
            }
        },
        {
            "$addFields": {
                "open_access_status": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$ifNull": [
                                                "$open_access_start_year",
                                                False
                                            ]
                                        },
                                        {
                                            "$gt": [
                                                "$open_access_start_year",
                                                0
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {
                                                    "$eq": [
                                                        {
                                                            "$ifNull": [
                                                                "$apc.charges",
                                                                0
                                                            ]
                                                        },
                                                        0
                                                    ]
                                                },
                                                {"$eq": ["$apc", {}]},
                                                {
                                                    "$not": {
                                                        "$ifNull": [
                                                            "$apc.charges",
                                                            False
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "then": "diamond"
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$ifNull": [
                                                "$open_access_start_year",
                                                False
                                            ]
                                        },
                                        {
                                            "$gt": [
                                                "$open_access_start_year",
                                                0
                                            ]
                                        },
                                        {
                                            "$gt": [
                                                {
                                                    "$ifNull": [
                                                        "$apc.charges",
                                                        0
                                                    ]
                                                },
                                                0
                                            ]
                                        }
                                    ]
                                },
                                "then": "gold"
                            },
                            {
                                "case": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {
                                                    "$not": {
                                                        "$ifNull": [
                                                            "$open_access_start_year",  # noqa: E501
                                                            False
                                                        ]
                                                    }
                                                },
                                                {
                                                    "$eq": [
                                                        "$open_access_start_year",  # noqa: E501
                                                        0
                                                    ]
                                                },
                                                {
                                                    "$eq": [
                                                        "$open_access_start_year",  # noqa: E501
                                                        None
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            "$gt": [
                                                {
                                                    "$ifNull": [
                                                        "$apc.charges",
                                                        0
                                                    ]
                                                },
                                                0
                                            ]
                                        }
                                    ]
                                },
                                "then": "hybrid"
                            }
                        ],
                        "default": "closed"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "open_access_status": 1
            }
        },
        {
            "$merge": {
                "into": "sources",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ], allowDiskUse=True)


def normalize_source_topics(collection) -> None:
    collection.aggregate(
        [
            {
                "$project": {
                    "_id": 1,
                }
            },
            {
                "$lookup": {
                    "from": "works",
                    "localField": "_id",
                    "foreignField": "source.id",
                    "pipeline": [
                        {
                            "$match": {
                                "primary_topic.id": {
                                    "$exists": True,
                                    "$ne": None,
                                }
                            }
                        },
                        {
                            "$count": "total",
                        },
                    ],
                    "as": "works_count_result",
                }
            },
            {
                "$addFields": {
                    "works_count": {
                        "$ifNull": [
                            {
                                "$arrayElemAt": [
                                    "$works_count_result.total",
                                    0,
                                ]
                            },
                            0,
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "topics_threshold": {
                        "$multiply": [
                            "$works_count",
                            0.01,
                        ]
                    }
                }
            },
            {
                "$lookup": {
                    "from": "works",
                    "localField": "_id",
                    "foreignField": "source.id",
                    "pipeline": [
                        {
                            "$match": {
                                "primary_topic.id": {
                                    "$exists": True,
                                    "$ne": None,
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "primary_topic": 1,
                            }
                        },
                        {
                            "$group": {
                                "_id": "$primary_topic.id",
                                "count": {"$sum": 1},
                                "topic": {"$first": "$primary_topic"},
                            }
                        },
                    ],
                    "as": "all_topics",
                }
            },
            {
                "$addFields": {
                    "topics": {
                        "$cond": {
                            "if": {"$eq": ["$works_count", 0]},
                            "then": [],
                            "else": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$all_topics",
                                            "as": "topic",
                                            "cond": {
                                                "$gte": [
                                                    "$$topic.count",
                                                    "$topics_threshold",
                                                ]
                                            },
                                        }
                                    },
                                    "as": "filtered_topic",
                                    "in": {
                                        "id": "$$filtered_topic.topic.id",
                                        "display_name": (
                                            "$$filtered_topic.topic.display_name"
                                        ),
                                        "subfield": (
                                            "$$filtered_topic.topic.subfield"
                                        ),
                                        "field": "$$filtered_topic.topic.field",
                                        "domain": "$$filtered_topic.topic.domain",
                                    },
                                }
                            },
                        }
                    }
                }
            },
            {
                "$merge": {
                    "into": "sources",
                    "on": "_id",
                    "whenMatched": "merge",
                    "whenNotMatched": "discard",
                }
            },
        ],
        allowDiskUse=True,
    )


DENORMALIZATION_PIPELINES = {
    "works": [
        set_works_authors_affiliations_country,
        set_works_authors_affiliations_country_code,
        set_works_groups_ranking,
        set_works_authors_ranking,
        set_works_citations_count_openalex,
        set_works_authors_full_data,
        set_works_authors_affiliations_dates,
        set_works_source_full_data,
        set_works_authors_affiliations_external_data,
        set_works_groups_citations_count,
        set_works_groups_ranking_to_works_collection,
        clean_works_authors_affiliations_country_fields,
        normalize_works_authors_ranking_empty_list,
    ],
    "sources": [
        set_sources_products_count,
        normalize_sources_products_count,
        set_sources_citations_count_openalex,
        normalize_sources_citations_count,
        normalize_sources_global_counts,
        normalize_source_apc_usd,
        normalize_source_scimago_best_quartile,
        normalize_source_open_access_status,
        normalize_source_topics
    ],
    "person": [
        set_person_affiliations_relations,
        clean_person_empty_affiliations_array,
    ],
    "affiliations": [
        set_affiliations_citations_count_openalex,
    ],
}


def _run_collection_pipelines(db, collection_name, pipelines):
    collection = db[collection_name]
    print(f"INFO: Denormalizing data in {collection_name}")
    for pipeline_func in pipelines:
        print(f"INFO: Running pipeline {pipeline_func.__name__}")
        pipeline_func(collection)


def denormalize(db, parallel_collections=None, max_parallel_jobs=None):
    """
    Denormalize the data in all configured collections

    Parameters
    ----------
    db : pymongo.database.Database
        Database object to denormalize
    """
    if parallel_collections is None:
        parallel_collections = False
    if max_parallel_jobs is None:
        max_parallel_jobs = 2
    max_parallel_jobs = max(1, max_parallel_jobs)

    collection_runs = list(DENORMALIZATION_PIPELINES.items())

    if not parallel_collections:
        for collection_name, pipelines in collection_runs:
            _run_collection_pipelines(db, collection_name, pipelines)
        return

    print(
        "INFO: Collection-level parallelization enabled "
        f"(max workers: {max_parallel_jobs})"
    )

    index = 0
    while index < len(collection_runs):
        collection_name, pipelines = collection_runs[index]
        if collection_name not in PARALLEL_SAFE_COLLECTIONS:
            _run_collection_pipelines(db, collection_name, pipelines)
            index += 1
            continue

        batch_end = index
        while batch_end < len(collection_runs) and (
            collection_runs[batch_end][0] in PARALLEL_SAFE_COLLECTIONS
        ):
            batch_end += 1

        batch = collection_runs[index:batch_end]
        if len(batch) == 1:
            _run_collection_pipelines(db, batch[0][0], batch[0][1])
            index = batch_end
            continue

        workers = min(max_parallel_jobs, len(batch))
        batch_names = [name for name, _ in batch]
        print(
            "INFO: Running collections in parallel: "
            f"{batch_names} with {workers} workers"
        )
        Parallel(n_jobs=workers, prefer="threads", backend="threading")(
            delayed(_run_collection_pipelines)(db, name, run_pipelines)
            for name, run_pipelines in batch
        )

        index = batch_end
