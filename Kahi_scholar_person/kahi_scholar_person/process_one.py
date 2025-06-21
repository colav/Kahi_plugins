from kahi_scholar_person.parser import parse_scholar
from time import time


def process_one(paper, db, collection, empty_person, verbose):
    # Parse the authors from the paper
    authors = parse_scholar(paper, empty_person)
    if not authors:
        return None

    for author in authors:
        # Retrieve the Scholar ID from the author's external IDs (if available)
        scholar_id = next(
            (exid["id"] for exid in author.get("external_ids", []) if exid["source"] == "scholar"), None)
        # If a Scholar ID exists, check if the author already exists in the database
        if scholar_id:
            author_db = collection.find_one({"external_ids.id": scholar_id})
        else:
            author_db = None
        if author_db:
            already_updated = False
            # Check if the author has already been updated from Scholar
            for upd in author["updated"]:
                if upd["source"] == "scholar":
                    already_updated = True
            if not already_updated:
                author_db["updated"].append(
                    {"source": "scholar", "time": int(time())})
            # Merge the full name (use the longest one)
            if len(author["full_name"]) > len(author_db["full_name"]):
                author_db["full_name"] = author["full_name"]
                author_db["first_names"] = author["first_names"]
                author_db["last_names"] = author["last_names"]
                author_db["initials"] = author["initials"]
                # Merge the aliases (only add new ones)
                if author_db["full_name"] not in author_db["aliases"]:
                    author_db["aliases"].append(author_db["full_name"])
            # Merge related works (only add new ones)
            if author["related_works"]:
                for work in author["related_works"]:
                    if work not in author_db["related_works"]:
                        author_db["related_works"].append(work)
            # Update the existing author document in the database
            collection.update_one(
                {"_id": author_db["_id"]},
                {"$set": {
                    "updated": author_db["updated"],
                    "full_name": author_db["full_name"],
                    "first_names": author_db["first_names"],
                    "last_names": author_db["last_names"],
                    "initials": author_db["initials"],
                    "aliases": author_db["aliases"],
                    "related_works": author_db["related_works"]
                }}
            )
        else:
            # If the author does not exist, insert them into the database
            collection.insert_one(author)

    return None
