import sys


def process_person_id(client, person_col, works_col, person, source):
    """
    Process the person ID based on the source and update the MongoDB collection.
    Parameters:
    ---------
    client: MongoDB client
    person_col: MongoDB collection for person
    works_col: MongoDB collection for works
    person: dict
        The person document to process
    source: str
        The source of the person ID (e.g., "mongodb_id", "scienti", etc.)
    """

    original_id = person["_id"]
    pid = None  # person id

    if source == "mongodb_id":
        pid = str(original_id)
    else:
        for ext_id in person.get("external_ids", []):
            if ext_id.get("source") == source:
                pid = ext_id.get("id", {})
                break
    if source == "scienti":
        # Si el source es 'scienti', buscar COD_RH en el campo 'id'
        pid = pid.get("COD_RH", None)
    else:
        pid = pid.split("/")[-1].replace("-", "").split("=")[-1]

    if not pid:
        # Si no hay COD_RH, generar ID basado en ObjectId
        print(
            "ERROR: No hay id para el documento:",
            original_id,
            "source:",
            source,
        )
        sys.exit(1)
    opid = pid

    # Insertar con nuevo _id
    new_doc = person.copy()
    new_doc["_id"] = pid
    new_doc["_id_old"] = original_id
    with client.start_session() as session:
        try:
            with session.start_transaction():  # atomic operation
                person_col.insert_one(new_doc)
                person_col.delete_one({"_id": original_id})
                # print(f"Reemplazado en person: {original_id} → {pid}")
        except Exception as e:
            print(
                f"Error insertando nuevo _id en person: {e}\n {source} {opid} {str(original_id)}")
            return pid

    # Actualizar works.authors.id
    works_col.update_many(
        {"authors.id": original_id},
        {"$set": {"authors.$[author].id": pid}},
        array_filters=[{"author.id": original_id}],
    )
