import requests

type_url_base = "https://openalex.org/T"


def request_topic_inference(title, abstract={}, journal_name="", inference_endpoint="http://localhost:8080/invocations"):
    """
    Method to request the topic inference for a given work

    Parameters
    ----------
    title : str
        Title of the work
    abstract : dict, optional
        Abstract of the work, by default {}
    journal_name : str, optional
        Name of the journal where the work was published, by default ""
    referenced_works : list, optional
        List of works referenced by the work, by default []
    inference_endpoint : str, optional
        Endpoint of the inference service, by default "http://localhost:8080/invocations"

    Returns
    -------
    requests.Response
        Response of the inference service
    """
    payload = [
        {"title": title,
         "abstract_inverted_index": abstract,
         "inverted": True,
         "referenced_works": [],
         "journal_display_name": journal_name}
    ]

    req = requests.post(inference_endpoint, json=payload)
    return req


def get_openalex_topic(col_oa, topic_pred):
    """
    Retrieve the topic from OpenAlex based on the prediction,
    and add the score of the prediction to the topic.
    The topic is retrieved from the OpenAlex collection and it has the following fields:
    - id: URL of the topic
    - display_name: Name of the topic
    - subfield: Subfield of the topic
    - field: Field of the topic
    - domain: Domain of the topic
    - score: Score of the prediction

    Parameters
    ----------
    col_oa : pymongo.collection.Collection
        Collection of OpenAlex topics
    topic_pred : dict
        Prediction of the topic, with the following fields:
        - topic_id: ID of the topic
        - topic_score: Score of the prediction

    Returns
    -------
    dict
        Topic from OpenAlex with the score of the prediction

    """
    topic_url = type_url_base + str(topic_pred['topic_id'])
    topic = col_oa.find_one({"id": topic_url}, {
                            "id": 1, "display_name": 1, "subfield": 1, "field": 1, "domain": 1})
    if topic is None:
        topic = {"id": topic_pred['topic_id'], "display_name": "Unknown",
                 "subfield": "Unknown", "field": "Unknown", "domain": "Unknown"}
        print("WARNING: Topic not found predicted in inference",
              topic_url, topic_pred)
    topic["score"] = topic_pred["topic_score"]
    return topic


def process_topic(col, col_oa, work, inference_endpoint="http://localhost:8080/invocations"):
    """
    Process the topic inference for a given work and retrieve the topics from OpenAlex.
    The work is updated with the primary topic and the list of topics.

    Parameters
    ----------
    col : pymongo.collection.Collection
        Collection of the works (kahi database)
    col_oa : pymongo.collection.Collection
        Collection of OpenAlex topics
    work : dict
        Work to process
    inference_endpoint : str, optional
        Endpoint of the inference service, by default "http://localhost:8080/invocations"
    """
    title = work["titles"][0]["title"]
    abstract = work["abstracts"][0]["abstract"] if len(
        work["abstracts"]) > 0 else {}
    journal_name = work["source"]["name"] if work["source"] != {} else ""
    req = request_topic_inference(
        title, abstract, journal_name, inference_endpoint)
    if req.status_code == 200:
        topics_ids_pred = req.json()

        for topic_pred in topics_ids_pred[0]:
            topic = get_openalex_topic(col_oa, topic_pred)

            if work["primary_topic"] == {}:
                work["primary_topic"] = topic
            work["topics"].append(topic)
        col.update_one({"_id": work["_id"]}, {
                       "$set": {"primary_topic": work["primary_topic"], "topics": work["topics"]}})
    else:
        print(
            f"ERROR: request for inference fails status code {req.status_code}\n", work)
