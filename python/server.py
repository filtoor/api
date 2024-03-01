"""
Filtoor API
"""
import os
import json
import time
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from joblib import Parallel, delayed
from helpers import extract_tokens, get_tokens
from db_helpers import CNFT, imageOCRTable, jsonMetadataTable, treeTable

load_dotenv()
app = Flask(__name__)
rpcUrl=os.getenv("RPC_URL")

nft_table = CNFT().table
image_ocr_table = imageOCRTable().table
json_metadata_table = jsonMetadataTable().table
tree_table = treeTable().table
session = CNFT().db.session

with open('model.json') as model_file:
    model = json.load(model_file)

def classify(tokens):
    """
    Return a 'spam' or 'ham' classification given a list of tokens
    """
    spam_likelihood = model["spam"]["size"] / (model["spam"]["size"] + model["ham"]["size"])
    ham_likelihood = 1 - spam_likelihood

    unique_tokens = set(tokens)

    for token in unique_tokens:
        spam_numerator = 1
        if token in model["spam"]["tokens"]:
            spam_numerator = model["spam"]["tokens"][token] + 1

        ham_numerator = 1
        if token in model["ham"]["tokens"]:
            ham_numerator = model["ham"]["tokens"][token] + 1

        spam_token_likelihood = spam_numerator / (model["spam"]["size"] + 2)
        ham_token_likelihood = ham_numerator / (model["ham"]["size"] + 2)

        spam_likelihood *= spam_token_likelihood
        ham_likelihood *= ham_token_likelihood

    if spam_likelihood > ham_likelihood:
        return "spam"

    return "ham"

def classify_one(token_id):
    """
    Pull from cache or classify a single token id
    """
    query_cnft_table = session.query(nft_table, json_metadata_table, tree_table, image_ocr_table).filter(nft_table.id == token_id).join(json_metadata_table, json_metadata_table.id == nft_table.jsonMetadataId).join(image_ocr_table, image_ocr_table.id == json_metadata_table.imageOCRId).join(tree_table, tree_table.id == nft_table.treeId).first()

    if query_cnft_table:
        _, json_metadata, tree_metadata, image_metadata = query_cnft_table
        tokens = get_tokens(image_metadata.tokens, json_metadata.attributes, tree_metadata.proofLength if tree_metadata else 0) # Tree metadata can be None
    else:
        tokens = extract_tokens(token_id, rpcUrl)

    classification = classify(tokens)
    return classification

@app.route("/classify", methods=["POST"])
def classify_route():
    """
    Classify route, takes multiple ids
    """
    data = request.json

    if "ids" not in data:
        return jsonify({
            "error": "No ids provided"
        }), 400

    result = []
    result = Parallel(n_jobs=-1, prefer="threads")(delayed(classify_one)(token_id) for token_id in data["ids"])

    return jsonify(result)

@app.route("/ingest", methods=["POST"])
def ingest_route():
    """
    Ingest route
    """
    data = request.json
    events = data[0]["events"]["compressed"]

    results = []
    for event in events:
        start_time = time.time()
        result = classify_one(event["assetId"])
        print(f"https://xray.helius.xyz/token/{event['assetId']}: {result} in {time.time() - start_time}s")
        results.append(result)

    return "ok"

if __name__ == "__main__":
    app.run()


