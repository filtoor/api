from flask import Flask, request, jsonify
from helpers import extract_tokens
import os
from dotenv import load_dotenv
import time
from joblib import Parallel, delayed
import json
import logging
from db_helpers import CNFT

load_dotenv()
app = Flask(__name__)
rpcUrl=os.getenv("RPC_URL")

nft_table = CNFT().table
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
    if (token in model["spam"]["tokens"]):
      spam_numerator = model["spam"]["tokens"][token] + 1
    ham_numerator = 1
    if (token in model["ham"]["tokens"]):
      ham_numerator = model["ham"]["tokens"][token] + 1
    spam_token_likelihood = spam_numerator / (model["spam"]["size"] + 2)
    ham_token_likelihood = ham_numerator / (model["ham"]["size"] + 2)

    spam_likelihood *= spam_token_likelihood
    ham_likelihood *= ham_token_likelihood

  if (spam_likelihood > ham_likelihood):
    return "spam"
  else:
    return "ham"
  
def classify_one(token_id):
  """
  Pull from cache or classify a single token id
  """
  query_cnft_table = session.get(nft_table, token_id)

  if query_cnft_table:
    json_id = query_cnft_table.jsonMetadataId
    tree_id = query_cnft_table.treeId
    tokens, json_id, tree_id = extract_tokens(token_id, rpcUrl, json_id, tree_id)
  else:
    tokens, json_id, tree_id = extract_tokens(token_id, rpcUrl)
    cnft_to_add = nft_table(address=token_id, jsonMetadataId=json_id, treeId=tree_id)
    session.add(cnft_to_add)
    session.commit()
  
  
  classification = classify(tokens)
  return classification

@app.route("/classify", methods=["POST"])
def classify_route():
  """
  Classify route, takes multiple ids
  """
  data = request.json

  if ("ids" not in data):
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
    print("https://xray.helius.xyz/token/{0}: {1} in {2}s".format(event["assetId"], result, time.time() - start_time))
    results.append(result)

  return "ok"

if __name__ == "__main__":
  app.run()
