from flask import Flask, request, jsonify
from helpers import extract_tokens
import os
from dotenv import load_dotenv
import time
from joblib import Parallel, delayed
import boto3
import json
import logging

load_dotenv()
app = Flask(__name__)
rpcUrl=os.getenv("RPC_URL")
dynamodb = boto3.resource('dynamodb')

cnftTable = dynamodb.Table('cnftTable')
uriTable = dynamodb.Table('uriTable')

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
  
def classify_one(id, uri=None):
  response = cnftTable.get_item(
    Key={
        'address': id,
    }
  )
  if ("Item" in response):
    return response["Item"]["classification"]

  if (uri):
    # cache based on uri of metadata
    # good for ingesting
    response = uriTable.get_item(
      Key={
          'uri': uri,
      }
    )
    if ("Item" in response):
      classification = response["Item"]["classification"]
      cnftTable.put_item(
        Item={
          'address': id,
          'classification': classification,
        }
      ) 
      return classification

  tokens = extract_tokens(rpcUrl)
  classification = classify(tokens)

  cnftTable.put_item(
    Item={
      'address': id,
      'classification': classification,
    }
  )

  if (uri):
    uriTable.put_item(
      Item={
        'uri': uri,
        'classification': classification,
      }
    )

  return classification

@app.route("/classify", methods=["POST"])
def classify_route():
  data = request.json

  if ("ids" not in data):
    return jsonify({
      "error": "No ids provided"
    }), 400

  result = []
  result = Parallel(n_jobs=-1, prefer="threads")(delayed(classify_one)(id) for id in data["ids"])

  return jsonify(result)

@app.route("/ingest", methods=["POST"])
def ingest_route():
  data = request.json
  events = data[0]["events"]["compressed"]

  results = []
  for event in events:
    start_time = time.time()
    result = classify_one(event["assetId"], event["metadata"]["uri"])
    print("https://xray.helius.xyz/token/{0}: {1} in {2}s".format(event["assetId"], result, time.time() - start_time))
    results.append(result)

  return "ok"

if __name__ == "__main__":
  logging.getLogger("werkzeug").disabled = True
  app.run()
