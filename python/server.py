from flask import Flask, request, jsonify
import requests
from borsh_construct import CStruct, U8, U32, U64, HashMap, String, Bytes, Vec
from solders.account_decoder import ParsedAccount
import base64
import math
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

rpcUrl=os.getenv("RPC_URL")

header_schema = CStruct(
  "versionedHeader" / U8,
  "notSure" / U8,
  "maxBufferSize" / U32,
  "maxDepth" / U32,
  "authority" / U8[32],
  "creationSlot" / U64,
  "padding" / U8[6],
  "sequenceNumber" / U64,
  "activeIndex" / U64,
  "bufferSize" / U64,
)

account_schema = CStruct(
    "accountType" / String,
    "header" / header_schema
)

# gets the proof length from a given treeId
# very useful for determining if tree is spam or not
# lots of crazy byte counting here
def get_proof_length(treeId):
  response = requests.post(rpcUrl, headers={
      "Content-Type": "application/json",
    },
    json={
      "jsonrpc": "2.0",
      "id": "i-love-mert",
      "method": "getAccountInfo",
      "params": [treeId, {
        "encoding": "base64"
      }],
    },
  )
  rpcResponse = response.json()
  data = rpcResponse["result"]["value"]["data"][0]
  byte_data = base64.b64decode(data.encode())
  parsed_bytes = header_schema.parse(byte_data)

  fixedHeaderSize = 80 
  maxDepth = parsed_bytes["maxDepth"]
  bufferSize = parsed_bytes["maxBufferSize"]

  changeLogSize = (40 + 32 * maxDepth) * bufferSize
  rightMostPathSize = 40 + 32 * maxDepth

  canopySize = len(byte_data) - (fixedHeaderSize + changeLogSize + rightMostPathSize)
  canopyHeight = int(math.log2(canopySize / 32 + 2) - 1)
  proofLength = maxDepth - canopyHeight

  return proofLength 

def classify_one(id):
  # TODO: check against the database
  # if found, return the result
  response = requests.post(rpcUrl, headers={
      "Content-Type": "application/json",
    },
    json={
      "jsonrpc": "2.0",
      "id": "i-love-mert",
      "method": "getAsset",
      "params": {
        "id": id,
        "displayOptions": {
          "showUnverifiedCollections": True,
          "showCollectionMetadata": True,
          "showFungible": False,
          "showInscription": False,
        },
      },
    },
  )
  rpcResponse = response.json()


  if "error" in rpcResponse:
    return "error"

  compressionData = rpcResponse["result"]["compression"]
  # TODO: check the data_hash against database
  # if found, return the result
  treeId = compressionData["tree"]
  proofLength = get_proof_length(treeId)
  print(proofLength)

  return rpcResponse

@app.route("/classify", methods=["POST"])
def classify():
  data = request.json

  if ("ids" not in data):
    return jsonify({
      "error": "No ids provided"
    }), 400

  result = 0
  for id in data["ids"]:
    classify_one(id)

  return jsonify(result)