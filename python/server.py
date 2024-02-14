from flask import Flask, request, jsonify
import requests
from borsh_construct import CStruct, U8, U32, U64, HashMap, String, Bytes, Vec
import base64
import math
import os
from dotenv import load_dotenv
from PIL import Image
import easyocr
import io
import time
from joblib import Parallel, delayed
import boto3
import json
import re

load_dotenv()
app = Flask(__name__)
rpcUrl=os.getenv("RPC_URL")
dynamodb = boto3.resource('dynamodb')

cnftTable = dynamodb.Table('cnftTable')
treeTable = dynamodb.Table('treeTable')
imageTable = dynamodb.Table('imageTable')
uriTable = dynamodb.Table('uriTable')

model_file = open('model.json')
model = json.load(model_file)

reader = easyocr.Reader(['en'])

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

# gets the proof length from a given treeId
# very useful for determining if tree is spam or not
# lots of crazy byte counting here
def get_proof_length(treeId):
  response = treeTable.get_item(
    Key={
        'address': treeId,
    }
  )
  if ("Item" in response):
    return response["Item"]["proofLength"]

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

  treeTable.put_item(
    Item={
      'address': treeId,
      'proofLength': proofLength,
    }
  )

  return proofLength

def get_image_words(imageUrl):
  response = imageTable.get_item(
    Key={
        'url': imageUrl,
    }
  )
  if ("Item" in response):
    return response["Item"]["words"]

  start = time.time()
  print(0, "fetching image")
  response = requests.get(imageUrl)

  print(time.time() - start, "converting image")
  img = Image.open(io.BytesIO(response.content))
  img = img.convert("RGB")
  img = img.resize((500, 500))
  imgByteArr = io.BytesIO()
  img.save(imgByteArr, format='JPEG')

  print(time.time() - start, "starting ocr")
  result = reader.readtext(imgByteArr.getvalue(), detail=0, batch_size=16)
  print(time.time() - start, "finished ocr")

  imageTable.put_item(
    Item={
      'url': imageUrl,
      'words': result,
    }
  )

  return result

def classify(tokens):
  spamLikelihood = model["spam"]["size"] / (model["spam"]["size"] + model["ham"]["size"])
  hamLikelihood = 1 - spamLikelihood

  uniqueTokens = set(tokens)

  for token in uniqueTokens:
    spamNumerator = 1
    if (token in model["spam"]["tokens"]):
      spamNumerator = model["spam"]["tokens"][token] + 1
    hamNumerator = 1
    if (token in model["ham"]["tokens"]):
      hamNumerator = model["ham"]["tokens"][token] + 1
    spamTokenLikelihood = spamNumerator / (model["spam"]["size"] + 2)
    hamTokenLikelihood = hamNumerator / (model["ham"]["size"] + 2)

    spamLikelihood *= spamTokenLikelihood
    hamLikelihood *= hamTokenLikelihood

  if (spamLikelihood > hamLikelihood):
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

  startTime = time.time()
  print(0, "rpc call 1")
  
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
  treeId = compressionData["tree"]
  print(time.time() - startTime, "rpc call 2")
  proofLength = get_proof_length(treeId)

  # prefer cdn when available
  # imageUrl = ""
  # if "cdn_uri" in rpcResponse["result"]["content"]["files"][0]:
  #   imageUrl = rpcResponse["result"]["content"]["files"][0]["cdn_uri"]
  # else:
  #   imageUrl = rpcResponse["result"]["content"]["links"]["image"]

  imageUrl = rpcResponse["result"]["content"]["links"]["image"]

  print(time.time() - startTime, "ocr call")
  imageWords = get_image_words(imageUrl)
  attributeWords = []

  if ("attributes" in rpcResponse["result"]["content"]["metadata"]):
    attributes = rpcResponse["result"]["content"]["metadata"]["attributes"]
    for attribute in attributes:
      attributeWords += attribute["value"].split()
      attributeWords += attribute["trait_type"].split()

  tokens = imageWords + attributeWords

  keywords = [
    "containsEmoji",
    "proofLengthImpossible",
    "imageContainsUrl",
    "not_containsEmoji",
    "not_proofLengthImpossible",
    "not_imageContainsUrl",
  ]

  tokens = list(filter(lambda token: len(token) > 2 and token not in keywords, tokens))

  EMOJI_PATTERN = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0001FA00-\U0001FA6F"  # Chess Symbols
    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027B0"  # Dingbats
    "\U000024C2-\U0001F251" 
    "]+",
  flags = re.UNICODE) 

  containsUrl = False
  containsEmoji = False

  for token in tokens:
    if (re.search(r'^[\S]+[.][\S]', token)):
      containsUrl = True
    if (re.search(EMOJI_PATTERN, token)):
      containsEmoji = True

  if (proofLength > 23):
    tokens.append("proofLengthImpossible")
  else:
    tokens.append("not_proofLengthImpossible")

  if (containsUrl):
    tokens.append("imageContainsUrl")
  else:
    tokens.append("not_imageContainsUrl")
  
  if (containsEmoji):
    tokens.append("containsEmoji")
  else:
    tokens.append("not_containsEmoji")

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
def classifyRoute():
  data = request.json

  if ("ids" not in data):
    return jsonify({
      "error": "No ids provided"
    }), 400

  result = []
  startTime = time.time()
  result = Parallel(n_jobs=-1, prefer="threads")(delayed(classify_one)(id) for id in data["ids"])
  print("returning after", time.time() - startTime)

  return jsonify(result)

@app.route("/ingest", methods=["POST"])
def ingestRoute():
  data = request.json
  events = data[0]["events"]["compressed"]

  results = []
  for event in events:
    print(event)
    result = classify_one(event["assetId"], event["metadata"]["uri"])
    results.append(result)
  
  return "ok" 

if __name__ == "__main__":
  app.run(host='0.0.0.0', port=8888)
