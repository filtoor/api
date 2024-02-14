from flask import Flask, request, jsonify
import requests
from borsh_construct import CStruct, U8, U32, U64, HashMap, String, Bytes, Vec
from solders.account_decoder import ParsedAccount
import base64
import math
import os
from dotenv import load_dotenv
from PIL import Image
import easyocr
import io
import time
import multiprocessing as mp
from ocr import get_image_words

load_dotenv()

app = Flask(__name__)

rpcUrl=os.getenv("RPC_URL")

no_processes = mp.cpu_count()
input_queues = []
output_queues = []

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

# def get_image_words(imageUrl, reader):
#   start = time.time()
#   print(0, "fetching image")
#   response = requests.get(imageUrl)

#   print(time.time() - start, "converting image")
#   img = Image.open(io.BytesIO(response.content))
#   img = img.convert("RGB")
#   img = img.resize((1000, 1000))
#   imgByteArr = io.BytesIO()
#   img.save(imgByteArr, format='JPEG')

#   print(time.time() - start, "starting ocr")
#   result = reader.readtext(imgByteArr.getvalue(), detail=0, batch_size=16)
#   print(time.time() - start, "finished ocr")

#   return result

def classify_one(id, reader):
  startTime = time.time()
  print(0, "rpc call 1")
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
  print(time.time() - startTime, "rpc call 2")
  proofLength = get_proof_length(treeId)

  # prefer cdn when available
  imageUrl = ""
  if "cdn_uri" in rpcResponse["result"]["content"]["files"][0]:
    imageUrl = rpcResponse["result"]["content"]["files"][0]["cdn_uri"]
  else:
    imageUrl =rpcResponse["result"]["content"]["links"]["image"]

  print(time.time() - startTime, "ocr call")
  imageWords = get_image_words(imageUrl, reader)

  return {"imageWords": imageWords, "proofLength": proofLength}

@app.route("/classify", methods=["POST"])
def classify():
  data = request.json

  if ("ids" not in data):
    return jsonify({
      "error": "No ids provided"
    }), 400

  result = []
  startTime = time.time()
  print(len(data["ids"]))
  with mp.Pool(7) as p:
    result = p.starmap(classify_one, [(id, reader) for id in data["ids"]])

  print("returning after", time.time() - startTime)

  return jsonify(result)

if __name__ == "__main__":
  reader = easyocr.Reader(['en'])
  mp.set_start_method('spawn')
  app.run()
