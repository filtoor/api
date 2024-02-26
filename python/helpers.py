import os
import requests
import base64
import math
from dotenv import load_dotenv
from borsh_construct import CStruct, U8, U32, U64
from PIL import Image
import easyocr
import io
import boto3
import re
import time
import imageio.v3 as iio
from db_helpers import imageOCRTable, jsonMetadataTable, treeTable

load_dotenv()


image_ocr_table = imageOCRTable().table
json_metadata_table = jsonMetadataTable().table
tree_table = treeTable().table
session = imageOCRTable().db.session

ENDPOINT = os.getenv("ENDPOINT")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")
PASSWORD = os.getenv("PASSWORD")
DBUSER = os.getenv("DBUSER")

reader = easyocr.Reader(['en'], gpu=False)

KEYWORDS = [
    "contains_emoji",
    "proof_lengthImpossible",
    "imagecontains_url",
    "not_contains_emoji",
    "not_proof_lengthImpossible",
    "not_imagecontains_url",
]


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
    flags = re.UNICODE
)

HEADER_SCHEMA = CStruct(
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

def get_image_words(image_url):
    """
    Get words from a given image url using OCR 
    If an mp4 file is given, get the image from the first frame
    image_url: the url of the image or video to get words from
    """
    response = session.query(image_ocr_table).filter(image_ocr_table.url == image_url).first()
    if response:
        return response.tokens, response.id
    
    try:
        start = time.time()
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
        response = requests.get(image_url, headers=headers, timeout=5)
        if response.status_code != 200:
            return []

        content_type = response.headers['Content-Type']

        # if Image is video, process the first frame
        if "video" in content_type:
            split = content_type.split("/")
            # As a last attempt, try to fallback on mp4 
            extension = "." + content_type.split("/")[1] if len(split) > 1 else ".mp4"
            frame = iio.imread(io.BytesIO(response.content), format_hint=extension, index=1)
            output = io.BytesIO()
            iio.imwrite(output, frame, plugin="pillow", extension=".jpeg")
            img = Image.open(output)

        else:
            img = Image.open(io.BytesIO(response.content))

        img = img.convert("RGB")
        img = img.resize((500, 500))
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format='JPEG')

        print("Image processing took", time.time() - start, "seconds")
        result = reader.readtext(img_byte_arr.getvalue(), detail=0, batch_size=16)
        print("OCR took", time.time() - start, "seconds")

        words = []
        for chunk in result:
            words += chunk.split()

        added_object_image_ocr = image_ocr_table(url=image_url, tokens=words)
        session.add(added_object_image_ocr)
        session.commit()
        return result, added_object_image_ocr.id
    except Exception as e:
        print(e)
        return []

def extract_tokens(token_id, rpc_url, json_id=None, tree_id=None):
    """
    Extract tokens (with the keywords above in mind) from an rpc_url 
    """
    image_words = []
    attributes = {}
    query_tree_metadata = session.get(tree_table, tree_id) if tree_id else None

    if query_tree_metadata:
        proof_length = query_tree_metadata.proofLength
    
    query_json_metadata = session.query(json_metadata_table, image_ocr_table).filter(json_metadata_table.id == json_id).join(image_ocr_table, image_ocr_table.id == json_metadata_table.imageOCRId).first() if json_id else None
    if query_json_metadata: 
        json_metadata, image_ocr_data = query_json_metadata
        attributes = json_metadata.attributes
        image_words = image_ocr_data.tokens

    else:
        response = requests.post(rpc_url, headers={
        "Content-Type": "application/json",
        },
        json={
        "jsonrpc": "2.0",
        "id": "i-love-mert",
        "method": "getAsset",
        "params": {
            "id": token_id,
            "displayOptions": {
            "showUnverifiedCollections": True,
            "showCollectionMetadata": True,
            "showFungible": False,
            "showInscription": False,
            },
        },
        }
        )
        rpc_response = response.json()

        if "error" in rpc_response:
            return "error"

        compression_data = rpc_response["result"]["compression"]
        
        if not query_tree_metadata:
            tree_id = compression_data["tree"]
            proof_length, max_depth, buffer_size = get_proof_length(tree_id, rpc_url)
            tree_item_to_add = tree_table(address=tree_id, proofLength=proof_length, maxDepth=max_depth, maxBuffer=buffer_size)
            session.add(tree_item_to_add)
            session.commit()
        
        json_metadata = rpc_response["result"]["content"]["metadata"]
        if "attributes" in json_metadata:
            attributes = rpc_response["result"]["content"]["metadata"]["attributes"]
        
        if "image" in rpc_response["result"]["content"]["links"]:
            image_url = rpc_response["result"]["content"]["links"]["image"]
            image_words, image_ocr_id = get_image_words(image_url)
        
        name = json_metadata["name"]
        description = json_metadata["description"]
        json_to_add = json_metadata_table(name=name, description=description, attributes=attributes, imageOCRId=image_ocr_id)
        session.add(json_to_add)
        session.commit()
        json_id = json_to_add.id
    
    attribute_words = []
    for attribute in attributes:
        if "value" in attribute:
            attribute_words += str(attribute["value"]).split()
        if "trait_type" in attribute:
            attribute_words += str(attribute["trait_type"]).split()


    tokens = image_words + attribute_words

    tokens = list(filter(lambda token: len(token) > 2 and token not in KEYWORDS, tokens))

    contains_url = False
    contains_emoji = False

    for token in tokens:
        if re.search(r'^[\S]+[.][\S]', token):
            contains_url = True
        if re.search(EMOJI_PATTERN, token):
            contains_emoji = True

    if proof_length > 23:
        tokens.append("proof_lengthImpossible")
    else:
        tokens.append("not_proof_lengthImpossible")

    if contains_url:
        tokens.append("imagecontains_url")
    else:
        tokens.append("not_imagecontains_url")
    if contains_emoji:
        tokens.append("contains_emoji")
    else:
        tokens.append("not_contains_emoji")
    return tokens, json_id, tree_id


def get_proof_length(tree_id, rpc_url):

    """
    Gets the proof length from a given tree_id
    Very useful for determining if tree is spam or not
    Lots of crazy byte counting here
    """
    response = requests.post(rpc_url, headers={
        "Content-Type": "application/json",
        },
        json={
        "jsonrpc": "2.0",
        "id": "i-love-mert",
        "method": "getAccountInfo",
        "params": [tree_id, {
            "encoding": "base64"
        }],
        },
    )
    rpc_response = response.json()
    data = rpc_response["result"]["value"]["data"][0]
    byte_data = base64.b64decode(data.encode())
    parsed_bytes = HEADER_SCHEMA.parse(byte_data)

    fixed_header_size = 80
    max_depth = parsed_bytes["maxDepth"]
    buffer_size = parsed_bytes["maxBufferSize"]
    change_log_size = (40 + 32 * max_depth) * buffer_size
    right_most_path_size = 40 + 32 * max_depth

    canopy_size = len(byte_data) - (fixed_header_size + change_log_size + right_most_path_size)
    canopy_height = int(math.log2(canopy_size / 32 + 2) - 1)
    proof_length = max_depth - canopy_height

    return proof_length, max_depth, buffer_size
