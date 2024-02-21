from dotenv import load_dotenv
import json
from helpers import extract_tokens, KEYWORDS
import os

load_dotenv()
RPC_URL=os.getenv("RPC_URL")

with open("spam_ids.json") as spam_json:
    spam_ids = json.load(spam_json) 
with open("ham_ids.json") as ham_json:
    ham_ids = json.load(ham_json)

model = {
    "spam": {
        "tokens": {},
        "size": 0,
    },
    "ham": {
        "tokens": {},
        "size": 0,
    },
}

def train(category, tokens):
    """
    Train the classifier on one category/tokens pair
    """
    model[category]["size"] += 1

    unique_tokens = set(tokens)
    for token in unique_tokens:
        model[category]["tokens"][token] = model[category]["tokens"].get(token, 0) + 1


def download_and_train():
    """
    Download and train the classifier on the spam and ham categories
    """
    for i, token_id in enumerate(spam_ids):
        tokens = extract_tokens(token_id, RPC_URL)
        train("spam", tokens)
        print(f"trained {token_id} as spam {i + 1}/{len(spam_ids)}")

    for i, token_id in enumerate(ham_ids):
        tokens = extract_tokens(token_id, RPC_URL)
        train("ham", tokens)
        print(f"trained {token_id} as ham {i + 1}/{len(ham_ids)}")

def clean_model():
    """
    Remove token lengths that can distract model results
    """
    for category in model:
        for token in list(model[category]["tokens"].keys()):
            if token in KEYWORDS:
                continue
            if len(token) < 2 or model[category]["tokens"][token] < 2:
                del model[category]["tokens"][token]

def main():
    download_and_train()
    clean_model()
    with open("model.json", "w") as f:
        json.dump(model, f)
    print("model saved to model.json")

if __name__ == "__main__":
  main()

