import json
import asyncio
from cnft_spam_filter import extract_tokens
import os

spam_ids = json.load(open("./spam_ids.json"))
ham_ids = json.load(open("./ham_ids.json"))

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

# train the classifier on one category/tokens pair
def train(category, tokens):
    model[category]["size"] += 1

    unique_tokens = set(tokens)
    for token in unique_tokens:
        model[category]["tokens"][token] = model[category]["tokens"].get(token, 0) + 1

# download and train the classifier on the spam and ham categories
async def download_and_train():
    for i, id in enumerate(spam_ids):
        tokens = await extract_tokens(id, os.environ.get("RPC_URL"))
        train("spam", tokens)
        print(f"trained {id} as spam {i + 1}/{len(spam_ids)}")

    for i, id in enumerate(ham_ids):
        tokens = await extract_tokens(id, os.environ.get("RPC_URL"))
        train("ham", tokens)
        print(f"trained {id} as ham {i + 1}/{len(ham_ids)}")

def clean_model():
    keywords = [
        "containsEmoji",
        "proofLengthImpossible",
        "imageContainsUrl",
        "not_containsEmoji",
        "not_proofLengthImpossible",
        "not_imageContainsUrl",
    ]

    for category in model:
        for token in list(model[category]["tokens"].keys()):
            if token in keywords:
                continue
            if model[category]["tokens"][token] < 2:
                del model[category]["tokens"][token]

async def main():
    await download_and_train()
    clean_model()
    with open("model.json", "w") as f:
        json.dump(model, f)
    print("model saved to model.json")

asyncio.run(main())
