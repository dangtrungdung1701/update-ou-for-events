import os
import json


def save_json(data, path):
    folder = os.path.dirname(path)
    if folder:  # only create if folder is not empty
        os.makedirs(folder, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
