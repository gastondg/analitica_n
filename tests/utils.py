import json


def load_json(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def save_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

class COLOR:
    BOLD   = '\033[1m'
    GREEN  = '\033[1;32m'
    YELLOW = '\033[1;33m'
    RED    = '\033[1;31m'
    CYAN   = '\033[36m'
    RESET  = '\033[0m'
