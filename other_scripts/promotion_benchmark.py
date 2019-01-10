import sys
import os.path
import json
sys.path.append('../')

from lambda_functions.rightcall.text import check_promo

data = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/'
json_path = data + 'comprehend/Promo/'

promo_refs = [file.split('.')[0] for file in os.listdir(json_path)]

promo_items = {}  # {for ref in promo_ref}

for ref in promo_refs:
    with open(json_path + ref + '.json', 'r') as f:
        promo_items[ref] = json.load(f)


def test_promo_accuracy(function, data):
    print(f"Testing {function.__name__} function")
    accurately_detected = 0
    for k, v in data.items():
        promo = function(v['text'])
        print(f"{k} - function detected: {promo} -- Actual: success")
        if promo == 'success':
            accurately_detected += 1

    print(f"Success rate: {round(accurately_detected/len(data), 2) * 100}%")


if __name__ == '__main__':
    test_promo_accuracy(check_promo, promo_items)
