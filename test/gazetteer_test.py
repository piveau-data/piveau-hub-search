import urllib.request
import json
import random

ground_truth_url = 'https://www.ppe-aws.europeandataportal.eu/data/search/gazetteer/autocomplete?q='

vhs_host = 'localhost'
vhs_port = 8080

vhs_url = 'http://' + vhs_host + ':' + str(vhs_port) + '/gazetteer/autocomplete?q='

alphabet = ["A", "B", "C", "D", "E", \
    "F", "G", "H", "I", "J", \
    "K", "L", "M", "N", "O", \
    "P", "Q", "R", "S", "T", \
    "U", "V", "W", "X", "Y", "Z"]

def test(c):
    print(c)

    # print(ground_truth_url + c)
    response_gt = urllib.request.urlopen(ground_truth_url + c)
    encoding_gt = response_gt.info().get_content_charset('utf8')
    data_gt = json.loads(response_gt.read().decode(encoding_gt))['result']['results']

    # print(data_gt)

    # print(vhs_url + c)
    response_vhs = urllib.request.urlopen(vhs_url + c)
    encoding_vhs = response_vhs.info().get_content_charset('utf8')
    data_vhs = json.loads(response_vhs.read().decode(encoding_vhs))['result']['results']

    # print(data_vhs)

    passed = True

    for i in range(0, len(data_gt)):
        if data_gt[i]['name'] != data_vhs[i]['name']:
            passed = False
            print(data_gt[i]['name'])
            print(data_vhs[i]['name'])

        if data_gt[i]['featureType'] != data_vhs[i]['featureType']:
            passed = False
            print(data_gt[i]['featureType'])
            print(data_vhs[i]['featureType'])

        geo_gt = data_gt[i]['geometry'].split(',')
        geo_vhs = data_vhs[i]['geometry'].split(',')

        for j in range(0, 4):
            if round(float(geo_gt[j]), 5) != round(float(geo_vhs[j]), 5):
            # if geo_gt[j] != geo_vhs[j]:
                passed = False
                print(geo_gt[j])
                print(str(round(float(geo_gt[j]), 5)))
                print(geo_vhs[j])
                print(str(round(float(geo_vhs[j]), 5)))

        if passed == False:
            break

    if passed:
        print('passed')
        return True
    else:
        print('not passed')
        return False

for c in alphabet:
    if not test(c):
        break

for c1 in alphabet:
    if c1 < "A":
        continue
    for c2 in alphabet:
        if c1+c2 == "OR":
            continue
        if not test(c1+c2):
            break

"""for c1 in alphabet:
    for c2 in alphabet:
        for c3 in alphabet:
            if c1+c2+c3 == "AND":
                continue
            if not test(c1+c2+c3):
                break"""

for i in range(0, 1000):
    c1 = random.choice(alphabet)
    c2 = random.choice(alphabet)
    c3 = random.choice(alphabet)
    if c1+c2+c3 == "AND":
        continue
    if not test(c1+c2+c3):
        break
