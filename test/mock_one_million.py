import urllib.request
import json
import time

mockaroo_url = 'https://api.mockaroo.com/api/f112b8c0?count=100&key=420877a0'

vhs_host = 'localhost'
vhs_port = 8080

vhs_url = 'http://' + vhs_host + ':' + str(vhs_port) + '/datasets'

for i in range(0, 10):
    start_time = time.time()

    response = urllib.request.urlopen(mockaroo_url)
    encoding = response.info().get_content_charset('utf8')
    data = json.loads(response.read().decode(encoding))

    elapsed_time = time.time() - start_time
    print("Iteration mockaroo: " + str(i) + "; time elapsed: " + str(elapsed_time))

    for j in range(0, 1000):
        request = urllib.request.Request(vhs_url)
        request.add_header('Content-Type', 'application/json')
        request.add_header('Authorization', '4ed2fd48-e49e-4619-ac39-f7665c4949ca')

        dict = {}
        dict['datasets'] = data

        jsondata = json.dumps(dict)
        jsondataasbytes = jsondata.encode('utf8')
        response = urllib.request.urlopen(request, jsondataasbytes)
        elapsed_time = time.time() - start_time
        if j % 50 == 0:
            print("Iteration: " + str(i) + "; SubIteration: " + str(j) + "; time elapsed: " + str(elapsed_time))

    elapsed_time = time.time() - start_time
    print("Iteration: " + str(i) + "; time elapsed in total: " + str(elapsed_time))
