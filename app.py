from flask import Flask, Response, request, jsonify
import simplejson as json
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient()
db = client.test

def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response
app.after_request(add_cors_headers)

def load_cities_data():
    with open('cities.json',"r") as data_file:
        data = json.load(data_file)
    return data

@app.route('/cities', methods=['GET'])
def api_city():
    data = load_cities_data()
    json_data = json.dumps(data)
    json_fixed = json_data.replace("'",'PLACEHOLDERCOMILLAS')
    json_fixed = json_data.replace('"',"")
    json_fixed = json_data.replace('PLACEHOLDERCOMILLAS','"')
    return Response(json_fixed, mimetype='application/json')

@app.route('/indicator', methods=['GET'])
def indicator():
    city_name = request.args.get('city')
    indicator_name = request.args.get('indicator')
    print(city_name[0])
    if city_name[0] == '"' or indicator_name[0] == '"':
        print("entre")
        city_name = city_name[1:-1]
        indicator_name = indicator_name[1:-1]

    print(city_name)
    cursor = db.test_cities.find( {"$and": [ { "city": city_name}, { "name": indicator_name } ] } )
    response_dict = {}
    timeline = []
    for document in cursor:
        response_dict["name"] = document["name"]
        response_dict["city"] = document["city"]
        response_dict["type"] = document["type"]
        response_dict["units"] = document["units"]
        response_dict["description"] = document["description"]
        timeline.append(document["value"])
    timeline_publicar = []
    try:
        for item in timeline[0]:
            timeline_publicar.append(item)
    except:
        timeline_publicar = []

    response_dict["timeline"] = timeline_publicar
    json_data = json.dumps(response_dict, indent=4)
    json_fixed = json_data.replace("'",'PLACEHOLDERCOMILLAS')
    json_fixed = json_data.replace('"',"")
    json_fixed = json_data.replace('PLACEHOLDERCOMILLAS','"')

    return Response(json.dumps([response_dict]), mimetype='application/json')

if __name__ == '__main__':
    app.after_request(add_cors_headers)
    app.run(host='0.0.0.0', debug=True)
