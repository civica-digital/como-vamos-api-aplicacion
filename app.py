from flask import Flask, Response, request
import simplejson as json
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient()
db = client.test

def load_cities_data():
    with open('cities.json',"r") as data_file:
        data = json.load(data_file)
    return data


@app.route('/cities', methods=['GET'])
def api_city():
    data = load_cities_data()
    return Response(json.dumps(data),  mimetype='application/json')

@app.route('/indicator', methods=['GET'])
def indicator():
    city_name = request.args.get('city')
    variable_name = request.args.get('variable')
    print(city_name)
    cursor = db.test_cities.find( {"$and": [ { "city": city_name}, { "name": variable_name } ] } )
    response_dict = {}
    timeline = []
    for document in cursor:
        response_dict["name"] = document["name"]
        response_dict["city"] = document["city"]
        response_dict["type"] = document["type"]
        print(response_dict.keys())
        timeline.append(document["value"])
    response_dict["timeline"] = timeline
    print(response_dict)

    return Response(json.dumps(response_dict),  mimetype='application/json')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
