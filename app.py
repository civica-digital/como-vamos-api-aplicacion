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
    json_data = json.dumps(data)
    json_fixed = json_data.replace("'",'PLACEHOLDERCOMILLAS')
    json_fixed = json_data.replace('"',"'")
    json_fixed = json_data.replace('PLACEHOLDERCOMILLAS','"')
    return Response(json_fixed ,  mimetype='application/json')

@app.route('/indicator', methods=['GET'])
def indicator():
    city_name = request.args.get('city')
    indicator_name = request.args.get('indicator')
    print(city_name)
    cursor = db.test_cities.find( {"$and": [ { "city": city_name}, { "name": indicator_name } ] } )
    response_dict = {}
    timeline = []
    for document in cursor:
        response_dict["name"] = document["name"]
        response_dict["city"] = document["city"]
        response_dict["type"] = document["type"]
        print(response_dict.keys())
        timeline.append(document["value"])
    response_dict["timeline"] = timeline
    json_data = json.dumps(response_dict)
    json_fixed = json_data.replace("'",'PLACEHOLDERCOMILLAS')
    json_fixed = json_data.replace('"',"'")
    json_fixed = json_data.replace('PLACEHOLDERCOMILLAS','"')
    json_fixed = json_data.replace("'",'"')

    return Response(json_fixed,  mimetype='application/json')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
