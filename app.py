from flask import Flask, Response, request, jsonify
import simplejson as json
from pymongo import MongoClient
import pandas as pd
import csv

app = Flask(__name__)

client = MongoClient()
db = client.test

geocities = {'Bogotá': {'latitude': 4.3153343, 'longitude': -74.1796036}, 'Barranquilla': {'latitude': 10.98381, 'longitude': -74.81802}, 'Bucaramanga Metropolitana': {'latitude': 7.09011, 'longitude': -73.13137}, 'Cali': {'latitude': 3.43401, 'longitude': -76.5264664}, 'Cartagena': {'latitude': 10.4250298, 'longitude': -75.5384064}, 'Ibagué': {'latitude': 4.4350801, 'longitude': -75.2194977}, 'Manizales': {'latitude': 5.06458, 'longitude': -75.5076218}, 'Medellín': {'latitude': 6.24579, 'longitude': -75.5745926}, 'Pereira': {'latitude': 4.8052201, 'longitude': -75.6944427}, 'Valledupar': {'latitude': 10.4665403, 'longitude': -73.2510529}, 'Yumbo': {'latitude': 3.56301, 'longitude': -76.4924316}}

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

def load_geocities_data():
    with open('geocities.json',"r") as data_file:
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


@app.route('/cities_with_indicator', methods=['GET'])
def cities_with_indicator():
    indicator_name = request.args.get('indicator')
    cities_in_documents = {}
    cursor = db.test_cities.find({ "name": indicator_name })
    for document in cursor:
        cities_in_documents[document["city"]] = 1
    cities_in_documents_list = []
    for city in cities_in_documents:
        print(city)
        cities_in_documents_list.append({"city": city, "longitude": geocities[city]["longitude"], "latitude": geocities[city]["latitude"]})
    print(cities_in_documents_list)
    json_data = json.dumps(cities_in_documents_list, indent=4)
    json_fixed = json_data.replace("'",'PLACEHOLDERCOMILLAS')
    json_fixed = json_data.replace('"',"")
    json_fixed = json_data.replace('PLACEHOLDERCOMILLAS','"')
    return Response(json_fixed, mimetype='application/json')

@app.route('/data.csv', methods=['GET'])
def gen_csv():
    indicator_1 = request.args.get('indicator_1')
    city_1 = request.args.get('city_1')
    indicator_2 = request.args.get('indicator_2')
    city_2 = request.args.get('city_2')
    if indicator_1 == None or indicator_2 == None or city_1 == None or city_2 == None:
        return redirect("http://comovamos.eokoe.com/", code=302)

    #Search for indicator 1
    cursor_1 = db.test_cities.find( {"$and": [ { "city": city_1}, { "name": indicator_1} ] } )
    dict_1 = {}
    for document in cursor_1:
        value = document["value"]
        dict_1 = {}
        for level1_value in value:
            yearly_dict = {}
            if isinstance(level1_value["value"],list) == True:
                for level2_value in level1_value["value"]:
                    indicator_name = city_1+ "_" + document["description"] + "_" + level2_value["name"]
                    yearly_dict[indicator_name] = level2_value["value"]
            else:
                indicator_name = city_1 + "_" + document["description"]
                yearly_dict[indicator_name] = level1_value["value"]
            dict_1[int(level1_value["year"])] = yearly_dict

    #Search for indicator 2
    cursor_2 = db.test_cities.find( {"$and": [ { "city": city_2}, { "name": indicator_2} ] } )
    dict_2 = {}
    for document in cursor_2:
        value = document["value"]
        dict_2 = {}
        for level1_value in value:
            yearly_dict = {}
            if isinstance(level1_value["value"],list) == True:
                for level2_value in level1_value["value"]:
                    indicator_name = city_2 + "_" + document["description"] + "_" + level2_value["name"]
                    yearly_dict[indicator_name] = level2_value["value"]
            else:
                indicator_name = city_2 + "_" + document["description"]
                yearly_dict[indicator_name] = level1_value["value"]
            dict_2[int(level1_value["year"])] = yearly_dict

    timeline_publicar = [dict_1,dict_2]
    df1 = pd.DataFrame.from_dict(dict_1, orient="index")
    df2 = pd.DataFrame.from_dict(dict_2, orient="index")
    df_concat = pd.concat([df1, df2], axis=1)
    df_concat.index.name = "Años"
    csv_buffer = df_concat.to_csv(path_or_buf = None, quoting = csv.QUOTE_ALL)

    return Response(csv_buffer, mimetype='text/csv')


if __name__ == '__main__':
    app.after_request(add_cors_headers)
    app.run(threaded=True, host='0.0.0.0', debug=True)
