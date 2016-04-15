import os
import requests
import Levenshtein
import pandas as pd
import csv
import json
import numpy as np
from pymongo import MongoClient
from math import isnan
import json



DATADIRECTORY = "data"
OBJECTIVEDATA_STRING = "objetivo"
SUBJECTIVEDATA_STRING = "subjetivo"
OBJECTIVEDATA_VERBOSE = "indicadores"
SUBJECTIVEDATA_VERBOSE = "encuestas"
DICTIONARY_STRING = "diccionario"
DATA_STRING = "datos"


#Diccionario provisional de ciudades
cities = []
cities_pretty_name = {"barranquilla":"Barranquilla","bogota":"Bogotá", "cartagena":"Cartagena", "bucaramanga-metropolitana":"Bucaramanga Metropolitana", "cali":"Cali", "ibague": "Ibagué", "manizales": "Manizales", "medellin":"Medellín", "pereira":"Pereira", "valledupar":"Valledupar", "yumbo": "Yumbo"}
#cities_pretty_name = {"bogota":"Bogotá"}


def return_city_files(allcityfiles,city):
    cityfiles = []
    for filename in allcityfiles:
        if city in filename:
            cityfiles.append(filename)
    return(cityfiles)

def identify_data_type(cityfiles):
    datatype_byfilename = {}
    for filename in cityfiles:
        ratio_objective = Levenshtein.ratio(filename,OBJECTIVEDATA_VERBOSE)
        ratio_subjetive = Levenshtein.ratio(filename,SUBJECTIVEDATA_VERBOSE)
        if ratio_objective > ratio_subjetive:
            datatype = OBJECTIVEDATA_STRING
        else:
            datatype = SUBJECTIVEDATA_STRING
        if DICTIONARY_STRING in filename:
            filetype = DICTIONARY_STRING
        else:
            filetype = DATA_STRING
        datatype_byfilename[filename] = {"datatype":datatype,"filetype":filetype}
    return(datatype_byfilename)

def return_allcityfiles():
    allcityfiles = os.listdir(DATADIRECTORY)
    return allcityfiles

def get_data_type(files_data_type,type_string):
    dictionaries = {}
    for filename in files_data_type:
        if files_data_type[filename]["filetype"] == type_string:
            dictionaries[files_data_type[filename]["datatype"]] =filename
    return(dictionaries)

def dict_key_by_value(dict_to_search, value):
    for key in dict_to_search:
        if dict_to_search[key]==value:
            return key

def string_cleaner_for_dictionary(source_string):
    midstring_switch = str(source_string).replace("': '","MIDSTRINGSIGNAL")
    endstring_switch = midstring_switch.replace("', '", "ENDOFSTRINGSIGNAL")
    quote_removal = endstring_switch.replace("'","")
    double_quote_removal = quote_removal.replace('"',"")
    midstring_backinplace = double_quote_removal.replace("MIDSTRINGSIGNAL","': '")
    endstring_backinplace = midstring_backinplace.replace("ENDOFSTRINGSIGNAL","', '")
    doublequote_addition = "{'"+str(endstring_backinplace[1:-1])+"'}"
    switch_quote_type= doublequote_addition.replace("'",'"')
    return switch_quote_type

def extract_data_columns(year_string,variable_name,data_file):
    extracted_data = data_file[[year_string,variable_name]]
    extracted_data = extracted_data[pd.notnull(data_file[variable_name])]
    return extracted_data

def average_per_year(year_string, variable_name, filtered_data,data_type):
    data_return = []
    astype_data = filtered_data.convert_objects(convert_numeric=True)
    if data_type == "subjective":
        astype_data = astype_data[(astype_data[variable_name] >= 1.0) & (astype_data[variable_name]<=5.0)]
    ave_data = astype_data.groupby(year_string).mean()
    data_availability = ave_data[variable_name].keys()
    for key in data_availability:
        data_return.append({"year":int(key),"value":str(ave_data[variable_name][key])})
    return data_return

def responses_per_year(year_string, variable_name, filtered_data, responses_variable):
    data_return = []
    unique_years = pd.unique(filtered_data['AÑO'].ravel())
    for year in unique_years:
        yearly_sum = {}
        yearly_data = filtered_data[filtered_data[year_string]==year]
        yearly_responses = {}
        for i, year_indicator in yearly_data.iterrows():
            string_choices = year_indicator[variable_name]
            string_array_choices = string_choices.split(";")
            for choice in string_array_choices:
                if choice in yearly_responses:
                    yearly_responses[choice] = yearly_responses[choice] + 1
                else:
                    yearly_responses[choice] = 1

        for key in yearly_responses:
            try:
                yearly_sum[responses_variable[variable_name][key]] = str(yearly_responses[key])
            except:
                yearly_sum[key] = str(yearly_responses[key])

        response_list = []
        for key in yearly_sum:
            response_list.append({"name":key, "value":yearly_sum[key]})

        data_return.append({"year":int(year),"value":response_list})
    return data_return

def clean_description(description):
    description_list = description.split(".")
    description_clean_join = description
    if len(description_list) > 1:
        description_list.pop(0)
        description_clean_join = ". ".join(description_list).strip()
    return description_clean_join

def DictListUpdate( dict1, dict2):
    for key in dict2:
        if key not in dict1:
            dict1[key] = dict2[key]
    return dict1

def extract_city_variableinfo(files_data_type,output_json,city,responses):
    dictionaries = get_data_type(files_data_type,DICTIONARY_STRING)
    objective_dictionary = pd.read_csv(DATADIRECTORY + "/" + dictionaries[OBJECTIVEDATA_STRING],delimiter=",", encoding="utf-8", dtype=np.string_ )
    subjective_dictionary = pd.read_csv(DATADIRECTORY + "/" + dictionaries[SUBJECTIVEDATA_STRING],delimiter=",", encoding="utf-8", dtype=np.string_ )

    output_json.append({"name":cities_pretty_name[city], "categories": []})
    output_json[-1]["categories"] = []

    rings = list(objective_dictionary.anillo.unique())

    for extra_ring in list(subjective_dictionary.dimension.unique()):
        if extra_ring not in rings:
            rings.append(extra_ring)

    category_position_index = {}
    for ring in rings:
    	if ring != "Indentificación Base de Datos":
	        output_json[-1]["categories"].append({"name" : ring,"indicators" : []})
	        category_position_index[ring] = len(output_json[-1]["categories"])-1

    units_per_variable = {}
    description_per_variable = {}
    ## Filling objective data
    for i, objective_dictionary_row in objective_dictionary.iterrows():
        if objective_dictionary_row["id"]==objective_dictionary_row["Indicador"]: next
        indicator_category = objective_dictionary_row["anillo"]
        if indicator_category == "Indentificación Base de Datos":
        	indicator_category = "Extra"
        category_position = category_position_index[indicator_category]
        current_indicator_data = {"name" : objective_dictionary_row["id"], "type":"objetivo", "description": objective_dictionary_row["Indicador"]}
        output_json[-1]["categories"][category_position_index[indicator_category]]["indicators"].append(current_indicator_data)

        units_per_variable[objective_dictionary_row["id"]] = objective_dictionary_row["unidad"]
        description_per_variable[objective_dictionary_row["id"]] = objective_dictionary_row["Indicador"]


    # Filling subjective data
    responses_by_variable = {}
    for i, subjective_dictionary_row in subjective_dictionary.iterrows():
        indicator_category = subjective_dictionary_row["dimension"]
        category_position = category_position_index[indicator_category]
        if subjective_dictionary_row["tipo_respuestas"] == "ordinal":
            data_type = "subjetivo ordinal"
            units_per_variable[subjective_dictionary_row["variable"]] = "promedio"
        else:
            data_type = "subjetivo categorico"
            units_per_variable[subjective_dictionary_row["variable"]] = "opiniones"
        current_indicator_data = {"name" : subjective_dictionary_row["variable"], "type":data_type, "description": clean_description(subjective_dictionary_row["descripcion"])}
        output_json[-1]["categories"][category_position_index[indicator_category]]["indicators"].append(current_indicator_data)

        clean_response_string = string_cleaner_for_dictionary(subjective_dictionary_row["respuestas"])

        description_per_variable[subjective_dictionary_row["variable"]] = subjective_dictionary_row["descripcion"]

        try:
            responses_by_variable[subjective_dictionary_row["variable"]] = json.loads(clean_response_string)
        except:
            responses_by_variable[subjective_dictionary_row["variable"]] = { "0": "NaN"}

    responses = DictListUpdate(responses_by_variable, responses)
    responses_df = pd.DataFrame(responses)
    responses_df .to_csv("Responses.csv")
    return output_json, responses, units_per_variable, description_per_variable

def generate_city_data():
    client = MongoClient()
    db = client.test

    allcityfiles = return_allcityfiles()
    output_variable_json = []
    responses = {}
    for city in cities_pretty_name:
        print("Cargando Variables de " + city)
        city_files = return_city_files(allcityfiles,city)
        files_data_type =identify_data_type(city_files)
        output_variable_json, responses,  units, description = extract_city_variableinfo(files_data_type,output_variable_json,city,responses)


    with open('cities.json', 'w') as fp:
        json.dump(output_variable_json, fp)

    with open ("cities.json", "r") as myfile:
        data=myfile.read()

    data = data.replace("NaN", '"NaN"')
    data_load = json.loads(data)
    cities_clean = []
    for city in data_load:
        city_clean = {}
        categories = city["categories"]
        city_clean["name"] = city["name"]
        categories_clean = []
        for category in categories:
            category_clean = {}
            if category["name"] != "NaN":
                category_clean["name"] = category["name"]
                category_clean["indicators"] = category["indicators"]
                categories_clean.append(category)
        city_clean["categories"] = categories_clean
        cities_clean.append(city_clean)


    with open ("cities.json", "w") as myfile:
        json.dump(cities_clean, myfile)

    allcityfiles = return_allcityfiles()
    for city_dictionary in output_variable_json:
        city_pretty = city_dictionary["name"]
        print("Cargando Variables de " + city_pretty)
        city_plain_name = dict_key_by_value(cities_pretty_name,city_pretty)

        city_files = return_city_files(allcityfiles,city_plain_name)
        files_data_type =identify_data_type(city_files)
        data_files = get_data_type(files_data_type,DATA_STRING)

        print(data_files)
        objective_data = pd.read_csv(DATADIRECTORY + "/" + data_files[OBJECTIVEDATA_STRING],delimiter=",", encoding="utf-8", dtype=np.string_ )
        subjective_data = pd.read_csv(DATADIRECTORY + "/" + data_files[SUBJECTIVEDATA_STRING],delimiter=",", encoding="utf-8", dtype=np.string_ )


        for category_data in city_dictionary["categories"]:
            for indicator_data in category_data["indicators"]:
                variable_name = indicator_data["name"]
                variable_type = indicator_data["type"]
                variable_description = indicator_data["description"]
                try:
                    if variable_type == "objetivo":
                        extracted_data = extract_data_columns("ANIO",variable_name,objective_data)
                        values = average_per_year("ANIO",variable_name,extracted_data,"objective")
                    elif variable_type == "subjetivo ordinal":
                        extracted_data = extract_data_columns("AÑO",variable_name,subjective_data)
                        values = average_per_year("AÑO",variable_name,extracted_data,"subjective")
                    else:
                        extracted_data = extract_data_columns("AÑO",variable_name,subjective_data)
                        values = responses_per_year("AÑO",variable_name,extracted_data,responses)
                except:
                    if variable_type == "objetivo":
                        values = [{"year":int(2014), "value": "0"}]
                    else:
                        values = [{"year":int(2014),"value":[{"Caso especial de los datos": "0"}]}]
                try:
                    variable_units = units[variable_name]
                    if variable_units is np.nan:
                        variable_units = "NaN"
                except:
                    variable_units = "NaN"

                return_dict = {"name":variable_name, "city":city_pretty, "type":variable_type, "value":values, "units": variable_units, "description":variable_description}
                db.test_cities.insert_one(return_dict)
    return "Success"

generate_city_data()
