import requests
import json
import sqlite3
from requests.auth import HTTPBasicAuth
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
from dotenv import load_dotenv, find_dotenv
import os
from akvo_api_config import Config
import time


config = Config()
username = config.CONF["USERNAME_S4G"]
password = config.CONF["PASSWORD_S4G"]
response = requests.get("https://ecosia.space4good.com/dashboard/site/?page_size=100000", auth=HTTPBasicAuth(username, password), allow_redirects=True)


content_page = json.loads(response.text)



#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS S4G_API_data_general;
DROP TABLE IF EXISTS S4G_API_data_fires;
DROP TABLE IF EXISTS S4G_API_data_deforestation;

CREATE TABLE S4G_API_data_general (identifier_akvo TEXT, partner_site_id TEXT, contract_number NUMERIC(20,2), country TEXT,
nr_photos_taken INTEGER, trees_planted INTEGER, issues INTEGER, invalid_polygon BOOLEAN, invalid_point BOOLEAN, unconnected BOOLEAN,
area_too_large BOOLEAN, area_too_small BOOLEAN, overlap BOOLEAN, circumference_too_large BOOLEAN, site_not_in_country BOOLEAN, area_water_ha NUMERIC(10,2)
, area_urban_ha NUMERIC(10,2), artificially_created_polygon BOOLEAN, health_index NUMERIC(20,15), health_index_normalized NUMERIC(20,15),
health_trend NUMERIC(20,15), health_trend_normalized NUMERIC(20,15));

--CREATE TABLE S4G_API_data_fires (identifier_akvo TEXT, );

--CREATE TABLE S4G_API_data_deforestation (identifier_akvo TEXT, );


''')

conn.commit()

for i in content_page['features']:
    #print(i)
    identifier_akvo = i.get('properties')['site_id']
    partner_site_id = i.get('properties')['partner_site_id']
    contract_number = i.get('properties')['contract_number']
    country = i.get('properties')['country']
    nr_photos_taken = i.get('properties')['nr_photos_taken']
    trees_planted = i.get('properties')['trees_planted']
    issues = i.get('properties')['data_quality']['issues']
    invalid_polygon = i.get('properties')['data_quality']['invalid_polygon']
    invalid_point = i.get('properties')['data_quality']['invalid_point']
    unconnected = i.get('properties')['data_quality']['unconnected']
    area_too_large = i.get('properties')['data_quality']['area_too_large']
    area_too_small = i.get('properties')['data_quality']['area_too_small']
    overlap = i.get('properties')['data_quality']['overlap']
    circumference_too_large = i.get('properties')['data_quality']['circumference_too_large']
    site_not_in_country = i.get('properties')['data_quality']['site_not_in_country']
    area_water_ha = i.get('properties')['data_quality']['area_water_ha']
    area_urban_ha = i.get('properties')['data_quality']['area_urban_ha']

    for deforestation in i.get('properties')['deforestation_point']:
        deforestation_date = deforestation.get('detect_date')
        deforestation_area = deforestation.get('area_ha')
        deforestation_nr_alerts = deforestation.get('n_alerts')


    for fire in i.get('properties')['fires']:
        fires = fire.get('detect_date')
        confidence = fire.get('confidence')
        area_ha = fire.get('area_ha')


    artificially_created_polygon = i.get('properties')['artificially_created_polygon']
    health_index = i.get('properties')['health_indicator']['health_index']
    health_index_normalized = i.get('properties')['health_indicator']['health_index_normalized']
    health_trend = i.get('properties')['health_indicator']['health_trend']
    health_trend_normalized = i.get('properties')['health_indicator']['health_trend_normalized']

    # Populate the tree registration table
    cur.execute('''INSERT INTO S4G_API_data_general (identifier_akvo, partner_site_id, contract_number, country, nr_photos_taken, trees_planted, issues, invalid_polygon, invalid_point, unconnected, area_too_large, area_too_small, overlap, circumference_too_large,
    site_not_in_country, area_water_ha, area_urban_ha, artificially_created_polygon, health_index, health_index_normalized,
    health_trend, health_trend_normalized)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_akvo,
    partner_site_id, contract_number, country, nr_photos_taken, trees_planted, issues, invalid_polygon, invalid_point, unconnected,
    area_too_large, area_too_small, overlap, circumference_too_large, site_not_in_country, area_water_ha, area_urban_ha, artificially_created_polygon,
    health_index, health_index_normalized, health_trend, health_trend_normalized))

    conn.commit()
