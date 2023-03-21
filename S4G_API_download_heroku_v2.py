import requests
import json
from requests.auth import HTTPBasicAuth
import re
import geodaisy.converters as convert
import psycopg2
import os


username = os.environ["USERNAME_S4G"]
password = os.environ["PASSWORD_S4G"]
response = requests.get("https://ecosia.space4good.com/dashboard/site/?page_size=100000", auth=HTTPBasicAuth(username, password), allow_redirects=True)
response_landcover = requests.get("https://ecosia.space4good.com/dashboard/landcover/?page_size=10000", auth=HTTPBasicAuth(username, password), allow_redirects=True)

#Processing status indicator done, can be checked with:
#https://ecosia.space4good.com/dashboard/processingstatus/?data_quality_check_issues_gt_0=true&site__country_code=per
content_page = json.loads(response.text)
content_page_landcover = json.loads(response_landcover.text)

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')

cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS S4G_API_data_quality_health;
DROP TABLE IF EXISTS S4G_API_data_fires;
DROP TABLE IF EXISTS S4G_API_fires;
DROP TABLE IF EXISTS S4G_API_data_deforestation;
DROP TABLE IF EXISTS S4G_API_deforestation;
DROP TABLE IF EXISTS S4G_API_NDVI_timeseries;
DROP TABLE IF EXISTS S4G_API_health_timeseries;
DROP TABLE IF EXISTS S4G_API_landcover_change;
DROP TABLE IF EXISTS S4G_NDVI_timeseries;
DROP TABLE IF EXISTS S4G_health_timeseries;
DROP TABLE IF EXISTS S4G_landcover_change;

CREATE TABLE S4G_API_data_quality_health (identifier_akvo TEXT, partner_site_id TEXT, contract_number NUMERIC(20,2), country TEXT,
nr_photos_taken INTEGER, trees_planted INTEGER, issues INTEGER, invalid_polygon BOOLEAN, invalid_point BOOLEAN, unconnected BOOLEAN,
area_too_large BOOLEAN, area_too_small BOOLEAN, overlap BOOLEAN, circumference_too_large BOOLEAN, site_not_in_country BOOLEAN, area_water_ha NUMERIC(10,2)
, area_urban_ha NUMERIC(10,2), artificially_created_polygon BOOLEAN, health_index NUMERIC, health_index_normalized NUMERIC,
health_trend NUMERIC, health_trend_normalized NUMERIC);

CREATE TABLE S4G_API_NDVI_timeseries (identifier_akvo TEXT, date DATE, ndvi_timeseries DECIMAL);

CREATE TABLE S4G_API_health_timeseries (identifier_akvo TEXT, date DATE, health_timeseries DECIMAL);

CREATE TABLE S4G_API_landcover_change (identifier_akvo TEXT, year INTEGER, month INTEGER, water DECIMAL, trees DECIMAL, grass DECIMAL, flooded_vegetation DECIMAL,
crops DECIMAL, shrub_scrub DECIMAL, built DECIMAL, bare DECIMAL, snow_ice DECIMAL);

CREATE TABLE S4G_API_fires (identifier_akvo TEXT, detection_date DATE, confidence_level NUMERIC(3,2), area_ha NUMERIC(10,3));

CREATE TABLE S4G_API_deforestation (identifier_akvo TEXT, deforestation_date DATE, deforestation_area NUMERIC(10,3), deforestation_nr_alerts NUMERIC(20,2));

''')

conn.commit()

#print(content_page)
#print(content_page_landcover)

for i in content_page['features']:
    print('XXXXX:   ',i)
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

        cur.execute('''INSERT INTO S4G_API_deforestation (identifier_akvo, deforestation_date, deforestation_area, deforestation_nr_alerts)
        VALUES (%s,%s,%s,%s)''', (identifier_akvo,
        deforestation_date, deforestation_area, deforestation_nr_alerts))

        conn.commit()

    for fire in i.get('properties')['fires']:
        detection_date = fire.get('detect_date')
        confidence = fire.get('confidence')
        area_ha = fire.get('area_ha')

        cur.execute('''INSERT INTO S4G_API_fires (identifier_akvo, detection_date, confidence_level, area_ha)
        VALUES (%s,%s,%s,%s)''', (identifier_akvo, detection_date, confidence, area_ha))

        conn.commit()

    # INGETRATE NDVI time series and heath time series!!!!!
    if i.get('properties')['health_indicator'] is not None:
        if i.get('properties')['health_indicator']['NDVI_timeseries'] is not None:
            NDVI_dict = i.get('properties')['health_indicator']['NDVI_timeseries']
            for date, ndvi_value in NDVI_dict.items():
                #print("TEST: ", identifier_akvo, date, ndvi_value)

                cur.execute('''INSERT INTO S4G_API_NDVI_timeseries (identifier_akvo, date, ndvi_timeseries)
                VALUES (%s,%s,%s)''', (identifier_akvo, date, ndvi_value))

                conn.commit()

        if i.get('properties')['health_indicator']['health_timeseries'] is not None:
            health_dict = i.get('properties')['health_indicator']['health_timeseries']
            for date, health_value in health_dict.items():
                #print("TEST: ", identifier_akvo, date, ndvi_value)

                cur.execute('''INSERT INTO S4G_API_health_timeseries (identifier_akvo, date, health_timeseries)
                VALUES (%s,%s,%s)''', (identifier_akvo, date, health_value))

                conn.commit()

        artificially_created_polygon = i.get('properties')['artificially_created_polygon']
        health_index = i.get('properties')['health_indicator']['health_index']
        health_index_normalized = i.get('properties')['health_indicator']['health_index_normalized']
        health_trend = i.get('properties')['health_indicator']['health_trend']
        health_trend_normalized = i.get('properties')['health_indicator']['health_trend_normalized']

        # Populate the tree registration table
        cur.execute('''INSERT INTO S4G_API_data_quality_health (identifier_akvo, partner_site_id, contract_number, country, nr_photos_taken, trees_planted, issues, invalid_polygon, invalid_point, unconnected, area_too_large, area_too_small, overlap, circumference_too_large,
        site_not_in_country, area_water_ha, area_urban_ha, artificially_created_polygon, health_index, health_index_normalized,
        health_trend, health_trend_normalized)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_akvo,
        partner_site_id, contract_number, country, nr_photos_taken, trees_planted, issues, invalid_polygon, invalid_point, unconnected,
        area_too_large, area_too_small, overlap, circumference_too_large, site_not_in_country, area_water_ha, area_urban_ha, artificially_created_polygon,
        health_index, health_index_normalized, health_trend, health_trend_normalized))

        conn.commit()

for j in content_page_landcover['features']:
    #print("LANDCOVER: ", j)
    identifier_akvo = j.get('properties')['site']['site_id']
    year = j.get('properties')['year']
    month = j.get('properties')['month']
    water = j.get('properties')['water']
    trees = j.get('properties')['trees']
    grass = j.get('properties')['grass']
    flooded_vegetation = j.get('properties')['flooded_vegetation']
    crops = j.get('properties')['crops']
    shrub_scrub = j.get('properties')['shrub_and_scrub']
    built = j.get('properties')['built']
    bare = j.get('properties')['bare']
    snow_ice = j.get('properties')['snow_and_ice']

    cur.execute('''INSERT INTO S4G_API_landcover_change (identifier_akvo, year, month, water, trees, grass, flooded_vegetation,
    crops, shrub_scrub, built, bare, snow_ice)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_akvo, year, month, water, trees, grass, flooded_vegetation,
    crops, shrub_scrub, built, bare, snow_ice))

    conn.commit()
