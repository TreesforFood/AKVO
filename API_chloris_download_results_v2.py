from chloris_app_sdk import ChlorisAppClient
import requests
import psycopg2
import os
import json


# NOTE: BEORE RUNNING THIS SCRIPT, THE DATA NEEDS TO BE PROCESSED AND GEOMETRIC ERROR DETECTION/CORRECTION MUST BE DONE!!!

refresh_tokenD = os.environ["refresh_token_chloris"]
organisation_id = os.environ["organisation_id_chloris"]

################################## connect to Postgresql database
#connect to Heroku Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()


# THIS TABLE TS TO DOWNLOAD AND STORE ALL CHLORIS RESULTS PERMANENTLY
cur.execute('''
DROP TABLE IF EXISTS superset_ecosia_CHLORIS_polygon;
DROP TABLE IF EXISTS superset_ecosia_CHLORIS_polygon_results;

CREATE TABLE IF NOT EXISTS superset_ecosia_CHLORIS_polygon_results (
identifier_akvo TEXT,
organisation TEXT,
id_planting_site TEXT,
area_site_km2 DECIMAL,
resolution INTEGER,
contract_number NUMERIC(20,1),
year_of_analisis INTEGER,
forest_area_per_year_km2 DECIMAL,
forest_stock_per_year_mt NUMERIC(20,2),
forest_stock_end_year_mt NUMERIC(20,2),
total_stock_end_year_mt NUMERIC(20,2),
total_loss_stock_mt NUMERIC(20,2),
total_gain_stock_mt NUMERIC(20,2)
);''')

conn.commit()

# create the client
client = ChlorisAppClient(
    organization_id=organisation_id,
    refresh_token=refresh_token, # env: CHLORIS_REFRESH_TOKEN
)

reporting_units = client.list_active_sites()

for unit_id in reporting_units:
    label = unit_id['label']
    reporting_unit_id = unit_id['reportingUnitId']
    stats = client.get_reporting_unit(reporting_unit_id, include_stats=True, include_downloads=False)
    #print('STATS A: ',stats)
    # for x in stats:
    #     print('STATS B: ',x)
    for i, values in enumerate(stats["annualYears"]):
        year = stats["annualYears"][i]
        print(f"Area of the site {label}: {stats['areaKm2']} km²")
        print(f"Forest area for {label} ({year}): {stats['forest']['annualAreaKm2'][i]} km²")
        print(f"Forest stock for {label} ({year}): {stats['forest']['annualStock'][i]} MT")
        print(f"Total stock in 2024 {label}: {stats['periodChangeEndYearStock']} MT")
        print(f"Forest stock in 2024 {label}: {stats['forest']['periodChangeEndYearStock']} MT")
        print(f"Forest analysis resolution {label}: {stats.get('resolution')} meters")
        print(f"Description of planting site {label}: {stats.get('description')}")
        print(f"Total loss (2000 - 2024) {label}: {stats['periodChangeLoss']} MT")
        print(f"Total gain (2000 - 2024) {label}: {stats['periodChangeGain']} MT")
        print(f"Net percentage (2000 - 2024) {label}: {stats['periodChangePercent']}%")
        print(f"Net percentage (2000 - 2024) {label}: {stats['periodChange']}MT")
        print(f"End year {label}: {stats['periodChangeEndYear']}")

        identifier_akvo = stats.get('description')
        organisation = stats.get('description')
        name_planting_site = label
        area_site_km2 = stats['areaKm2']
        resolution = stats['resolution']
        contract_number = 0
        year_of_analisis = stats["annualYears"][i]
        forest_area_per_year_km2 = stats['forest']['annualAreaKm2'][i]
        forest_stock_per_year_mt = stats['forest']['annualStock'][i]
        forest_stock_end_year_mt = stats['forest']['periodChangeEndYearStock']
        total_stock_end_year_mt = stats['periodChangeEndYearStock']
        total_loss_stock_mt = stats['periodChangeLoss']
        total_gain_stock_mt = stats['periodChangeGain']


        #print(project_id, name_project, status_project)
        cur.execute('''INSERT INTO superset_ecosia_CHLORIS_polygon_results (identifier_akvo, organisation, id_planting_site, area_site_km2, resolution, contract_number,
        year_of_analisis, forest_area_per_year_km2, forest_stock_per_year_mt,forest_stock_end_year_mt,
        total_stock_end_year_mt, total_loss_stock_mt, total_gain_stock_mt)
        VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s)''',
        (identifier_akvo, organisation, name_planting_site, area_site_km2, resolution, contract_number,
        year_of_analisis, forest_area_per_year_km2, forest_stock_per_year_mt,forest_stock_end_year_mt,
        total_stock_end_year_mt, total_loss_stock_mt, total_gain_stock_mt))

        conn.commit()
