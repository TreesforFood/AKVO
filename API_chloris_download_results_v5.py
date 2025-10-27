from chloris_app_sdk import ChlorisAppClient
import requests
import psycopg2
import os
import json
import re


# NOTE: BEORE RUNNING THIS SCRIPT, THE DATA NEEDS TO BE PROCESSED AND GEOMETRIC ERROR DETECTION/CORRECTION MUST BE DONE!!!

# NOTE: BEORE RUNNING THIS SCRIPT, THE DATA NEEDS TO BE PROCESSED AND GEOMETRIC ERROR DETECTION/CORRECTION MUST BE DONE!!!

refresh_token = os.environ["refresh_token_chloris"]
organisation_id = os.environ["organisation_id_chloris"]

################################## connect to Postgresql database
#connect to Heroku Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()


cur.execute('''
DROP TABLE IF EXISTS superset_ecosia_CHLORIS_polygon;
DROP TABLE IF EXISTS superset_ecosia_CHLORIS_polygon_results;''')
conn.commit()

# THIS TABLE IS TO DOWNLOAD AND STORE ALL CHLORIS RESULTS PERMANENTLY
cur.execute('''CREATE TABLE IF NOT EXISTS superset_ecosia_CHLORIS_polygon_results (
identifier_akvo TEXT,
organisation TEXT,
id_planting_site TEXT,
area_site_km2 DECIMAL,
resolution INTEGER,
contract_number NUMERIC(20,2),
year_of_analisis INTEGER,
forest_area_per_year_km2 DECIMAL,
forest_agb_stock_per_year_mt NUMERIC(20,4),
forest_stock_start_year_mt NUMERIC(20,4),
forest_stock_end_year_mt NUMERIC(20,4),
total_stock_end_year_mt NUMERIC(20,4),
avg_density_end_year_mt NUMERIC(20,4),
total_loss_stock_mt NUMERIC(20,4),
total_gain_stock_mt NUMERIC(20,4)
);''')

conn.commit()

# create the client
client = ChlorisAppClient(
    organization_id=organisation_id,
    refresh_token=refresh_token, # env: CHLORIS_REFRESH_TOKEN
)

reporting_units = client.list_active_sites()
#print('Higest level: ', reporting_units)

for unit_id in reporting_units:
    #print('ReportingUnitS level: ', unit_id)
    label_identifier = unit_id['label']

    reporting_unit_id = unit_id['reportingUnitId']
    stats = client.get_reporting_unit(reporting_unit_id, include_stats=True, include_downloads=False)
    #print('STATS GGGG: ',stats)
    # for x in stats:
    #     print('STATS B: ',x)
    try:
        stats["annualYears"]
        print('THROUGH: ', stats["annualYears"])
    except KeyError:
        continue
    else:
        for i, values in enumerate(stats["annualYears"]):
            year = stats["annualYears"][i]
            print('YEAR: ', year)
            # print(f"Area of the site ({label_identifier}): {stats['areaKm2']} km²")
            # print(f"Forest area for {label_identifier} ({year}): {stats['forest']['annualAreaKm2'][i]} km²")
            # print(f"Forest stock for {label_identifier} ({year}): {stats['forest']['annualStock'][i]} MT")
            # print(f"Total stock in 2024 {label_identifier}: {stats['periodChangeEndYearStock']} MT")
            # print(f"Forest stock in 2024 {label_identifier}: {stats['forest']['periodChangeEndYearStock']} MT")
            # print(f"Forest analysis resolution {label_identifier}: {stats.get('resolution')} meters")
            # print(f"Description of planting site {label_identifier}: {stats.get('description')}")
            # print(f"Total loss (2000 - 2024) {label_identifier}: {stats['periodChangeLoss']} MT")
            # print(f"Total gain (2000 - 2024) {label_identifier}: {stats['periodChangeGain']} MT")
            # print(f"Net percentage (2000 - 2024) {label_identifier}: {stats['periodChangePercent']}%")
            # print(f"Net percentage (2000 - 2024) {label_identifier}: {stats['periodChange']}MT")
            # print(f"End year {label_identifier}: {stats['periodChangeEndYear']}")
            # print(f"Forest agb annual stock {label_identifier}: {stats['annualStock']}")

            identifier_akvo = label_identifier

            description = stats.get('description')
            #print('Desciption label: ', description)

            # Get name of organisation from description label
            if description is not None:
                count_index_organisation = description.find(' :: ')
                if count_index_organisation != -1 and count_index_organisation > 0:  # Check if '-' is found and is not the first character
                    name_organisation = description[:count_index_organisation]
                else:
                    name_organisation = description

            else:
                name_organisation = None

            organisation = name_organisation

            # Get contract number from description label
            if description is not None:
                for match in re.finditer(' :: ', description):
                    truncate_contract_begin = match.end()
                for match in re.finditer(' :-: ', description):
                    truncate_contract_end = match.end()-5

                try:
                    contract_number = float(description[truncate_contract_begin:truncate_contract_end])
                    print(type(contract_number))
                except ValueError:
                    contract_number = None


            # Get planting site id from description label
            if description is not None:
                for match in re.finditer(' :-: ', description):
                    truncate_planting_site = match.end()
                    #print('Number planting site truncate:', truncate_planting_site)
                    if truncate_planting_site is not None:
                        name_planting_site = description[truncate_planting_site:]
                    else:
                        name_planting_site = None

            area_site_km2 = stats['areaKm2']
            resolution = stats['resolution']
            year_of_analisis = stats["annualYears"][i]
            forest_area_per_year_km2 = stats['forest']['annualAreaKm2'][i]
            forest_agb_stock_per_year_mt = stats['annualStock'][i]
            forest_stock_start_year_mt = stats['periodChangeStartYearStock']
            forest_stock_end_year_mt = stats['periodChangeEndYearStock']
            total_stock_end_year_mt = stats['periodChangeEndYearStock']
            avg_density_end_year_mt = stats['periodChangeEndYearStock']
            total_loss_stock_mt = stats['periodChangeLoss']
            total_gain_stock_mt = stats['periodChangeGain']


            #print(project_id, name_project, status_project)
            cur.execute('''INSERT INTO superset_ecosia_CHLORIS_polygon_results (identifier_akvo, organisation, id_planting_site, area_site_km2, resolution, contract_number,
            year_of_analisis, forest_area_per_year_km2, forest_agb_stock_per_year_mt,forest_stock_start_year_mt, forest_stock_end_year_mt,
            total_stock_end_year_mt, avg_density_end_year_mt, total_loss_stock_mt, total_gain_stock_mt)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (identifier_akvo, organisation, name_planting_site, area_site_km2, resolution, contract_number,
            year_of_analisis, forest_area_per_year_km2, forest_agb_stock_per_year_mt,forest_stock_start_year_mt, forest_stock_end_year_mt,
            total_stock_end_year_mt, avg_density_end_year_mt, total_loss_stock_mt, total_gain_stock_mt))

            conn.commit()
