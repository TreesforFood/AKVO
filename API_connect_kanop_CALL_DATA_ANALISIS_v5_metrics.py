import requests
import json
import psycopg2
import re
import geojson
import os
from datetime import datetime


#connect to Heroku Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

CLIENT_ID = os.environ["CLIENT_ID_KANOP"]
CLIENT_SECRET = os.environ["PASSWORD_KANOP"]

# ------ Login to KANOP
token = CLIENT_SECRET
root = 'https://main.api.kanop.io'
headers = {"Authorization": f"Bearer {token}", "Accept-version": "v1"}
references = requests.get(f"{root}/projects", headers = headers)
projects_dict = references.json()
print('A: ', projects_dict)

cur.execute('''DROP TABLE IF EXISTS KANOP_analysis;''')
conn.commit()

cur.execute('''DROP TABLE IF EXISTS superset_ecosia_KANOP_polygon_level_1_moment;''')
conn.commit()

cur.execute('''CREATE TABLE superset_ecosia_KANOP_polygon_level_1_moment (
id SERIAL,
identifier_akvo TEXT,
name_project TEXT,
processing_status_site_year TEXT,
request_measurement_date DATE,
year_of_analisis INTEGER,
kanop_project_id TEXT,
forestCover_present NUMERIC(20,1),
forestCover_previous NUMERIC(20,1),
forestCover_change NUMERIC(20,1),

canopyCover_present NUMERIC(20,1),
canopyCover_previous NUMERIC(20,1),
canopyCover_change NUMERIC(20,1),

canopyHeightMean_present NUMERIC(20,1),
canopyHeightMean_previous NUMERIC(20,1),
canopyHeightMean_change NUMERIC(20,1),

treeHeightMean_present NUMERIC(20,1),
treeHeightMean_previous NUMERIC(20,1),
treeHeightMean_change NUMERIC(20,1),

livingAbovegroundBiomass_present NUMERIC(20,1),
livingAbovegroundBiomass_previous NUMERIC(20,1),
livingAbovegroundBiomass_change NUMERIC(20,1),

livingAbovegroundBiomassPerHa_present NUMERIC(20,1),
livingAbovegroundBiomassPerHa_previous NUMERIC(20,1),
livingAbovegroundBiomassPerHa_change NUMERIC(20,1),

livingBelowgroundBiomass_present NUMERIC(20,1),
livingBelowgroundBiomass_previous NUMERIC(20,1),
livingBelowgroundBiomass_change NUMERIC(20,1),

livingBelowgroundBiomassPerHa_present NUMERIC(20,1),
livingBelowgroundBiomassPerHa_previous NUMERIC(20,1),
livingBelowgroundBiomassPerHa_change NUMERIC(20,1),

livingBiomass_present NUMERIC(20,1),
livingBiomass_previous NUMERIC(20,1),
livingBiomass_change NUMERIC(20,1),

livingBiomassPerHa_present NUMERIC(20,1),
livingBiomassPerHa_previous NUMERIC(20,1),
livingBiomassPerHa_change NUMERIC(20,1),

carbon_present NUMERIC(20,1),
carbon_previous NUMERIC(20,1),
carbon_change NUMERIC(20,1),

carbonPerHa_present NUMERIC(20,1),
carbonPerHa_previous NUMERIC(20,1),
carbonPerHa_change NUMERIC(20,1),

co2eq_present NUMERIC(20,1),
co2eq_previous NUMERIC(20,1),
co2eq_change NUMERIC(20,1),

co2eqPerHa_present NUMERIC(20,1),
co2eqPerHa_previous NUMERIC(20,1),
co2eqPerHa_change NUMERIC(20,1)

);''')

conn.commit()


# Projects meta: List all your projects
project_list_overview = projects_dict['projects'] # Generates a list with dictionaries of projects. Print(project_list_overview) # Output: {'projectId': 1880, 'customerId': 254, 'name': 'APAF', 'description': '','country': 'Ivory Coast', 'projectType': 'climate', 'startDate': '2018-01-01','status': 'CREATED', 'area': 51.89, 'duration': 20, 'polygonCount': 7, 'createdAt': '2023-10-02 18:31:34.556883','updatedAt': '2023-10-02 18:32:02.459363'}
print('B: ', project_list_overview)

for project_list in project_list_overview:
    # Get details for a project
    project_id = str(project_list['projectId'])
    contract_number = project_list['name']
    country = project_list['country']
    project_status = project_list['status']
    polygon_count = project_list['polygonCount']
    area = project_list['area']

    # Get details for a specific project (with project ID)
    project_details = requests.get(f"{root}/projects/{project_id}", headers = headers)
    project_details = project_details.json()
    metrics = project_details['metrics']
    name_project = project_details.get('name')
    indicators = ','.join(metrics)
    print("indicators: ", indicators)
    #print('Metrics of a project: ', metrics) # Output: Metrics of a project:  ['forestCover', 'canopyCover', 'canopyHeightMean', 'treeHeightMean', 'livingAbovegroundBiomass', 'livingAbovegroundBiomassPerHa', 'livingBelowgroundBiomass', 'livingBelowgroundBiomassPerHa', 'livingBiomass', 'livingBiomassPerHa', 'carbon', 'carbonPerHa', 'co2eq', 'co2eqPerHa']

    # Get the aggregation levels for your project
    get_aggregation_levels = requests.get(f"{root}/projects/{project_id}/aggregationLevels",headers=headers)
    aggregation_level = get_aggregation_levels.json().get('aggregationLevels')
    #print('Aggregation levels: ', aggregation_level)

    list_of_polygon_ids = requests.get(f"{root}/projects/{project_id}/aggregationLevels/polygons",headers=headers)
    polygon_id_list = list_of_polygon_ids.json()
    polygon_id_list = polygon_id_list['polygons']
    #print(polygon_id_list)
    #print('Project ID: ', project_id, contract_number, 'Polygon ID:', polygon_id_list)

    # Get project configuration details. Nice to have. Not important for default values.
    configuration_details = requests.get(f"{root}/projects/{project_id}/configurations", headers=headers)
    project_configuration_details = configuration_details.json()
    #print(project_id, contract_number, request_id, project_configuration_details)

    # Get data on requests level (on PRODUCT AND DATE LEVEL)
    response_project_level = requests.get(f"{root}/projects/{project_id}/requests", headers=headers)
    requests_projects = response_project_level.json()
    #print('requests_projects: ',requests_projects)


    try:
        requests_projects['dataRequests'][0] #  'dataRequests' has empty list in case no processing status was provided (cancelling or completed)
    except IndexError:
        continue
    else:
        for data_requests in requests_projects['dataRequests']:
            processing_status = data_requests['status']
            request_id = data_requests['requestId']
            product = data_requests['product']
            methodology = data_requests['methodology']
            request_measurement_date = data_requests['requestMeasurementDate']


            # List of the project request details. No results in here yet, only metrics/units. Consider if this is interesting to use.
            details_data_requests = requests.get(f"{root}/projects/{project_id}/requests/{request_id}",headers=headers)
            details_data_request = details_data_requests.json()
            status_project = details_data_request.get('status')
            #print(name_project, request_measurement_date, status_project)

            # Get metrics results on PROJECT LEVEL (PER PRODUCT AND PER YEAR/DATE)
            response_project_level_metrics = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/metrics",headers=headers)
            response_project_level_metrics = response_project_level_metrics.json()
            #print('Metrics results: ', response_site_level_metrics)


            try:
                response_project_level_metrics['results']
            except KeyError:
                for polygon_id in polygon_id_list:
                    identifier_akvo = polygon_id
                    name_project = name_project
                    status_project = status_project
                    request_measurement_date = request_measurement_date
                    dt = datetime.strptime(request_measurement_date, '%Y-%m-%d')
                    year_of_analisis = dt.year
                    kanop_project_id = project_id
                    #print(project_id, name_project, status_project)
                    cur.execute('''INSERT INTO superset_ecosia_KANOP_polygon_level_1_moment (
                    identifier_akvo, name_project, processing_status_site_year, request_measurement_date, year_of_analisis, kanop_project_id)
                    VALUES (%s,%s,%s,%s,%s,%s)''',
                    (identifier_akvo, name_project, status_project, request_measurement_date, year_of_analisis, kanop_project_id))

                    conn.commit()

                continue

            # Get metrics' on POLYGON LEVEL. Here the polygon ID's can be linked with the results (results on polygon level)
            metrics_polygon_level = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/metrics/{indicators}",headers=headers)
            metrics_polygon_level = metrics_polygon_level.json()
            print("metrics_polygon_level: ", metrics_polygon_level)
            #print('metrics_polygon_level: ', name_project, status_project, project_id, request_measurement_date, metrics_polygon_level)
            metrics_polygon_level = metrics_polygon_level['results']


            for polygon_id in polygon_id_list:
                print(project_id, name_project, status_project, request_measurement_date, polygon_id)
                for k,v in metrics_polygon_level.items():
                    identifier_akvo = polygon_id
                    request_measurement_date = request_measurement_date
                    # print('TYPE: ', type(request_measurement_date))
                    # year_of_analisis = request_measurement_date.year
                    dt = datetime.strptime(request_measurement_date, '%Y-%m-%d')
                    year_of_analisis = dt.year
                    kanop_project_id = project_id

                    if k == 'canopyCover':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    canopyCover = None
                                    continue
                                else:
                                    canopyCover = value['value']

                    if k == 'forestCover':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    forestCover = None
                                    continue
                                else:
                                    forestCover = value['value']

                    if k == 'canopyHeightMean':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    canopyHeightMean = None
                                    continue
                                else:
                                    canopyHeightMean = value['value']

                    if k == 'treeHeightMean':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    treeHeightMean = value['value']

                    if k == 'livingAbovegroundBiomass':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    livingAbovegroundBiomass = value['value']
                                except KeyError:
                                    livingAbovegroundBiomass = None
                                    continue
                                else:
                                    livingAbovegroundBiomass = value['value']

                    if k == 'livingAbovegroundBiomassPerHa':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    livingAbovegroundBiomassPerHa = value['value']

                    if k == 'livingBelowgroundBiomass':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    livingBelowgroundBiomass = value['value']

                    if k == 'livingBelowgroundBiomassPerHa':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    livingBelowgroundBiomassPerHa = value['value']

                    if k == 'livingBiomass':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    livingBiomass = value['value']

                    if k == 'livingBiomassPerHa':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    livingBiomassPerHa = value['value']

                    if k == 'carbon':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    carbon = value['value']

                    if k == 'carbonPerHa':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    carbonPerHa = value['value']

                    if k == 'co2eq':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    co2eq = value['value']

                    if k == 'co2eqPerHa':
                        for value in v['averageDistribution']:
                            if value['aggregate'] == polygon_id:
                                try:
                                    value['value']
                                except KeyError:
                                    continue
                                else:
                                    co2eqPerHa = value['value']

                # Populate the KANOP table
                cur.execute('''INSERT INTO superset_ecosia_KANOP_polygon_level_1_moment (
                identifier_akvo, name_project, processing_status_site_year, request_measurement_date, year_of_analisis, kanop_project_id, forestCover_present, canopyCover_present, canopyHeightMean_present, treeHeightMean_present,
                livingAbovegroundBiomass_present,livingAbovegroundBiomassPerHa_present,livingBelowgroundBiomass_present,livingBelowgroundBiomassPerHa_present,
                livingBiomass_present,livingBiomassPerHa_present,carbon_present,carbonPerHa_present,co2eq_present,co2eqPerHa_present)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (identifier_akvo, name_project, status_project, request_measurement_date, year_of_analisis, kanop_project_id, forestCover, canopyCover, canopyHeightMean, treeHeightMean,
                livingAbovegroundBiomass,livingAbovegroundBiomassPerHa,livingBelowgroundBiomass,livingBelowgroundBiomassPerHa,
                livingBiomass,livingBiomassPerHa,carbon,carbonPerHa,co2eq,co2eqPerHa))

                conn.commit()


# Remove the processing status' and only keep the 'COMPLETED' status
cur.execute('''DELETE FROM superset_ecosia_KANOP_polygon_level_1_moment
WHERE processing_status_site_year != 'COMPLETED';''')


cur.execute('''
WITH kanop_change_table AS (SELECT
id,
identifier_akvo,
request_measurement_date,
processing_status_site_year,
kanop_project_id,

LAG(forestCover_present) OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS forestCover_previous,
forestCover_present - LAG(forestCover_present) OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS forestCover_change,

LAG(canopyCover_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS canopyCover_previous,
canopyCover_present - LAG(canopyCover_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS canopyCover_change,

LAG(canopyHeightMean_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS canopyHeightMean_previous,
canopyHeightMean_present - LAG(canopyHeightMean_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS canopyHeightMean_change,

LAG(treeHeightMean_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS treeHeightMean_previous,
treeHeightMean_present - LAG(treeHeightMean_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS treeHeightMean_change,

livingAbovegroundBiomass_present,

LAG(livingAbovegroundBiomass_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingAbovegroundBiomass_previous,
livingAbovegroundBiomass_present - LAG(livingAbovegroundBiomass_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingAbovegroundBiomass_change,

LAG(livingAbovegroundBiomassPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingAbovegroundBiomassPerHa_previous,
livingAbovegroundBiomassPerHa_present - LAG(livingAbovegroundBiomassPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingAbovegroundBiomassPerHa_change,

LAG(livingBelowgroundBiomass_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBelowgroundBiomass_previous,
livingBelowgroundBiomass_present - LAG(livingBelowgroundBiomass_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBelowgroundBiomass_change,

LAG(livingBelowgroundBiomassPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBelowgroundBiomassPerHa_previous,
livingBelowgroundBiomassPerHa_present - LAG(livingBelowgroundBiomassPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBelowgroundBiomassPerHa_change,

LAG(livingBiomass_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBiomass_previous,
livingBiomass_present - LAG(livingBiomass_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBiomass_change,

LAG(livingBiomassPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBiomassPerHa_previous,
livingBiomassPerHa_present - LAG(livingBiomassPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS livingBiomassPerHa_change,

LAG(carbon_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS carbon_previous,
carbon_present - LAG(carbon_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS carbon_change,

LAG(carbonPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS carbonPerHa_previous,
carbonPerHa_present - LAG(carbonPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS carbonPerHa_change,

LAG(co2eq_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS co2eq_previous,
co2eq_present - LAG(co2eq_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS co2eq_change,

LAG(co2eqPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS co2eqPerHa_previous,
co2eqPerHa_present - LAG(co2eqPerHa_present)
OVER (PARTITION BY identifier_akvo, kanop_project_id ORDER BY request_measurement_date) AS co2eqPerHa_change

FROM superset_ecosia_kanop_polygon_level_1_moment
ORDER BY request_measurement_date)

------------------

UPDATE superset_ecosia_kanop_polygon_level_1_moment AS a
SET
forestCover_previous = b.forestCover_previous,
forestCover_change = b.forestCover_change,
canopyCover_previous = b.canopyCover_previous,
canopyCover_change = b.canopyCover_change,
canopyHeightMean_previous = b.canopyHeightMean_previous,
canopyHeightMean_change = b.canopyHeightMean_change,
treeHeightMean_previous = b.treeHeightMean_previous,
treeHeightMean_change = b.treeHeightMean_change,
livingAbovegroundBiomass_previous = b.livingAbovegroundBiomass_previous,
livingAbovegroundBiomass_change = b.livingAbovegroundBiomass_change,
livingAbovegroundBiomassPerHa_previous = b.livingAbovegroundBiomassPerHa_previous,
livingAbovegroundBiomassPerHa_change = b.livingAbovegroundBiomassPerHa_change,
livingBelowgroundBiomass_previous = b.livingBelowgroundBiomass_previous,
livingBelowgroundBiomass_change = b.livingBelowgroundBiomass_change,
livingBelowgroundBiomassPerHa_previous = b.livingBelowgroundBiomassPerHa_previous,
livingBelowgroundBiomassPerHa_change = b.livingBelowgroundBiomassPerHa_change,
livingBiomass_previous = b.livingBiomass_previous,
livingBiomass_change = b.livingBiomass_change,
livingBiomassPerHa_previous = b.livingBiomassPerHa_previous,
livingBiomassPerHa_change = b.livingBiomassPerHa_change,
carbon_previous = b.carbon_previous,
carbon_change = b.carbon_change,
carbonPerHa_previous = b.carbonPerHa_previous,
carbonPerHa_change = b.carbonPerHa_change,
co2eq_previous = b.co2eq_previous,
co2eq_change = b.co2eq_change,
co2eqPerHa_previous = b.co2eqPerHa_previous,
co2eqPerHa_change = b.co2eqPerHa_change

FROM kanop_change_table b
WHERE a.id = b.id;''')

conn.commit()

cur.execute('''

GRANT USAGE ON SCHEMA PUBLIC TO ecosia_superset;
GRANT USAGE ON SCHEMA HEROKU_EXT TO ecosia_superset;

GRANT SELECT ON TABLE superset_ecosia_kanop_polygon_level_1_moment TO ecosia_superset;

DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_kanop_polygon_level_1_moment;

ALTER TABLE superset_ecosia_kanop_polygon_level_1_moment enable ROW LEVEL SECURITY;

CREATE POLICY ecosia_superset_policy ON superset_ecosia_kanop_polygon_level_1_moment TO ecosia_superset USING (true);''')

conn.commit()

cur.close()
