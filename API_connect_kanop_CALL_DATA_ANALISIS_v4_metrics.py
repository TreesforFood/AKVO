import requests
import json
import psycopg2
import re
import geojson
import os


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

cur.execute('''DROP TABLE IF EXISTS KANOP_analysis;''')
conn.commit()
cur.execute('''DROP TABLE IF EXISTS kanop_analysis_polygon_level_1_moment;''')
conn.commit()
cur.execute('''DROP TABLE IF EXISTS superset_ecosia_KANOP_polygon_level_1_moment;''')
conn.commit()

cur.execute('''CREATE TABLE superset_ecosia_KANOP_polygon_level_1_moment (
id SERIAL,
identifier_akvo TEXT,
name_project TEXT,
processing_status_site_year TEXT,
processing_status_site_overall TEXT,
request_measurement_date DATE,
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

# cur.execute('''CREATE TABLE IF NOT EXISTS KANOP_analysis_polygon_rasterfiles (
# project,
# metrics,
# change_date,
# raster_file
#
# ;''')
#
# conn.commit()

# Projects meta: List all your projects
project_list_overview = projects_dict['projects'] # Generates a list with dictionaries of projects. Print(project_list_overview) # Output: {'projectId': 1880, 'customerId': 254, 'name': 'APAF', 'description': '','country': 'Ivory Coast', 'projectType': 'climate', 'startDate': '2018-01-01','status': 'CREATED', 'area': 51.89, 'duration': 20, 'polygonCount': 7, 'createdAt': '2023-10-02 18:31:34.556883','updatedAt': '2023-10-02 18:32:02.459363'}

# # Get the metadata for a project. Gives a list of configurations/settings used by KANOP. Consider if this is interesting to have
# meta_data_of_project = requests.get(f"{root}/projects/references",headers=headers)
# project_meta_data = meta_data_of_project.json()
# print(project_meta_data)


for project_list in project_list_overview:
    # Get details for a project
    #print('Details of project: ', project_list)
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
                    kanop_project_id = project_id
                    #print(project_id, name_project, status_project)
                    cur.execute('''INSERT INTO superset_ecosia_KANOP_polygon_level_1_moment (
                    identifier_akvo, name_project, processing_status_site_year, request_measurement_date, kanop_project_id)
                    VALUES (%s,%s,%s,%s,%s)''',
                    (identifier_akvo, name_project, status_project, request_measurement_date, kanop_project_id))

                    conn.commit()

                continue
            else:
                project_year_metrics_forest_cover = response_project_level_metrics['results']['forestCover']
                project_year_metrics_mean_canopy_height = response_project_level_metrics['results']['canopyHeightMean']
                project_year_metrics_mean_tree_height = response_project_level_metrics['results']['treeHeightMean']
                project_year_metrics_biomass = response_project_level_metrics['results']['biomass']
                project_year_metrics_carbon = response_project_level_metrics['results']['carbon']
                project_year_metrics_co2seq = response_project_level_metrics['results']['co2eq']

            # Get metrics' on POLYGON LEVEL. Here the polygon ID's can be linked with the results (results on polygon level)
            metrics_polygon_level = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/metrics/{indicators}",headers=headers)
            metrics_polygon_level = metrics_polygon_level.json()
            #print('metrics_polygon_level: ', name_project, status_project, project_id, request_measurement_date, metrics_polygon_level)
            metrics_polygon_level = metrics_polygon_level['results']


            for polygon_id in polygon_id_list:
                #print(project_id, name_project, status_project, request_measurement_date, polygon_id)
                for k,v in metrics_polygon_level.items():
                    identifier_akvo = polygon_id
                    request_measurement_date = request_measurement_date
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
                identifier_akvo, name_project, processing_status_site_year, request_measurement_date, kanop_project_id, forestCover_present, canopyCover_present, canopyHeightMean_present, treeHeightMean_present,
                livingAbovegroundBiomass_present,livingAbovegroundBiomassPerHa_present,livingBelowgroundBiomass_present,livingBelowgroundBiomassPerHa_present,
                livingBiomass_present,livingBiomassPerHa_present,carbon_present,carbonPerHa_present,co2eq_present,co2eqPerHa_present)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (identifier_akvo, name_project, status_project, request_measurement_date, kanop_project_id, forestCover, canopyCover, canopyHeightMean, treeHeightMean,
                livingAbovegroundBiomass,livingAbovegroundBiomassPerHa,livingBelowgroundBiomass,livingBelowgroundBiomassPerHa,
                livingBiomass,livingBiomassPerHa,carbon,carbonPerHa,co2eq,co2eqPerHa))

                conn.commit()


        # Get the evolution of a metric over time.
        # Get values (for all sites per submission) for each requested year for a specific indicator ON PROJECT LEVEL.
        evolution_metrics_over_time = requests.get(f"{root}/projects/{project_id}/evolution/{indicators}",headers=headers)
        evolution_metrics_over_time = evolution_metrics_over_time.json()
        #print('evolution_metrics_over_time: ' , country, contract_number, evolution_metrics_over_time) # Output: evolution_metrics_over_time:  Ivory Coast APAF {'meta': {}, 'context': {'projectId': 1880, 'indicators': ['carbon'], 'aggregationLevel': 'polygons', 'aggregationLevelValues': ['1', '2', '3', '4', '5', '6', '7'], 'timeseries': 'evolution', 'timeseriesValues': ['2019-07-01', '2022-07-01']}, 'results': {'carbon': {'averageDistribution': [{'aggregate': '2019-07-01', 'value': 2359.7, 'confidenceLowerBound': 1875.7, 'confidenceUpperBound': 2815.6}, {'aggregate': '2022-07-01', 'value': 2038.9, 'confidenceLowerBound': 1621.0, 'confidenceUpperBound': 2438.2}]}}}


        # Get the net change (variation) of an indicator over time.
        # This endpoint shows the delta (change) between 2 requested years for a specific indicator. So it shows the difference between the values of the 'evolution_metrics_over_time' variable (see end-point above)
        net_change_indicator_over_time = requests.get(f"{root}/projects/{project_id}/variation/{indicators}",headers=headers)
        net_change_indicator_over_time = net_change_indicator_over_time.json()
        #print('net_change_indicator_over_time: ', country, contract_number, net_change_indicator_over_time) # Output: net_change_indicator_over_time:  Ivory Coast APAF {'meta': {}, 'context': {'projectId': 1880, 'indicators': ['carbon'], 'aggregationLevel': 'polygons', 'aggregationLevelValues': ['1', '2', '3', '4', '5', '6', '7'], 'timeseries': 'variation', 'timeseriesValues': ['2019-07-01', '2022-07-01']}, 'results': {'carbon': {'averageDistribution': [{'aggregate': '2019-07-01'}, {'aggregate': '2022-07-01', 'value': -320.831}]}}}

        #Get the change details of an indicator over time
        change_details_indicators_over_time = requests.get(f"{root}/projects/{project_id}/change/{indicators}",headers=headers)
        change_details_indicators_over_time = change_details_indicators_over_time.json()
        #print('change_details_indicators: ', country, contract_number, change_details_indicators_over_time) # Output


        list_change_gis_files = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFiles",headers=headers)
        list_change_gis_files = list_change_gis_files.json()  #[0].get('name')

        list_gis_indicators = ['forest_cover', 'canopy_height_mean', 'tree_height_mean', 'living_aboveground_biomass_per_ha', 'living_belowground_biomass_per_ha', 'living_biomass_per_ha', 'living_biomass_carbon_stock_per_ha', 'living_biomass_CO2eq_per_ha']

        # variationGisFilesByRequest = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFilesByRequest",headers=headers)
        # variationGisFilesByRequest = variationGisFilesByRequest.json()
        # #print(project_id, variationGisFilesByRequest)
        # for variation_indicator in variationGisFilesByRequest['variationGISFilesByRequest']:
        #     for gis_indicator in list_gis_indicators:
        #         if variation_indicator['name'] == gis_indicator:
        #             variationGisFileName = variation_indicator['name']
        #             compareToRequestID = variation_indicator['compareToRequestId']
        #             compareToYear = variation_indicator['compareToYear']
                    #print('TEST: ', variationGisFileName, compareToRequestID, compareToYear)


                    # response = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFilesByRequest/{variationGisFileName}?compareToRequestId={compareToRequestID}", headers=headers)
                    # #print(type(change_gis_files)) # gives back a class 'requests.models.Response'. Check how to read this...
                    # print(response.status_code)
                    # #print(response.headers)
                    # value, params = cgi.parse_header(response.headers['content-disposition'])
                    # with open(f"{params['filename']}", "wb") as raster:
                    #     raster.write(response.content)
                    #     #raster2pgsql -s 4326 -I -C -M C:\temp\test_1.tif -t 100x100 myschema.mytable > out.sql


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
ORDER BY request_measurement_date),

------------------

kanop_processing_status_ranking AS (SELECT
id,
identifier_akvo,
request_measurement_date,
processing_status_site_year,
kanop_project_id,

CASE
WHEN processing_status_site_year ISNULL
THEN 0
WHEN processing_status_site_year = 'CANCELLED'
THEN 1
WHEN processing_status_site_year = 'REQUESTED'
THEN 2
WHEN processing_status_site_year = 'APPROVED'
THEN 3
WHEN processing_status_site_year = 'RUNNING'
THEN 4
WHEN processing_status_site_year = 'COMPLETED'
THEN 5
END AS calculation
FROM superset_ecosia_kanop_polygon_level_1_moment),

sum_results_rankings AS (SELECT
identifier_akvo,
kanop_project_id,
SUM(calculation) AS calculation
FROM kanop_processing_status_ranking
GROUP BY kanop_project_id, identifier_akvo),

classify_sum_rankings AS (SELECT
identifier_akvo,
kanop_project_id,
CASE
WHEN calculation <= 2
THEN 'CANCELLED'
WHEN calculation > 2 AND calculation <= 4
THEN 'REQUESTED'
WHEN calculation > 4 AND calculation < 10
THEN 'RUNNING'
WHEN calculation >= 10
THEN 'COMPLETED'
END AS status_processing
FROM sum_results_rankings),

kanop_processing_status_overall AS (SELECT
id,
y.identifier_akvo,
y.kanop_project_id,
request_measurement_date,
processing_status_site_year,
status_processing

FROM superset_ecosia_kanop_polygon_level_1_moment z
JOIN classify_sum_rankings y
ON y.identifier_akvo = z.identifier_akvo
AND y.kanop_project_id = z.kanop_project_id)

UPDATE superset_ecosia_kanop_polygon_level_1_moment AS a
SET
processing_status_site_overall = c.status_processing,
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

FROM
kanop_change_table b
JOIN kanop_processing_status_overall c
ON b.identifier_akvo = c.identifier_akvo
AND b.kanop_project_id = c.kanop_project_id
WHERE a.id = b.id;

''')

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
