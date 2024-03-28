import requests
import json
import psycopg2
import cgi
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

cur.execute('''DROP TABLE IF EXISTS KANOP_analysis_polygon_level_1_moment;''')
cur.execute('''DROP TABLE IF EXISTS superset_ecosia_KANOP_polygon_level_1_moment;''')

conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS superset_ecosia_KANOP_polygon_level_1_moment (
identifier_akvo TEXT,
request_measurement_date DATE,
kanop_project_id TEXT,
forestCover NUMERIC(20,1),
canopyCover NUMERIC(20,1),
canopyHeightMean NUMERIC(20,1),
treeHeightMean NUMERIC(20,1),
livingAbovegroundBiomass NUMERIC(20,1),
livingAbovegroundBiomassPerHa NUMERIC(20,1),
livingBelowgroundBiomass NUMERIC(20,1),
livingBelowgroundBiomassPerHa NUMERIC(20,1),
livingBiomass NUMERIC(20,1),
livingBiomassPerHa NUMERIC(20,1),
carbon NUMERIC(20,1),
carbonPerHa NUMERIC(20,1),
co2eq NUMERIC(20,1),
co2eqPerHa NUMERIC(20,1));''')

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
    #print(metrics)
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
            #print(contract_number, details_data_request)
            #details_data_request = details_data_request.get('metrics')[10]


            # Get metrics results on PROJECT LEVEL (PER PRODUCT AND PER YEAR/DATE)
            response_project_level_metrics = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/metrics",headers=headers)
            response_project_level_metrics = response_project_level_metrics.json()
            #print('Metrics results: ', response_site_level_metrics)

            try:
                response_project_level_metrics['results']
                #print('Project ID: ', project_id, contract_number, 'request_id: ', request_id, 'response_site_level_metrics: ', response_site_level_metrics['results']) # Output: Project ID:  1880 APAF request_id:  cb1631a7-f9f4-407e-84a1-db6fa9087ed8 response_site_level_metrics:  {'forestCover': 73.58588194556484, 'canopyHeightMean': 3.854736898602207, 'treeHeightMean': 6.7732785701801745, 'biomass': 4234, 'carbon': 2038, 'co2eq': 7475.8723846077655}
                # Project ID:  1880 APAF request_id:  6fb1e7b6-662f-485f-80ea-2a1beb70d2a4 response_site_level_metrics:  {'forestCover': 68.84960117740289, 'canopyHeightMean': 4.0949824766278295, 'treeHeightMean': 6.956564657732989, 'biomass': 4891, 'carbon': 2359, 'co2eq': 8652.252806614346}
            except KeyError:
                continue
            else:
                project_year_metrics_forest_cover = response_project_level_metrics['results']['forestCover']
                project_year_metrics_mean_canopy_height = response_project_level_metrics['results']['canopyHeightMean']
                project_year_metrics_mean_tree_height = response_project_level_metrics['results']['treeHeightMean']
                project_year_metrics_biomass = response_project_level_metrics['results']['biomass']
                project_year_metrics_carbon = response_project_level_metrics['results']['carbon']
                project_year_metrics_co2seq = response_project_level_metrics['results']['co2eq']
                #print('Project ID: ', project_id, contract_number, 'request_id: ', request_id, 'request_measurement_date: ', request_measurement_date, 'site_year_metrics_forest_cover: ', site_year_metrics_forest_cover, 'site_year_metrics_mean_canopy_height: ', site_year_metrics_mean_canopy_height, 'site_year_metrics_mean_tree_height: ', site_year_metrics_mean_tree_height, 'site_year_metrics_carbon: ', site_year_metrics_carbon)
                # Output: Project ID:  1880 APAF request_id:  cb1631a7-f9f4-407e-84a1-db6fa9087ed8 request_measurement_date:  2022-07-01 site_year_metrics_forest_cover:  73.58588194556484 site_year_metrics_mean_canopy_height:  3.854736898602207 site_year_metrics_mean_tree_height:  6.7732785701801745 site_year_metrics_carbon:  2038
                # Project ID:  1880 APAF request_id:  6fb1e7b6-662f-485f-80ea-2a1beb70d2a4 request_measurement_date:  2019-07-01 site_year_metrics_forest_cover:  68.84960117740289 site_year_metrics_mean_canopy_height:  4.0949824766278295 site_year_metrics_mean_tree_height:  6.956564657732989 site_year_metrics_carbon:  2359

            # Get metrics' on POLYGON LEVEL. Here the polygon ID's can be linked with the results (results on polygon level)
            metrics_polygon_level = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/metrics/{indicators}",headers=headers)
            metrics_polygon_level = metrics_polygon_level.json()
            #print('metrics_polygon_level: ', project_id, request_measurement_date, metrics_polygon_level)
            metrics_polygon_level = metrics_polygon_level['results']
            #print(type(metrics_polygon_level))

            for polygon_id in polygon_id_list:
                #print(project_id, request_measurement_date, polygon_id)
                for k,v in metrics_polygon_level.items():
                    #print(k,v['averageDistribution'])
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
                identifier_akvo,request_measurement_date, kanop_project_id, forestCover, canopyCover, canopyHeightMean, treeHeightMean,
                livingAbovegroundBiomass,livingAbovegroundBiomassPerHa,livingBelowgroundBiomass,livingBelowgroundBiomassPerHa,
                livingBiomass,livingBiomassPerHa,carbon,carbonPerHa,co2eq,co2eqPerHa)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (identifier_akvo,request_measurement_date, kanop_project_id, forestCover, canopyCover, canopyHeightMean, treeHeightMean,
                livingAbovegroundBiomass,livingAbovegroundBiomassPerHa,livingBelowgroundBiomass,livingBelowgroundBiomassPerHa,
                livingBiomass,livingBiomassPerHa,carbon,carbonPerHa,co2eq,co2eqPerHa))

                conn.commit()


        # # Get the evolution of a metric over time.
        # # Get values (for all sites per submission) for each requested year for a specific indicator ON PROJECT LEVEL.
        # evolution_metrics_over_time = requests.get(f"{root}/projects/{project_id}/evolution/{indicators}",headers=headers)
        # evolution_metrics_over_time = evolution_metrics_over_time.json()
        # #print('evolution_metrics_over_time: ' , country, contract_number, evolution_metrics_over_time) # Output: evolution_metrics_over_time:  Ivory Coast APAF {'meta': {}, 'context': {'projectId': 1880, 'indicators': ['carbon'], 'aggregationLevel': 'polygons', 'aggregationLevelValues': ['1', '2', '3', '4', '5', '6', '7'], 'timeseries': 'evolution', 'timeseriesValues': ['2019-07-01', '2022-07-01']}, 'results': {'carbon': {'averageDistribution': [{'aggregate': '2019-07-01', 'value': 2359.7, 'confidenceLowerBound': 1875.7, 'confidenceUpperBound': 2815.6}, {'aggregate': '2022-07-01', 'value': 2038.9, 'confidenceLowerBound': 1621.0, 'confidenceUpperBound': 2438.2}]}}}
        #
        # # Get the net change (variation) of an indicator over time.
        # # This endpoint shows the delta (change) between 2 requested years for a specific indicator. So it shows the difference between the values of the 'evolution_metrics_over_time' variable (see end-point above)
        # net_change_indicator_over_time = requests.get(f"{root}/projects/{project_id}/variation/{indicators}",headers=headers)
        # net_change_indicator_over_time = net_change_indicator_over_time.json()
        # #print('net_change_indicator_over_time: ', country, contract_number, net_change_indicator_over_time) # Output: net_change_indicator_over_time:  Ivory Coast APAF {'meta': {}, 'context': {'projectId': 1880, 'indicators': ['carbon'], 'aggregationLevel': 'polygons', 'aggregationLevelValues': ['1', '2', '3', '4', '5', '6', '7'], 'timeseries': 'variation', 'timeseriesValues': ['2019-07-01', '2022-07-01']}, 'results': {'carbon': {'averageDistribution': [{'aggregate': '2019-07-01'}, {'aggregate': '2022-07-01', 'value': -320.831}]}}}

        # #Get the change details of an indicator over time
        # change_details_indicators_over_time = requests.get(f"{root}/projects/{project_id}/change/{indicators}",headers=headers)
        # change_details_indicators_over_time = change_details_indicators_over_time.json()
        # #print('change_details_indicators: ', country, contract_number, change_details_indicators_over_time) # Output
        #
        #
        # list_change_gis_files = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFiles",headers=headers)
        # list_change_gis_files = list_change_gis_files.json()  #[0].get('name')
        # #print(len(list_change_gis_files[0]))
        # #if len(list_change_gis_files)>0:
        # #print(country, organisation, list_change_gis_files['variationGISFiles'])
        #
        # #list_stock_change_gis_files = requests.post(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFiles/{variationGisFileName}?referenceYear={referenceYear}&versusYear={versusYear}",headers=headers)
        # #print(list_stock_change_gis_files.json())
        #
        # #response = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFiles/{variationGisFileName}?referenceYear={referenceYear}&versusYear={versusYear}",headers=headers)
        # #print(response.status_code)
        # #print(response.json())
        #
        # list_gis_indicators = ['forest_cover', 'canopy_height_mean', 'tree_height_mean', 'living_aboveground_biomass_per_ha', 'living_belowground_biomass_per_ha', 'living_biomass_per_ha', 'living_biomass_carbon_stock_per_ha', 'living_biomass_CO2eq_per_ha']
        #
        # variationGisFilesByRequest = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFilesByRequest",headers=headers)
        # variationGisFilesByRequest = variationGisFilesByRequest.json()
        # #print(project_id, variationGisFilesByRequest)
        # for variation_indicator in variationGisFilesByRequest['variationGISFilesByRequest']:
        #     for gis_indicator in list_gis_indicators:
        #         if variation_indicator['name'] == gis_indicator:
        #             variationGisFileName = variation_indicator['name']
        #             compareToRequestID = variation_indicator['compareToRequestId']
        #             compareToYear = variation_indicator['compareToYear']
        #             #print('TEST: ', variationGisFileName, compareToRequestID, compareToYear)


                    # response = requests.get(f"{root}/projects/{project_id}/requests/{request_id}/variationGisFilesByRequest/{variationGisFileName}?compareToRequestId={compareToRequestID}", headers=headers)
                    # #print(type(change_gis_files)) # gives back a class 'requests.models.Response'. Check how to read this...
                    # print(response.status_code)
                    # #print(response.headers)
                    # value, params = cgi.parse_header(response.headers['content-disposition'])
                    # with open(f"{params['filename']}", "wb") as raster:
                    #     raster.write(response.content)
                    #     #raster2pgsql -s 4326 -I -C -M C:\temp\test_1.tif -t 100x100 myschema.mytable > out.sql
