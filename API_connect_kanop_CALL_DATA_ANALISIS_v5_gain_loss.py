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


cur.execute('''DROP TABLE IF EXISTS superset_ecosia_KANOP_polygon_gain_loss_net;''')
conn.commit()


cur.execute('''CREATE TABLE superset_ecosia_KANOP_polygon_gain_loss_net (
kanop_project_id TEXT,
identifier_akvo TEXT,
name_project TEXT,
co2eq_gain NUMERIC(20,1),
co2eq_loss NUMERIC(20,1),
co2eq_net NUMERIC(20,1)
);''')

conn.commit()


# Projects meta: List all your projects
project_list_overview = projects_dict['projects'] # Generates a list with dictionaries of projects. Print(project_list_overview) # Output: {'projectId': 1880, 'customerId': 254, 'name': 'APAF', 'description': '','country': 'Ivory Coast', 'projectType': 'climate', 'startDate': '2018-01-01','status': 'CREATED', 'area': 51.89, 'duration': 20, 'polygonCount': 7, 'createdAt': '2023-10-02 18:31:34.556883','updatedAt': '2023-10-02 18:32:02.459363'}


for project_list in project_list_overview:
    # Get details for a project
    #print('Details of project: ', project_list)
    project_id = str(project_list['projectId'])
    contract_number = project_list['name']

    # Get details for a specific project (with project ID)
    project_details = requests.get(f"{root}/projects/{project_id}", headers = headers)
    project_details = project_details.json()
    metrics = project_details['metrics']
    name_project = project_details.get('name')
    indicators = ','.join(metrics)
    #print('Metrics of a project: ', metrics) # Output: Metrics of a project:  ['forestCover', 'canopyCover', 'canopyHeightMean', 'treeHeightMean', 'livingAbovegroundBiomass', 'livingAbovegroundBiomassPerHa', 'livingBelowgroundBiomass', 'livingBelowgroundBiomassPerHa', 'livingBiomass', 'livingBiomassPerHa', 'carbon', 'carbonPerHa', 'co2eq', 'co2eqPerHa']

    # # Get the aggregation levels for your project
    # get_aggregation_levels = requests.get(f"{root}/projects/{project_id}/aggregationLevels",headers=headers)
    # aggregation_level = get_aggregation_levels.json().get('aggregationLevels')
    # #print('Aggregation levels: ', aggregation_level)

    list_of_polygon_ids = requests.get(f"{root}/projects/{project_id}/aggregationLevels/polygons",headers=headers)
    polygon_id_list = list_of_polygon_ids.json()
    polygon_id_list = polygon_id_list['polygons']
    #print(polygon_id_list)
    #print('Project ID: ', project_id, contract_number, 'Polygon ID:', polygon_id_list)


    for polygon_id in polygon_id_list:
        #print(project_id, polygon_id, indicators)
        params = {'aggregationLevel': 'polygons'}
        params['aggregationLevelFilters'] = polygon_id
        #print(str(params))
        variation_metrics_over_time = requests.get(f"{root}/projects/{project_id}/change/{indicators}",headers=headers, params=params)
        variation_metrics_over_time = variation_metrics_over_time.json()
        #print(project_id, polygon_id, variation_metrics_over_time)

        for k,v in variation_metrics_over_time.items():
            #print(project_id, polygon_id, k,v)
            identifier_akvo = polygon_id
            kanop_project_id = project_id

            if k == 'results':
                y = v['co2eq']['averageDistribution']
                co2eq_gain = (y[1]['value'].get('gain'))
                co2eq_loss = (y[1]['value'].get('loss'))
                co2eq_net = (y[1]['value'].get('net'))

                #for x in y:
                    #print('year: ',x['aggregate'])
                    #print('results: ',x['value'])
#                     if bool(x['value']) == True:
#                         co2eq_gain = x['value']['gain']
#                         co2eq_loss = x['value']['loss']
#                         co2eq_net = x['value']['net']
#                     else:
#                         print(kanop_project_id, identifier_akvo)
#                         print('no gain, loss or net value calculated for this identifier')
#                         continue
#
#
                print(kanop_project_id, identifier_akvo, co2eq_gain, co2eq_loss, co2eq_net)
                # Populate the KANOP table
                cur.execute('''INSERT INTO superset_ecosia_KANOP_polygon_gain_loss_net (
                kanop_project_id, identifier_akvo, name_project, co2eq_gain, co2eq_loss, co2eq_net)
                VALUES (%s,%s,%s,%s,%s,%s)''',
                (kanop_project_id, identifier_akvo, name_project, co2eq_gain, co2eq_loss, co2eq_net))

                conn.commit()


cur.execute('''

GRANT SELECT ON TABLE superset_ecosia_KANOP_polygon_gain_loss_net TO ecosia_superset;

DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_KANOP_polygon_gain_loss_net;

ALTER TABLE superset_ecosia_KANOP_polygon_gain_loss_net enable ROW LEVEL SECURITY;

CREATE POLICY ecosia_superset_policy ON superset_ecosia_KANOP_polygon_gain_loss_net TO ecosia_superset USING (true);''')

conn.commit()

cur.close()
