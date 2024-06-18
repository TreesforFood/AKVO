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


# THIS TABLE TS TO DOWNLOAD AND STORE ALL KANOP RESULTS PERMANENTLY
cur.execute('''CREATE TABLE IF NOT EXISTS superset_ecosia_KANOP_polygon_gain_loss_net (
kanop_project_id TEXT,
identifier_akvo TEXT,
name_project TEXT,
co2eq_gain NUMERIC(20,1),
co2eq_loss NUMERIC(20,1),
co2eq_net NUMERIC(20,1)
);''')

conn.commit()


# DROP AND CREATE AN EMPTY TABLE TO POPULATE/DOWNLOAD THE PROJECT IDS FROM KANOP
cur.execute('''DROP TABLE IF EXISTS kanop_temp_table_project_ids;''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS kanop_temp_table_project_ids (
kanop_project_id TEXT
);''')


# # Projects meta: List all your projects
project_list_overview = projects_dict['projects'] # Generates a list with dictionaries of projects. Print(project_list_overview) # Output: {'projectId': 1880, 'customerId': 254, 'name': 'APAF', 'description': '','country': 'Ivory Coast', 'projectType': 'climate', 'startDate': '2018-01-01','status': 'CREATED', 'area': 51.89, 'duration': 20, 'polygonCount': 7, 'createdAt': '2023-10-02 18:31:34.556883','updatedAt': '2023-10-02 18:32:02.459363'}


for project_list in project_list_overview: # harvest all project_ids on the KANOP server

    project_id = str(project_list['projectId'])
    contract_number = project_list['name']

    # Save the project ids in a table
    cur.execute('''INSERT INTO kanop_temp_table_project_ids (
    kanop_project_id)
    VALUES (%s)''',
    (project_id,))

    conn.commit()

    # Check which project_id is not yet in the database
    cur.execute('''SELECT a.kanop_project_id, b.kanop_project_id FROM kanop_temp_table_project_ids a
    LEFT JOIN (SELECT
    DISTINCT(kanop_project_id) AS kanop_project_id
    FROM superset_ecosia_KANOP_polygon_gain_loss_net) b
    ON a.kanop_project_id = b.kanop_project_id
    WHERE b.kanop_project_id ISNULL;''')
    #AND b.co2eq_gain ISNULL;''')

    conn.commit()

    # Fetch all project_ids that are not yet in the database
    project_ids_not_in_db = cur.fetchall()
    print(project_ids_not_in_db)

# Loop thourhg the project_ids that are not yet in the database
for row in project_ids_not_in_db:
    project_id = row[0]
    print('kanop_project_id being processed: ', project_id)

    # Get details for a specific project (with project ID)
    project_details = requests.get(f"{root}/projects/{project_id}", headers = headers)
    project_details = project_details.json()
    metrics = project_details['metrics']
    name_project = project_details.get('name')
    indicators = ','.join(metrics)

    list_of_polygon_ids = requests.get(f"{root}/projects/{project_id}/aggregationLevels/polygons",headers=headers)
    polygon_id_list = list_of_polygon_ids.json()
    polygon_id_list = polygon_id_list['polygons']

    counter = 0

    for polygon_id in polygon_id_list:
        #print(project_id, polygon_id, indicators)
        params = {'aggregationLevel': 'polygons'}
        params['aggregationLevelFilters'] = polygon_id
        #print(str(params))
        variation_metrics_over_time = requests.get(f"{root}/projects/{project_id}/change/{indicators}",headers=headers, params=params)
        variation_metrics_over_time = variation_metrics_over_time.json()
        #print(variation_metrics_over_time)
        #print(project_id, polygon_id, variation_metrics_over_time)


        for k,v in variation_metrics_over_time.items():
            #print(project_id, polygon_id, k,v)
            identifier_akvo = polygon_id
            kanop_project_id = project_id
            counter = counter + 1
            print('number of polygons being processed for', kanop_project_id,': ', counter)


            if k == 'results':
                print(v['co2eq']['averageDistribution'][1]['value'].get('gain'))
                if v['co2eq']['averageDistribution'][1]['value'].get('gain') is None or v['co2eq']['averageDistribution'][1]['value'].get('gain') is None:
                    # kanop_project_id = None
                    # co2eq_gain = None
                    # co2eq_loss = None
                    # co2eq_net = None
                    continue

                else:
                    y = v['co2eq']['averageDistribution']
                    co2eq_gain = (y[1]['value'].get('gain'))
                    co2eq_loss = (y[1]['value'].get('loss'))
                    co2eq_net = (y[1]['value'].get('net'))


                #print(kanop_project_id, identifier_akvo, co2eq_gain, co2eq_loss, co2eq_net)
                # Populate the KANOP table
                cur.execute('''INSERT INTO superset_ecosia_KANOP_polygon_gain_loss_net (
                kanop_project_id, identifier_akvo, name_project, co2eq_gain, co2eq_loss, co2eq_net)
                VALUES (%s,%s,%s,%s,%s,%s)''',
                (kanop_project_id, identifier_akvo, name_project, co2eq_gain, co2eq_loss, co2eq_net))

                conn.commit()


cur.close()
