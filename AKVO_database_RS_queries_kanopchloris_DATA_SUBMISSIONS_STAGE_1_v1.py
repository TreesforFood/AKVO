import requests
import json
import psycopg2
import re
import geojson
import os
from chloris_app_sdk import ChlorisAppClient


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



#LOGIN CHLORIS
refresh_token = os.environ["refresh_token_chloris"]
organisation_id = os.environ["organisation_id_chloris"]

# create the client
client = ChlorisAppClient(
    organization_id=organisation_id,
    refresh_token=refresh_token, # env: CHLORIS_REFRESH_TOKEN
)

cur.execute('''
DROP TABLE IF EXISTS chloris_uploads;
DROP TABLE IF EXISTS kanop_uploads;''')
conn.commit()

cur.execute('''
CREATE TABLE chloris_uploads (identifier_akvo TEXT);
CREATE TABLE kanop_uploads (identifier_akvo TEXT);''')

# CHLORIS UPLOADS
reporting_units = client.list_active_sites()

for unit_id in reporting_units:
    #print('ReportingUnitS level: ', unit_id)
    label_identifier_chloris = unit_id['label']
    cur.execute('''INSERT INTO chloris_uploads (identifier_akvo)
    VALUES (%s)''',
    (label_identifier_chloris,))

conn.commit()



# KANOP UPLOADS
project_list_overview = projects_dict['projects'] # Generates a list with dictionaries of projects. Print(project_list_overview) # Output: {'projectId': 1880, 'customerId': 254, 'name': 'APAF', 'description': '','country': 'Ivory Coast', 'projectType': 'climate', 'startDate': '2018-01-01','status': 'CREATED', 'area': 51.89, 'duration': 20, 'polygonCount': 7, 'createdAt': '2023-10-02 18:31:34.556883','updatedAt': '2023-10-02 18:32:02.459363'}

for project_list in project_list_overview:
    project_id = str(project_list['projectId'])
    submitted_polygons_kanop = requests.get(f"{root}/projects/{project_id}/aggregationLevels/polygons",headers=headers)
    submitted_polygons = submitted_polygons_kanop.json()
    for label_identifier_kanop in submitted_polygons['polygons']:
        cur.execute('''INSERT INTO kanop_uploads (identifier_akvo)
        VALUES (%s)''',
        (label_identifier_kanop,))

conn.commit()

cur.close()
