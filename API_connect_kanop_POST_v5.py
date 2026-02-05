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

contract_number = input('select the contract number: ')


# Based on the contract number and planting year, the polygons will be uploaded to KANOP into a project folder
cur.execute('''
DROP TABLE IF EXISTS KANOP_latest_uploads;

CREATE TABLE KANOP_latest_uploads AS
WITH KANOP_new_uploads AS (SELECT
a.identifier_akvo,
b.identifier_akvo AS identifier_already_submitted_to_kanop,
a.contract_number,
a.planting_date,
ST_AsText(a.polygon::geometry) as polygon,
TO_CHAR(a.planting_date::date, 'yyyy') AS planting_year_uploaded
FROM akvo_tree_registration_areas_updated a

LEFT JOIN kanop_uploads b
ON a.identifier_akvo = b.identifier_akvo

WHERE a.polygon NOTNULL
AND a.total_nr_geometric_errors = 0
AND a.planting_date <> '')

SELECT * FROM KANOP_new_uploads
WHERE contract_number = %s
AND identifier_already_submitted_to_kanop ISNULL''', (contract_number,))

conn.commit()

cur.execute('''SELECT
COUNT(*) FROM KANOP_latest_uploads;''')
rowcount = cur.fetchone()[0]
rowcount = str(rowcount)

conn.commit()

kanop_upload_number = input('Enter the upload number (1,2 or 3) to integrate in the KANOP project name: ')

project_name = 'contract '+contract_number+' | Upload #'+kanop_upload_number

# Create project: Step 1
create_project = requests.post(f"https://main.api.kanop.io/projects/", headers = headers, json={"name": project_name})

projectId = create_project.json().get('projectId')
print("ProjectId = :",projectId)

list_areas = []
count_areas_total = 0
count_areas_success = 0


# ------- Create polygons for upload
cur.execute("SELECT identifier_akvo, planting_date, polygon FROM KANOP_latest_uploads ORDER BY planting_date DESC")
row = cur.fetchall() # output is list of tuples named rows

for input_data in row:
    polygons = input_data[2]
    polygon_reference = input_data[0]
    print(polygon_reference)
    count_areas_total += 1


    # Populate your project with one or more polygons. Done by sending raw data.
    upload_polygons = requests.post(f"https://main.api.kanop.io/projects/{projectId}/polygons",data={"crs": 4326,"geometry": polygons,"customerReferencePolygon": polygon_reference},headers=headers)
    if upload_polygons.status_code == 201:
        print('Sucessfull upload of polygon: ', polygon_reference)
        count_areas_success += 1
    else:
        print('Error with upload of polygon:: ', polygon_reference, '. Problem was: ', upload_polygons.json())

# Confirm a project. Validate your project before asking for analysis. Without this command a project is not listed at KANOP
confirm_project = requests.patch(f"https://main.api.kanop.io/projects/{projectId}?confirm=true", headers=headers)
if confirm_project.status_code == 200:
    print('Project confirmed sucessfully. From the total of ', count_areas_total, ' areas ', count_areas_success, ' were sucessfully uploaded')
else:
    print('Error in confirmation of project.', 'Problem was: ', confirm_project.json())


# IMPORTANT NOTE: AFTER PROCESSING A POST AND CONFIRMING A PROJECT, YOU ALSO HAS TO RUN THE SCRIPT FOR A DATA REQUEST.
