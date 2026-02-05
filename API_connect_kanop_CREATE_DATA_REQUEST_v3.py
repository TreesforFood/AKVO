import requests
import json
import psycopg2
from akvo_api_config import Config
from io import BytesIO
import pybase64
import datetime


# IMPORTANT NOTE: AFTER PROCESSING A POST AND CONFIRMING A PROJECT, YOU ALSO HAS TO RUN THIS SCRIPT FOR A DATA REQUEST.
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


# Projects meta: List all your projects
project_list_overview = projects_dict['projects'] # Generates a list with dictionaries of projects. Print(project_list_overview) # Output: {'projectId': 1880, 'customerId': 254, 'name': 'APAF', 'description': '','country': 'Ivory Coast', 'projectType': 'climate', 'startDate': '2018-01-01','status': 'CREATED', 'area': 51.89, 'duration': 20, 'polygonCount': 7, 'createdAt': '2023-10-02 18:31:34.556883','updatedAt': '2023-10-02 18:32:02.459363'}

for project_list in project_list_overview:
    project_ids = str(project_list['projectId'])
    contract_number = project_list['name']
    country = project_list['country']
    project_status = project_list['status']
    polygon_count = project_list['polygonCount']
    area = project_list['area']
    print('Project ID: ', project_ids, ' contract_number: ', contract_number, ' Country: ', country, ' Nr polygons: ', polygon_count)


project_id = input('Fill in the project ID to submit a data request: ')
list_areas = []
count_areas_total = 0
count_areas_success = 0

# ------- Create polygons for upload
config = Config()
conn = psycopg2.connect(host= config.CONF["HOST_PSTGRS"],database= config.CONF["DATABASE_PSTGRS"],user= config.CONF["USER_PSTGRS"],password= config.CONF["PASSWORD_PSTGRS"])
cur = conn.cursor()

cur.execute("SELECT identifier_akvo, planting_date, polygon FROM KANOP_latest_uploads")
row = cur.fetchall() # output is list of tuples named rows
#for input_data in row:
    #planting_date = input_data[1] # output is string
    #planting_year = planting_date[0:4]
    #earliest_planting_year = planting_year+'-07-01'
    #date_to_analise_1 = '2023-07-01'
    #count_areas_total += 1


# Create a data request. After confirming a project, a data request should be submitted to KANOP for the project
create_data_request_begin = requests.post(f"https://main.api.kanop.io/projects/{project_id}/requests?product=standard_10m",headers=headers)

if create_data_request_begin.status_code == 201:
    print('Data (on earliest planting date) is requested sucessfully. Message: ',create_data_request_begin.json())
else:
    print('Error in data request.', 'Problem was: ', create_data_request_begin.json())
