from shapely import wkt
from shapely import wkb
from shapely.ops import transform
from pyodk.client import Client
import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine
import requests
import os

# Retrieve environment variables
base_url = "https://ecosia.getodk.cloud"
username = os.environ["ODK_CENTRAL_USERNAME"]
password = os.environ["ODK_CENTRAL_PASSWORD"]
default_project_id = 1

# Define the file content
file_content = f"""[central]
base_url = "{base_url}"
username = "{username}"
password = "{password}"
default_project_id = {default_project_id}
"""

# Define a writable path (/app/tmp is a writable directory on Heroku)
file_path = "/app/tmp/pyodk_config.ini"

# Create the directory if it doesn't exist
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Write the configuration to the file
with open(file_path, "w") as file:
    file.write(file_content)


# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

# Drop the latests upload table
cur.execute('''DROP TABLE IF EXISTS airtable_download_monitoring_results;''')
conn.commit()

cur.execute('''
CREATE TABLE airtable_download_monitoring_results (identifier TEXT, notes TEXT, planting_date DATE, nr_trees_survived INTEGER));
''')
conn.commit()


# Connect to Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_contracts = os.environ["URL_AIRTABLE_CONTRACTS"]
response = requests.get(url_contracts, headers=headers)
data_contracts = response.json()


# Pagination function to parse through all Airtable pages (Each Airtable page has 100 rows).
global offset
offset = '0'
result = []
desired_users = {}

while True :
    url = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Monitoring"

    try :
        response= requests.get(url +'?offset=' + offset, headers=headers)
        response_Table = response.json()
        records = list(response_Table['records'])
        result.append(records)
        #print(records[0]['fields']['Username'] , len(records))

        try :
            offset = response_Table['offset']

        except Exception as ex:
            break

    except error as e:
        print('Error: ', e)

count = 0

# Get the results from the pagination function
for x in result:
    for y in x:
        identifier = y['fields']['identifier']
        notes = y['fields']['identifier']
        planting_date = y['fields']['identifier']
        nr_trees_survived = y['fields']['identifier']

        print(identifier, notes, planting_date, nr_trees_survived)

        cur.execute('''INSERT INTO airtable_download_monitoring_results (identifier, notes, planting_date, nr_trees_survived)
        VALUES (%s,%s,%s,%s)''', (identifier, notes, planting_date, nr_trees_survived))

        conn.commit()






conn.commit()
cur.close()
