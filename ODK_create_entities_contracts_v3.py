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

# first drop the latests upload table
cur.execute('''DROP TABLE IF EXISTS getodk_entities_contract_table;''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS getodk_entities_contract_table (label TEXT, organisation TEXT, contract_number TEXT, country TEXT);''')
conn.commit()


# Connect to Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_subcontracts = os.environ["URL_AIRTABLE_SUBCONTRACTS"]
response_subcontracts = requests.get(url_subcontracts, headers=headers)
data_subcontracts = response_subcontracts.json()
url_contracts = os.environ["URL_AIRTABLE_CONTRACTS"]
response_maincontracts = requests.get(url_contracts, headers=headers)
data_contracts = response_maincontracts.json()


#### GET MAIN CONTRACTS
# Pagination function to parse through all CONTRACTS on the MAIN CONTRACT page
global offset
offset = '0'
result = []
desired_users = {}

while True :
    url = url_contracts

    try :
        response= requests.get(url +'?offset=' + offset, headers=headers)
        response_Table = response.json()
        records = list(response_Table['records'])
        result.append(records)

        try :
            offset = response_Table['offset']

        except Exception as ex:
            break

    except error as e:
        print(e)

count = 0

# Get the results from the pagination function
for x in result:
    for y in x:
        count = count + 1
        open_for_reporting = y['fields']['Open for reporting']
        print('all contracts', maincontractnr,':', open_for_reporting)
        maincontractnr = str(y['fields']['ID'])
        maincontractnr = str(maincontractnr + '.00')

        if open_for_reporting is True:

            try:
                y['fields']['AKVO ID (from Partner ID) (from Project)'][0]
            except KeyError:
                continue
            else:
                organisation = str(y['fields']['AKVO ID (from Partner ID) (from Project)'][0])

            label = str(maincontractnr)
            country = str(y['fields']['Country (from Project ID)'][0])

            print('Only open contracts', country,':', maincontractnr, ':', open_for_reporting)

            cur.execute('''INSERT INTO getodk_entities_contract_table (label, organisation, contract_number, country)
            VALUES (%s,%s,%s,%s)''', (label, organisation, maincontractnr, country))
            conn.commit()


#### GET SUB CONTRACTS
# Pagination function to parse through all SUBCONTRACTS on the SUBCONTRACT page
offset = '0'
result = []
desired_users = {}

while True :
    url = url_subcontracts

    try :
        response= requests.get(url +'?offset=' + offset, headers=headers)
        response_Table = response.json()
        records = list(response_Table['records'])
        result.append(records)

        try :
            offset = response_Table['offset']

        except Exception as ex:
            break

    except error as e:
        print(e)

count = 0

# Get the results from the pagination function
for x in result:
    for y in x:
        count = count + 1
        open_for_reporting = y['fields']['Open for reporting']
        if open_for_reporting is True:
            maincontract = str(y['fields']['Contractnr_odk'][0])
            organisation = str(y['fields']['Subpartner_odk'][0])
            subcontractnr = str(y['fields']['Subcontract'])
            subcontract_nr = str(maincontract + '.' + subcontractnr)
            country = str(y['fields']['Country (from Project ID) (from Contractnr)'][0])
            label = str(maincontract + '.' + subcontractnr)

            cur.execute('''INSERT INTO getodk_entities_contract_table (label, organisation, contract_number, country)
            VALUES (%s,%s,%s,%s)''', (label, organisation, subcontract_nr, country))
            conn.commit()

# Select all rows and fetch them all
cur.execute('''SELECT
label,
contract_number,
LOWER(organisation) AS organisation,
LOWER(country) as country

FROM getodk_entities_contract_table''')
conn.commit()
rows_dict = cur.fetchall()

# Convert the postgres data into a dictionary and place these dictionaries into a list
columns = []
entities_list = []
entities = {}
for column in cur.description:
    columns.append(column[0].lower())
for row in rows_dict:
    for i in range(len(row)):
        entities[columns[i]] = row[i]
        if isinstance(row[i], str):
            entities[columns[i]] = row[i].strip()
    print('FREEK:', entities)
    entities_list.append(entities.copy())

# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")

client.open()

client.entities.merge(entities_list, entity_list_name='contracts', project_id=1, match_keys=None, add_new_properties=True, update_matched=True, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()


# Drop the latests upload table
#cur.execute('''DROP TABLE IF EXISTS getodk_entities_contract_table;''')
conn.commit()
cur.close()
