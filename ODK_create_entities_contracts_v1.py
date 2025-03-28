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
url_contracts = os.environ["URL_AIRTABLE_CONTRACTS"]
response = requests.get(url_contracts, headers=headers)
data_contracts = response.json()


list_offsets_subcontracts = []

# Harvest the SUB contract numbers from the FIRST page in airtable (100 rows per page), using no 'offset' string code (since it needs to be collected first)
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.
for ids in data_subcontracts['records']:
    #print(ids)
    maincontractnr = str(ids['fields']['Contractnr_odk'][0])
    organisation = str(ids['fields']['Subpartner_odk'][0])
    subcontractnr = str(ids['fields']['Subcontract'])
    subcontract_nr = str(maincontractnr + '.' + subcontractnr)
    print('A: ', subcontract_nr)
    country = str(ids['fields']['Country (from Project ID) (from Contractnr)'][0])
    #label = str(ids['fields']['Full Subcontract'])
    label = str(maincontractnr + '.' + subcontractnr)
    #print(label, maincontractnr, subcontractnr, organisation, maincontractnr)

    cur.execute('''INSERT INTO getodk_entities_contract_table (label, organisation, contract_number, country)
    VALUES (%s,%s,%s,%s)''', (label, organisation, subcontract_nr, country))
    conn.commit()

try:
    list_offsets_subcontracts.append(data_subcontracts['offset'])
    print(list_offsets_subcontracts)
except KeyError:
    print("No more offset values found")



# Collect the first 'offset' string code for the NEXT 100 rows page. Put that into a offset list for iteration
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.

# Loop through the first (again), second and subsequent 100 row pages and collect the offset string codes and put them into the offset list
for offset_loop in list_offsets_subcontracts:
    url_contracts = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Subcontracts" + "?offset=" + offset_loop
    response_subcontracts = requests.get(url_subcontracts, headers=headers)
    data_subcontracts = response.json()
    for ids in data_subcontracts['records']:

        maincontractnr = str(ids['fields']['Contractnr_odk'][0])
        subcontractnr = str(ids['fields']['Subcontract'])
        organisation = str(ids['fields']['Subpartner_odk'][0])
        subcontract_nr = str(maincontractnr + '.' + subcontractnr)
        print('B: ', subcontract_nr)
        country = str(ids_main['fields']['Country (from Project ID)'][0])
        #label = str(ids['fields']['Full Subcontract'])
        label = str(maincontractnr + '.' + subcontractnr)
        #print(label, maincontractnr, subcontractnr, organisation, contract_nr)

        cur.execute('''INSERT INTO getodk_entities_contract_table (label, organisation, contract_number, country)
        VALUES (%s,%s,%s,%s)''', (label, organisation, subcontract_nr, country))
        conn.commit()

    try:
        list_offsets_subcontracts.append(data_subcontracts['offset'])
    except KeyError:
        continue
    else:
        list_offsets_subcontracts.append(data_subcontracts['offset'])

################################
# Connect to Airtable and the specific table
#url_maincontracts = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts"
url_maincontracts = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/tbltBZqyiGSiPf0Q9"
response_maincontracts = requests.get(url_maincontracts, headers=headers)
data_maincontracts = response_maincontracts.json()
#print(data_maincontracts)

list_offsets_maincontracts = []

# Harvest the MAIN contract numbers from the FIRST page in airtable (100 rows per page), using no 'offset' string code (since it needs to be collected first)
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.
for ids_main in data_maincontracts['records']:
    #print(ids_main['offset'])
    try:
        maincontractnr = str(ids_main['fields']['ID'])
        maincontractnr = str(maincontractnr + '.00')
        print('D: ', maincontractnr)
    except KeyError:
        continue

    try:
        ids_main['fields']['AKVO ID (from Partner ID) (from Project)'][0]
    except KeyError:
        continue
    else:
        organisation = str(ids_main['fields']['AKVO ID (from Partner ID) (from Project)'][0])

    #label = str(maincontractnr + ' (' + organisation + ')')
    label = str(maincontractnr)
    #print(label, maincontractnr, organisation)
    country = str(ids_main['fields']['Country (from Project ID)'][0])

    cur.execute('''INSERT INTO getodk_entities_contract_table (label, organisation, contract_number, country)
    VALUES (%s,%s,%s,%s)''', (label, organisation, maincontractnr, country))
    conn.commit()


# Collect the first 'offset' string code for the NEXT 100 rows page. Put that into a offset list for iteration
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.

try:
    data_maincontracts['offset']
except KeyError:
    print('No more offset values')
else:
    list_offsets_maincontracts.append(data_maincontracts['offset'])

    for offset_loop in list_offsets_maincontracts:
        url_maincontracts = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts" + "?offset=" + offset_loop
        response_maincontracts = requests.get(url_maincontracts, headers=headers)
        data_maincontracts = response_maincontracts.json()

        for ids_main in data_maincontracts['records']:
            maincontractnr = str(ids_main['fields']['ID'])
            maincontractnr = str(maincontractnr + '.00')
            #print('E: ', maincontractnr)
            try:
                ids_main['fields']['AKVO ID (from Partner ID) (from Project)'][0]
            except KeyError:
                continue
            else:
                organisation = str(ids_main['fields']['AKVO ID (from Partner ID) (from Project)'][0])

            #label = str(maincontractnr + ' (' + organisation + ')')
            label = str(maincontractnr)
            #print(label, maincontractnr, organisation)
            country = str(ids_main['fields']['Country (from Project ID)'][0])

            cur.execute('''INSERT INTO getodk_entities_contract_table (label, organisation, contract_number, country)
            VALUES (%s,%s,%s,%s)''', (label, organisation, maincontractnr, country))
            conn.commit()

        try:
            data_maincontracts['offset']
        except KeyError:
            continue
        else:
            list_offsets_maincontracts.append(data_maincontracts['offset'])

print(list_offsets_maincontracts)

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

#Connect to ODK central server and use the merge command
client = Client(config_path="config.toml", cache_path="pyodk_cache.toml")

client.open()

client.entities.merge(entities_list, entity_list_name='contracts', project_id=1, match_keys=None, add_new_properties=True, update_matched=True, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()


# Drop the latests upload table
#cur.execute('''DROP TABLE IF EXISTS getodk_entities_contract_table;''')
conn.commit()
cur.close()
