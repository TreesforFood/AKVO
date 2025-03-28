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
cur.execute('''DROP TABLE IF EXISTS getodk_entities_species_table;''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS getodk_entities_species_table (label TEXT, organisation TEXT, latin_name TEXT, local_name TEXT);''')
conn.commit()


# Connect to Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_species = os.environ["URL_AIRTABLE_SPECIES"]
response = requests.get(url_species, headers=headers)
data_url_species = response.json()


#Create empty contract list to collect all activated contracts for monitoring
list_species = []

# Function to check if the organisation column contains multpile organisations in a comma-seperated string.
def check_comma_seperate(input_string):
    if input_string is not None:
        list_organisations = input_string.split(",")
        if len(list_organisations) > 2:
            for x in list_organisations:
                return x
        else:
            return input_string
    else:
        return 'No organisation name'


# Harvest the species from the FIRST page in airtable (100 rows per page), using no 'offset' string code (since it needs to be collected first)
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.
for ids in data_url_species['records']:
    try:
        #organisation = check_comma_seperate(ids['fields']['Species partner']
        organisation_x = ids['fields']['Species partner']
        if organisation_x is not None:
        #print('2E:', organisation_x)
            organisation_list = organisation_x.split(",")
            #print('2F:', len(organisation_list))
            if len(organisation_list) > 2:
                for x in organisation_list:
                    print('2G:', x)
            else:
                #print('2H:', organisation_x)
                organisation = organisation_x

                local_name = ids['fields']['Local name w. method']
                latin_name = ids['fields']['code species + method']
                #label = organisation + local_name + latin_name
                # Populate the species table
                cur.execute('''INSERT INTO getodk_entities_species_table (organisation, latin_name, local_name)
                VALUES (%s,%s,%s)''', (organisation, latin_name, local_name))
                conn.commit()

        if organisation_x is None:
            organisation = 'No organisation name given for this species'
    except:
        continue


# Collect the first 'offset' string code for the NEXT 100 rows page. Put that into a offset list for iteration
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.
offset = data_url_species['offset']
list_offsets = []
list_offsets.append(offset)


# Loop through the first (again), second and subsequent 100 row pages and collect the offset string codes and put them into the offset list
for offset_loop in list_offsets:
    url_species = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/TS casc." + "?offset=" + offset_loop
    response = requests.get(url_species, headers=headers)
    data_species = response.json()
    try:
        data_species['offset']
    except KeyError: # with Keyerror there is NO more subsequent page. However, the data from this last page still needs to be processed
        for ids in data_species['records']:
            try:

                #organisation = check_comma_seperate(ids['fields']['Species partner']
                organisation_x = ids['fields']['Species partner']
                if organisation_x is not None:
                #print('2E:', organisation_x)
                    organisation_list = organisation_x.split(",")
                    #print('2F:', len(organisation_list))
                    if len(organisation_list) > 2:
                        for x in organisation_list:
                            print('2G:', x)
                    else:
                        #print('2H:', organisation_x)
                        organisation = organisation_x

                        local_name = ids['fields']['Local name w. method']
                        latin_name = ids['fields']['code species + method']
                        #label = organisation + local_name + latin_name
                        # Populate the species table
                        cur.execute('''INSERT INTO getodk_entities_species_table (organisation, latin_name, local_name)
                        VALUES (%s,%s,%s)''', (organisation, latin_name, local_name))
                        conn.commit()

                if organisation_x is None:
                    organisation = 'No organisation name given for this species'
            except:
                break
    else:
        offset = data_species['offset']
        list_offsets.append(offset)
        # While collecting the offset string codes in a list, at the same time the contract data from that page is collected and also put in a list.
        for ids in data_species['records']:
            try:
                #organisation = check_comma_seperate(ids['fields']['Species partner'])
                organisation = ids['fields']['Species partner']
                if organisation is not None:
                    #print('3I:', organisation)
                    if type(organisation) is list:
                        organisation = organisation[0]
                        #print('3J:', organisation)
                        local_name = ids['fields']['Local name w. method']
                        latin_name = ids['fields']['code species + method']
                        # Populate the species table
                        cur.execute('''INSERT INTO getodk_entities_species_table (organisation, latin_name, local_name)
                        VALUES (%s,%s,%s)''', (organisation, latin_name, local_name))
                        conn.commit()

                    else:
                        organisation_list = organisation.split(",")
                        for x in organisation_list:
                            organisation = x.strip()
                            #print('3K:', organisation)

                            local_name = ids['fields']['Local name w. method']
                            latin_name = ids['fields']['code species + method']
                            # Populate the species table
                            cur.execute('''INSERT INTO getodk_entities_species_table (organisation, latin_name, local_name)
                            VALUES (%s,%s,%s)''', (organisation, latin_name, local_name))
                            conn.commit()
                if organisation is None:
                    organisation = 'No organisation name given for this species'
                    #print('TEST: ', organisation)
            except:
                continue

# Select all rows and fetch them all
cur.execute('''WITH temp_getodk_entities_species_table AS (
SELECT
CONCAT(organisation,'|', latin_name, '|', local_name, '|', CAST(label AS varchar(10))) as label,
latin_name,
local_name,
LOWER(CAST(organisation AS varchar(80))) as organisation,
'' as lower
FROM getodk_entities_species_table)

SELECT
DISTINCT(label) as label,
latin_name,
local_name,
LOWER(CAST(organisation AS varchar(80))) as organisation,
'' as lowerlatin_name,
local_name,
LOWER(CAST(organisation AS varchar(80))) as organisation,
'' as lower
FROM temp_getodk_entities_species_table
WHERE organisation NOTNULL''')
conn.commit()
rows_dict = cur.fetchall()
#print(rows_dict)
#test = cur.description
#print(test)


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
    entities_list.append(entities.copy())


# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")

client.open()

client.entities.merge(entities_list, entity_list_name='species_list', project_id=1, match_keys=None, add_new_properties=True, update_matched=False, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()


# first drop the latests upload table
#cur.execute('''DROP TABLE IF EXISTS getodk_entities_species_table;''')
conn.commit()
cur.close()
