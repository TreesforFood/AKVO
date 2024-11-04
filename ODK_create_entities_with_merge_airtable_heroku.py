from shapely import wkt
from shapely import wkb
from shapely.ops import transform
from pyodk.client import Client
import pandas as pd
import requests
import psycopg2
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
import requests
import os


#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

# first drop the latests upload table
cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table;''')

#Create empty contract list to collect all activated contracts for monitoring
list_contracts = []

# Connect to Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_contracts = os.environ["URL_AIRTABLE_CONTRACTS"]
response = requests.get(url_contracts, headers=headers)
data_contracts = response.json()

# get the token from AKVO
data = {"client_id": os.environ["CLIENT_ID"], "username" : os.environ["USERNAME"], "password": os.environ["PASSWORD"], "grant_type": os.environ["GRANT_TYPE"], "scope": os.environ["SCOPE"]}
response = requests.post("https://akvofoundation.eu.auth0.com/oauth/token", data=data)

# Harvest the contract numbers from the FIRST page in airtable (100 rows per page), using no 'offset' string code (since it needs to be collected first)
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.
for ids in data_contracts['records']:
    try:
        contract_id = ids['fields']['ID']
        confirmation_monitoring = ids['fields']['monitor?'] # Returns true if activated. If not activated it loops into the python Except
        if confirmation_monitoring is True:
            list_contracts.append(contract_id)
            tuple_contracts_to_monitor = tuple(list_contracts)
    except:
        continue

# Collect the first 'offset' string code for the NEXT 100 rows page. Put that into a offset list for iteration
# Not the most clean solution. Ideally, we create a function for this in order not to use two of the same loops.
offset = data_contracts['offset']
list_offsets = []
list_offsets.append(offset)

# Loop through the first (again), second and subsequent 100 row pages and collect the offset string codes and put them into the offset list
for offset_loop in list_offsets:
    url_contracts = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts" + "?offset=" + offset_loop
    response = requests.get(url_contracts, headers=headers)
    data_contracts = response.json()
    try:
        data_contracts['offset']
    except KeyError:
        break
    else:
        offset = data_contracts['offset']
        list_offsets.append(offset)
        # While collecting the offset string codes in a list, at the same time the contract data from that page is collected and also put in a list.
        for ids in data_contracts['records']:
            try:
                contract_id = ids['fields']['ID']
                confirmation_monitoring = ids['fields']['monitor?'] # Returns true if activated. If not activated it loops into the python Except
                if confirmation_monitoring is True:
                    list_contracts.append(contract_id)
                    tuple_contracts_to_monitor = tuple(list_contracts,)
            except:
                continue

for contracts_uploaded in list_contracts:
    print("Uploaded contracts: ", contracts_uploaded)

print(tuple_contracts_to_monitor)

# Select entities to upload to GetODK. Note that the label column of the ODK entities table does not accept strange characters. So these are removed in this sql
# Also duplicate labels should be avoided!
cur.execute(
'''CREATE TABLE getodk_entities_upload_table AS
SELECT
DISTINCT(CONCAT('Country: ', country, ' | Organisation: ', LOWER(organisation), ' | Contract number: ', contract_number, ' | Site ID: ', REGEXP_REPLACE(id_planting_site, '[^a-zA-Z0-9 ]', '', 'g'), ' | Ecosia site id:', identifier_akvo)) AS label,
country,
LOWER(organisation) as organisation,
id_planting_site, '' AS location_area,
'' AS geometry,
polygon,
'' AS geometry_point,
'' AS odk_entity_geometry,
contract_number::varchar(255),
'' AS identifier_akvo,
ST_AsText(polygon) AS new_polygon,
identifier_akvo AS ecosia_site_id,
calc_area::varchar(255) AS area_ha,
tree_number::varchar(255) AS tree_number,
submitter AS user_name_enumerator

FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL AND contract_number IN %s''', (tuple_contracts_to_monitor,))

conn.commit()

cur.execute('''SELECT new_polygon, ecosia_site_id FROM getodk_entities_upload_table;''')
conn.commit()

rows = cur.fetchall()

# Reverse the x and y coordinates
def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x

dict = {}
lat_lon_coords = []

# Create a dictionary and appending the polygons to this dictionary
id = [row[1]for row in rows]
geometries = [wkt.loads(row[0]) for row in rows]
for lon_lat_coords in geometries:
    lat_lon_coords.append(transform(flip, lon_lat_coords).wkt)

# Linking the polygons to their identifier
for key in id:
    for value in lat_lon_coords:
        #print('v:', value)
        dict[key] = value
        lat_lon_coords.remove(value)
        break


for key,value in dict.items():
    cur.execute('''UPDATE getodk_entities_upload_table
    SET geometry = %s
    WHERE ecosia_site_id = %s''', (value,key))
    conn.commit()

# Remove the WKT format ('Polygon(( etc))')
cur.execute('''UPDATE getodk_entities_upload_table
SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POLYGON (('),'))'),',',';');''')
conn.commit()

cur.execute('''SELECT * FROM getodk_entities_upload_table''')
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
    #print('FREEK:', entities)
    entities_list.append(entities.copy())


#Connect to ODK central server and use the merge command
client = Client(config_path="config.toml", cache_path="pyodk_cache.toml")
client.open()

client.entities.merge(entities_list, entity_list_name='monitoring_trees', project_id=1, match_keys=None, add_new_properties=True, update_matched=False, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()


# first drop the latests upload table
cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table;''')
conn.commit()
cur.close()
