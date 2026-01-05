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
cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table;''')
cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table_unregistered_farmers;''')
conn.commit()

#Create empty contract list to collect all activated contracts for monitoring
list_contracts = []
list_identifiers = []

# Connect to Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_contracts = os.environ["URL_AIRTABLE_CONTRACTS"]
response = requests.get(url_contracts, headers=headers)
data_contracts = response.json()

###### SCRIPT TO CREATE ENTITIES FOR TREE MONITORING (NORMAL TREE REGISTRATION)

# Pagination function to parse through all Airtable pages (Each Airtable page has 100 rows).
global offset
offset = '0'
result = []
desired_users = {}

while True :
    url = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts"

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
        print(e)

count = 0

# Get the results from the pagination function
for x in result:
    for y in x:
        #print(y)
        contracts = y['fields']['ID']
        contract_id = str(contracts)+'.00'
        #print(contracts)
        try:
            confirmation_monitoring = y['fields']['monitor?']
            #print(confirmation_monitoring)
            if confirmation_monitoring is True: # Returns true if activated. If not activated it loops into the python Except
                try:
                    check_availability_identifiers_akvo = y['fields']['sites for monitor']
                except KeyError:
                # Only store contract numbers where no identifier is given. If an identifier is given, no contract number must be added to the list/tuple
                    list_contracts.append(contract_id)
                    #tuple_contracts_to_monitor = tuple(list_contracts)
                    #print("TEST 3 contracts: ", list_contracts)
                else:
                    list_identifiers_specific_to_monitor = y['fields']['sites for monitor'].split(",")
                    for x in list_identifiers_specific_to_monitor:
                        list_identifiers.append(x)
                        # if contract_id == '179.00':
                        #     print(tuple(list_identifiers))

        except KeyError:
             continue


tuple_contracts = tuple(list_contracts)
list_identifiers_clean = []
for i in list_identifiers:
    j = i.replace(' ','')
    list_identifiers_clean.append(j)

tuple_identifiers = tuple(list_identifiers_clean)


# Select entities to upload to GetODK. Note that the label column of the ODK entities table does not accept strange characters. So these are removed in this sql
# Also duplicate labels should be avoided!
cur.execute(
'''CREATE TABLE getodk_entities_upload_table AS

WITH temp_contract_overview AS (

SELECT DISTINCT(CONCAT('Organisation: ', LOWER(organisation), ' | Contract number: ', contract_number, ' | Site ID: ', REGEXP_REPLACE(id_planting_site, '[^a-zA-Z0-9 ]', '', 'g'), ' | Name owner: ', name_owner , ' | Ecosia site id: ', identifier_akvo)) AS label,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN country NOTNULL
THEN country
ELSE 'Country unknown'
END AS country,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN organisation NOTNULL
THEN organisation
ELSE 'organisation_unknown'
END AS organisation,

id_planting_site,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN name_owner NOTNULL
THEN CONCAT(id_planting_site, ' | ', name_owner)
WHEN name_owner = ''
THEN CONCAT(id_planting_site, ' | owner unknown')
WHEN name_owner ISNULL
THEN CONCAT(id_planting_site, ' | owner unknown')
END AS name_id_planting_site,

'' AS location_area,
'' AS geometry,
'' AS polygon,
'' AS geometry_point,
'' AS odk_entity_geometry,
--CONCAT(contract_number::INTEGER::varchar(10),'.00') AS contract_number_match_airtable,
CONCAT(SUBSTRING(contract_number::varchar(10) FROM 1 FOR POSITION('.' IN contract_number::varchar(10)) - 1),'.00') AS contract_number_match_airtable,
contract_number::varchar(10),
'' AS identifier_akvo,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN submission NOTNULL
THEN TO_CHAR(submission, 'YYYY-MM-DD')
ELSE 'Submission date unknown'
END AS submission,

ST_AsText(polygon) AS new_polygon,
identifier_akvo AS ecosia_site_id,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN calc_area > 0
THEN calc_area
ELSE '0'
END AS area_ha,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN planting_date NOTNULL
THEN planting_date
ELSE 'planting date unknown'
END AS planting_date,

'planting_site' AS landscape_element,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN tree_number NOTNULL
THEN CAST(tree_number AS text)
WHEN tree_number ISNULL
THEN CAST(0 AS text)
END AS tree_number,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN submitter NOTNULL
THEN submitter
ELSE 'submitter unknown'
END AS user_name_enumerator

FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL
AND form_source = 'normal tree registration'
AND (test = 'This is real, valid data'
OR test = '')


UNION -- Union sites with Polygons and sites with Points into 1 table


SELECT
DISTINCT(CONCAT('Organisation: ', LOWER(organisation), ' | Contract number: ', contract_number, ' | Site ID: ', REGEXP_REPLACE(id_planting_site, '[^a-zA-Z0-9 ]', '', 'g'), ' | Name owner: ', name_owner , ' | Ecosia site id: ', identifier_akvo)) AS label,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN country NOTNULL
THEN country
ELSE 'Country unknown'
END AS country,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN organisation NOTNULL
THEN organisation
ELSE 'organisation_unknown'
END AS organisation,

id_planting_site,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN name_owner NOTNULL
THEN CONCAT(id_planting_site, ' | ', name_owner)
WHEN name_owner = ''
THEN CONCAT(id_planting_site, ' | owner unknown')
WHEN name_owner ISNULL
THEN CONCAT(id_planting_site, ' | owner unknown')
END AS name_id_planting_site,

'' AS location_area,
'' AS geometry,
'' AS polygon,
'' AS geometry_point,
'' AS odk_entity_geometry,
--CONCAT(contract_number::INTEGER::varchar(10),'.00') AS contract_number_match_airtable,
CONCAT(SUBSTRING(contract_number::varchar(10) FROM 1 FOR POSITION('.' IN contract_number::varchar(10)) - 1),'.00') AS contract_number_match_airtable,
contract_number::varchar(10),
'' AS identifier_akvo,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN submission NOTNULL
THEN TO_CHAR(submission, 'YYYY-MM-DD')
ELSE 'Submission date unknown'
END AS submission,

ST_AsText(centroid_coord) AS new_polygon,
identifier_akvo AS ecosia_site_id,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN calc_area > 0
THEN calc_area
ELSE '0'
END AS area_ha,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN planting_date NOTNULL
THEN planting_date
ELSE 'planting date unknown'
END AS planting_date,

'planting_site' AS landscape_element,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN tree_number NOTNULL
THEN CAST(tree_number AS text)
WHEN tree_number ISNULL
THEN CAST(0 AS text)
END AS tree_number,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN submitter NOTNULL
THEN submitter
ELSE 'submitter unknown'
END AS user_name_enumerator

FROM akvo_tree_registration_areas_updated
WHERE polygon ISNULL AND centroid_coord NOTNULL
AND form_source = 'normal tree registration'
AND (test = 'This is real, valid data'
OR test = ''))

SELECT
label,
LOWER(country) AS country,
LOWER(organisation) AS organisation,
id_planting_site,
name_id_planting_site,
location_area,
geometry,
polygon,
geometry_point,
odk_entity_geometry,
contract_number,
identifier_akvo,
new_polygon,
ecosia_site_id,
CAST(area_ha AS TEXT) AS area_ha,
tree_number,
user_name_enumerator,
submission AS site_registration_date,
planting_date,
landscape_element

FROM temp_contract_overview
where contract_number_match_airtable IN %s OR ecosia_site_id IN %s;''', (tuple_contracts, tuple_identifiers))

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

# Update the table with reverse coordinates
for key,value in dict.items():
    cur.execute('''UPDATE getodk_entities_upload_table
    SET geometry = %s
    WHERE ecosia_site_id = %s''', (value,key))
    conn.commit()

# Remove the WKT format ('POLYGON(( etc))')
cur.execute('''UPDATE getodk_entities_upload_table
SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POLYGON (('),'))'),',',';')::varchar(50000)
WHERE geometry LIKE 'POLYGON%';''')
conn.commit()

# Remove the WKT format ('POINT(( etc))')
cur.execute('''UPDATE getodk_entities_upload_table
SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POINT (('),'))'),',',';')::varchar(50000)
WHERE geometry LIKE 'POINT%';''')
conn.commit()

# Select all rows and fetch them all
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
    print('FREEK:', entities)
    entities_list.append(entities.copy())

# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")
client.open()

client.entities.merge(entities_list, entity_list_name='monitoring_trees', project_id=1, match_keys=None, add_new_properties=True, update_matched=True, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()

conn.commit()


###### SCRIPT TO CREATE ENTITIES FOR TREE MONITORING OF UNREGISTERED FARMERS

## IMPORTANT NOTE: The script below is creating a list of unregistered farmers (AKVO) of whom their site has been mapped (registered) with AKVO.
## The script below DOES NOT integrate the list uf unregistered farmers of which their sites have NOT BEEN mapped yet (As these farmers are not integrated in the table 'akvo_tree_registration_areas_updated'). Hence, this script probably needs to be adapted by adding also the AKVO list of farmers that are only listed and of whom their sites have not been registered yet!
## The script below DOES NOT list unregistered farmers (listed with ODK), as their instances are uploaded to the entity list automatically with the ODK (tree distribution) form. MAybe at a later stage we can still integrate this so that the list is entirely refreshed with this scipt.

# Pagination function to parse through all Airtable pages (Each Airtable page has 100 rows).
#global offset
offset = '0'
result = []
desired_users = {}

while True :
    url = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts"

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
        print(e)

count = 0

# Get the results from the pagination function
for x in result:
    for y in x:
        #print(y)
        contracts = y['fields']['ID']
        contract_id = str(contracts)+'.00'
        #print(contracts)
        try:
            confirmation_monitoring = y['fields']['monitor?']
            #print(confirmation_monitoring)
            if confirmation_monitoring is True: # Returns true if activated. If not activated it loops into the python Except
                try:
                    check_availability_identifiers_akvo = y['fields']['sites for monitor']
                except KeyError:
                # Only store contract numbers where no identifier is given. If an identifier is given, no contract number must be added to the list/tuple
                    list_contracts.append(contract_id)
                    #tuple_contracts_to_monitor = tuple(list_contracts)
                    #print("TEST 3 contracts: ", list_contracts)
                else:
                    list_identifiers_specific_to_monitor = y['fields']['sites for monitor'].split(",")
                    for x in list_identifiers_specific_to_monitor:
                        list_identifiers.append(x)
                        # if contract_id == '179.00':
                        #     print(tuple(list_identifiers))

        except KeyError:
             continue


tuple_contracts = tuple(list_contracts)
list_identifiers_clean = []
for i in list_identifiers:
    j = i.replace(' ','')
    list_identifiers_clean.append(j)

tuple_identifiers = tuple(list_identifiers_clean)

# Select entities to upload to GetODK. Note that the label column of the ODK entities table does not accept strange characters. So these are removed in this sql
# Also duplicate labels should be avoided!
cur.execute(
'''CREATE TABLE getodk_entities_upload_table_unregistered_farmers AS
WITH temp_contract_overview_unreg AS (SELECT
DISTINCT
(CONCAT('name farmer: ', a.name_owner, 'name id planting site: ', a.id_planting_site, 'instance:', a.instance)) AS label,
(CONCAT(a.name_owner, ' (', a.gender_owner, ') site ID:', a.id_planting_site)) AS search_label,
a.name_owner AS full_name_farmer,
a.id_planting_site AS name_id_planting_site,
a.nr_trees_received::TEXT AS total_nr_trees_received,
ST_AsText(a.centroid_coord),

a.identifier_akvo AS ecosia_site_id_dist,
a.contract_number::TEXT AS contract_number,
CONCAT(a.contract_number::varchar(10)) AS contract_number_match_airtable,
LOWER(a.organisation) AS organisation,
a.submitter AS user_name_enumerator,
a.gender_owner,
'' AS landscape_element,
'' AS other_recipients

FROM akvo_tree_registration_areas_updated a
WHERE centroid_coord NOTNULL
AND form_source = 'unregistered_farmers'
AND test = 'This is real, valid data')

SELECT
b.label,
b.full_name_farmer,
b.name_id_planting_site,
b.total_nr_trees_received,
b.st_astext AS geometry,
b.ecosia_site_id_dist,
b.contract_number,
b.organisation,
b.user_name_enumerator,
b.gender_owner AS gender,
b.search_label,
'' AS landscape_element,
'' AS other_recipients

FROM temp_contract_overview_unreg b
where contract_number_match_airtable IN %s OR ecosia_site_id_dist IN %s;''', (tuple_contracts, tuple_identifiers))
conn.commit()

cur.execute('''SELECT geometry, ecosia_site_id_dist FROM getodk_entities_upload_table_unregistered_farmers;''')

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

# Update the table with reverse coordinates
for key,value in dict.items():
    cur.execute('''UPDATE getodk_entities_upload_table_unregistered_farmers
    SET geometry = %s
    WHERE ecosia_site_id_dist = %s''', (value,key))
    conn.commit()

# Remove the WKT format ('POINT(( etc))')
cur.execute('''UPDATE getodk_entities_upload_table_unregistered_farmers
SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POINT (('),'))'),',',';')::varchar(50000)
WHERE geometry LIKE 'POINT%';''')
conn.commit()

# Select all rows and fetch them all
cur.execute('''SELECT * FROM getodk_entities_upload_table_unregistered_farmers''')
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
    print(entities_list)

# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")
client.open()

client.entities.merge(entities_list, entity_list_name='farmers_list', project_id=1, match_keys=None, add_new_properties=True, update_matched=True, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()
conn.commit()
cur.close()
