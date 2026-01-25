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
        print('Error: ', e)

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
                    count_identifiers = 0
                    for x in list_identifiers_specific_to_monitor:
                        list_identifiers.append(x)
                        # check number of identifiers for a certain contract
                        if contract_id == '190.00':
                            count_identifiers += 1
                            print(count_identifiers, x)



        except KeyError:
            continue


tuple_contracts = tuple(list_contracts)
list_identifiers_clean = []
for i in list_identifiers:
    j = i.replace(' ','')
    list_identifiers_clean.append(j)

tuple_identifiers = tuple(list_identifiers_clean)

#print(tuple_identifiers)
count_contracts = 0
for contracts in tuple_contracts:
    count_contracts += 1
    print(count_contracts, contracts)


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
--WHERE
--polygon NOTNULL
--AND form_source = 'normal tree registration'
--AND
WHERE test = 'This is real, valid data'
OR test = ''


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
--WHERE
--polygon ISNULL AND centroid_coord NOTNULL
--AND form_source = 'normal tree registration'
--AND
WHERE test = 'This is real, valid data'
OR test = '')

SELECT
ROW_NUMBER()OVER(PARTITION BY label ORDER BY label) AS row_number, --Give duplicates a number higher than 1
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

# Remove the duplicate labels
cur.execute('''DELETE FROM getodk_entities_upload_table WHERE row_number > 1;''')
conn.commit()

cur.execute('''SELECT new_polygon, ecosia_site_id FROM getodk_entities_upload_table
WHERE new_polygon NOTNULL;''')
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
    print(key,value)
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

# We need to set the column RN to text ttype because this is the only data type alowed by ODK entities. However, before we can change to text we first need to define is=t as bigint
cur.execute('''UPDATE getodk_entities_upload_table
SET row_number = row_number::bigint;''')
conn.commit()

cur.execute('''ALTER TABLE getodk_entities_upload_table ALTER COLUMN row_number TYPE text USING row_number::text;''')

# Set the new_polygon column to string (text) where there is no polygon (NULL values)
cur.execute('''UPDATE getodk_entities_upload_table
SET geometry = ''
WHERE geometry ISNULL;''')
conn.commit()

# Set the RN column to string because that is the only type allowed by ODK entitities
cur.execute('''UPDATE getodk_entities_upload_table
SET row_number = row_number::text;''')
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
    entities_list.append(entities.copy())
    print('FREEK:', entities_list)


#Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")

client.open()

client.entities.merge(entities_list, entity_list_name='monitoring_trees', project_id=1, match_keys=None, add_new_properties=True, update_matched=True, delete_not_matched=False, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()

conn.commit()
cur.close()
