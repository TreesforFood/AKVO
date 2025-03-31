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
cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table_nurseries;''')


# Connection to airtable
# Connection to Airtable is not needed in this script since all nurseries in our database will be uploaded to the ODK entity list


# Select entities to upload to GetODK. Note that the label column of the ODK entities table does not accept strange characters. So these are removed in this sql
# Also duplicate labels should be avoided!
cur.execute(
'''CREATE TABLE getodk_entities_upload_table_nurseries AS
(SELECT DISTINCT(CONCAT('Organisation: ', LOWER(organisation), ' | Nursery name: ', nursery_name, ' | Nursery id:', identifier_akvo)) AS label,

identifier_akvo AS ecosia_nursery_id,
'' AS contract_number,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN nursery_name NOTNULL
THEN nursery_name
ELSE 'nursery_name_unknown'
END AS name_nursery,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN organisation NOTNULL
THEN LOWER(organisation)
ELSE 'organisation_unknown'
END AS organisation,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN submission NOTNULL
THEN submission::TEXT
ELSE 'registration_date_unknown'
END AS nursery_registration_date,

CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
WHEN submitter NOTNULL
THEN submitter
ELSE 'name_submitter_unknown'
END AS user_name_enumerator,

ST_AsText(centroid_coord) AS geometry

FROM akvo_nursery_registration
WHERE centroid_coord NOTNULL)''')

conn.commit()

cur.execute('''SELECT geometry, ecosia_nursery_id FROM getodk_entities_upload_table_nurseries;''')

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
        dict[key] = value
        lat_lon_coords.remove(value)
        break


# Update the table with reverse coordinates
for key,value in dict.items():
    cur.execute('''UPDATE getodk_entities_upload_table_nurseries
    SET geometry = %s
    WHERE ecosia_nursery_id = %s''', (value,key))
    conn.commit()


# Remove the WKT format ('POINT(( etc))')
cur.execute('''UPDATE getodk_entities_upload_table_nurseries
SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POINT (('),'))'),',',';')::varchar(100)
WHERE geometry LIKE 'POINT%';''')
conn.commit()

# Select all rows and fetch them all
cur.execute('''SELECT * FROM getodk_entities_upload_table_nurseries''')
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


# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")

client.open()

client.entities.merge(entities_list, entity_list_name='monitoring_nurseries', project_id=1, match_keys=None, add_new_properties=True, update_matched=False, delete_not_matched=True, source_label_key='label', source_keys=None,create_source=None, source_size=None)

client.close()


#first drop the latests upload table
#cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table;''')
conn.commit()
cur.close()
