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

# # Define a writable path (/app/tmp is a writable directory on Heroku)
# file_path = "/app/tmp/pyodk_config.ini"
#
# # Create the directory if it doesn't exist
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
#
# # Write the configuration to the file
# with open(file_path, "w") as file:
#     file.write(file_content)


# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()



# 5. Process in batches of 1000 rows
batch_size = 1000

while True:
    cur.execute('''
        WITH to_update AS (SELECT ctid
        FROM getodk_entities_upload_table_registrations
        WHERE geometry LIKE 'POLYGON%'
        LIMIT %s)

        UPDATE getodk_entities_upload_table_registrations AS t
        SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POLYGON (('),'))'),',',';')::varchar(50000)
        WHERE geometry LIKE 'POLYGON%'
        FROM to_update AS u
        WHERE t.ctid = u.ctid''', (batch_size,))

    if cur.rowcount == 0:
        break

    conn.commit() # commit after each batch

cur.close()
