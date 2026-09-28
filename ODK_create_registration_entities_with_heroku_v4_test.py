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

# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

batch_size = 100

while True:
    cur.execute('''WITH to_update AS (SELECT ecosia_site_id FROM getodk_entities_upload_table_registrations
    WHERE geometry LIKE 'POLYGON%'
    LIMIT %s)

    UPDATE getodk_entities_upload_table_registrations AS t
    SET geometry = REPLACE(RTRIM(LTRIM(geometry,'POLYGON (('),'))'),',',';')::varchar(50000)
    FROM to_update AS u
    WHERE t.ecosia_site_id = u.ecosia_site_id;''', (batch_size,))

    if cur.rowcount == 0:
        break

    conn.commit() # commit after each batch

cur.close()
