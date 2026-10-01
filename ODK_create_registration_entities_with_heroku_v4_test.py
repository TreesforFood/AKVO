from shapely import wkt
from shapely import wkb
from shapely.ops import transform
from shapely.geometry import shape
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


# 3. Define the flip function
def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x


batch_size = 100
offset = 0

while True:
    # Fetch a batch of rows
    cur.execute('''
        SELECT polygon, ecosia_site_id
        FROM getodk_entities_upload_table_registrations
        WHERE polygon IS NOT NULL
          AND name_partner IS NOT NULL
          AND contract_number IS NOT NULL
          AND ecosia_site_id IS NOT NULL
        LIMIT %s OFFSET %s
    ''', (batch_size, offset))

    rows = cur.fetchall()

    # If no more rows, break the loop
    if not rows:
        break

    print(f"Processing batch starting at offset {offset} with {len(rows)} rows...")

    # Prepare data for transformation
    id_list = []
    lat_lon_coords = []

    for pol, id in rows:
        polygon_wkt = pol
        ecosia_id = id

    # Parse WKT to Shapely geometry
    if polygon_wkt is None:
        continue

    try:
        geom = shape(polygon_wkt)

        # Transform coordinates (swap lon/lat)
        transformed_geom = transform(flip, geom)

        # Convert to clean WKT string (remove extra parentheses and spaces)
        clean_wkt = transformed_geom.wkt.replace('POLYGON ((', 'POLYGON(').replace('))', ')')

        lat_lon_coords.append(clean_wkt)

        id_list.append(ecosia_id)

    except Exception as e:
        print(f"Error transforming polygon for ecosia_site_id={ecosia_id}: {e}")
        continue


    # Update the table with reverse coordinates
    for key, value in zip(id_list, lat_lon_coords):
        cur.execute('''
            UPDATE getodk_entities_upload_table_registrations
            SET geometry = %s
            WHERE ecosia_site_id = %s
        ''', (value, key))

    conn.commit()

    print(f"Batch at offset {offset} processed.")

    # Increment offset for the next batch
    offset += batch_size

cur.close()
conn.close()
