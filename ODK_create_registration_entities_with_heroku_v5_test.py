from shapely import wkt
from shapely import wkb
from shapely.ops import transform
from shapely.geometry import shape
from shapely.geometry import Point
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
cur.execute('''DROP TABLE IF EXISTS getodk_entities_upload_table_registrations;''')
conn.commit()

# 1. Create the table with the full dataset (no LIMIT)
cur.execute(
    '''CREATE TABLE getodk_entities_upload_table_registrations AS

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

    '' AS geometry,

    CONCAT(
    CASE
        WHEN POSITION('.' IN contract_number::varchar(10)) > 0 THEN
        SUBSTRING(contract_number::varchar(10) FROM 1 FOR POSITION('.' IN contract_number::varchar(10)) - 1)
        ELSE
        contract_number::varchar(10)
    END,
    '.00'
    ) AS contract_number_match_airtable,

    contract_number::varchar(10),

    CASE -- Fields can not be empty when uploaded to the entity list of ODK. If so, ODK gives a 'no string' error
    WHEN submission NOTNULL
    THEN TO_CHAR(submission, 'YYYY-MM-DD')
    ELSE 'Submission date unknown'
    END AS submission,


    CASE
    WHEN polygon IS NOT NULL AND NOT ST_IsEmpty(polygon::geometry)
    THEN ST_AsText(polygon)
    WHEN (polygon IS NULL OR ST_IsEmpty(polygon::geometry))
    AND centroid_coord IS NOT NULL AND ST_IsValid(centroid_coord::geometry) AND NOT ST_IsEmpty(centroid_coord::geometry)
    THEN ST_AsText(centroid_coord)
    ELSE NULL
    END AS polygon,

    identifier_akvo AS ecosia_site_id,

    '' AS monitor_check,

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
    WHERE test = 'This is real, valid data'
    OR test = '')

    SELECT
    ROW_NUMBER()OVER(PARTITION BY label ORDER BY label) AS row_number, --Give duplicates a number higher than 1
    label,
    LOWER(country) AS country,
    LOWER(organisation) AS name_partner,
    id_planting_site,
    name_id_planting_site,
    geometry,
    contract_number,
    polygon,
    ecosia_site_id,
    monitor_check,
    CAST(area_ha AS TEXT) AS area_ha,
    tree_number,
    user_name_enumerator,
    submission AS site_registration_date,
    planting_date,
    landscape_element

    FROM temp_contract_overview
    LIMIT 2000;''') # Later remove this LIMIT 2000. This is for testing!!!

conn.commit()


# 2. Remove the duplicate labels
cur.execute('''DELETE FROM getodk_entities_upload_table_registrations WHERE row_number > 1;''')
conn.commit()

# 3. Define the flip function
def flip(x, y):
    """Flips the x and y coordinate values"""
    return y, x


batch_size = 100
offset = 0

# --- One-time DDL operations (run once, not in loop) ---
cur.execute('''
    ALTER TABLE getodk_entities_upload_table_registrations
    ALTER COLUMN row_number TYPE text USING row_number::text;
''')
conn.commit()

cur.execute('''
    UPDATE getodk_entities_upload_table_registrations
    SET geometry = ''
    WHERE geometry IS NULL OR geometry = '';
''')
conn.commit()

while True:
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

    if not rows:
        break

    print(f"Processing batch starting at offset {offset} with {len(rows)} rows...")

    id_list = []
    lat_lon_coords = []

    for pol, ecosia_id in rows:
        #print(pol) # POLYGON((3 5, 5 6, etc))
        #print(type(pol)) # str type
        try:

            if pol is None:
                raise ValueError(f"Polygon is None for ecosia_id={ecosia_id}")

            geometries = wkt.loads(pol)
            print(geometries)
            print(type(geometries))

            # Creates error: Error transforming polygon for ecosia_id=scdt-jedt-8kag: 'Polygon' object is not iterable
            # for lon_lat_coords in geometries:
            #     lat_lon_coords.append(transform(flip, lon_lat_coords).wkt)
            #     print('lat_lon_coords: ', lat_lon_coords)


            for lon, lat in geometries.exterior.coords:
                transformed = transform(flip, Point(lon, lat))
                lat_lon_coords.append(transformed.wkt)

            id_list.append(ecosia_id)

        except ValueError as ve:
            print(f"Null polygon skipped for ecosia_id={ecosia_id}: {ve}")
            continue
        except Exception as e:
            print(f"Error transforming polygon for ecosia_id={ecosia_id}: {e}")
            continue

    # --- Batch UPDATE using executemany (much faster) ---
    if id_list:
        cur.executemany('''
            UPDATE getodk_entities_upload_table_registrations
            SET geometry = %s
            WHERE ecosia_site_id = %s
        ''', list(zip(lat_lon_coords, id_list)))

        # Clean geometry strings in one shot
        cur.execute('''
            UPDATE getodk_entities_upload_table_registrations
            SET geometry = REPLACE(
                REPLACE(
                    REPLACE(geometry, 'POLYGON ((', 'POLYGON('),
                    '))', ')'),
                ',', ';')
            WHERE geometry LIKE 'POLYGON%'
              AND ecosia_site_id = %s''', (id_list,))

        conn.commit()

    # # --- Set monitor_check for matching IDs ---
    # if 'tuple_contracts' in locals() and 'tuple_identifiers' in locals():
    #     cur.execute('''
    #         UPDATE getodk_entities_upload_table_registrations
    #         SET monitor_check = '1'
    #         WHERE ecosia_site_id = ANY(%s::text[])
    #     ''', (tuple_contracts + tuple_identifiers,))
    #     conn.commit()

    # # --- Fetch only the current batch for ODK merge ---
    # placeholders = ','.join(['%s'] * len(id_list)) if id_list else "'NULL'"
    # cur.execute(f'''
    #     SELECT * FROM getodk_entities_upload_table_registrations
    #     WHERE ecosia_site_id IN ({placeholders})
    # ''', id_list)
    #
    rows_dict = cur.fetchall()

    # Build entities list
    columns = [col[0].lower() for col in cur.description]
    entities_list = []
    for row in rows_dict:
        entity = {}
        for i, val in enumerate(row):
            key = columns[i]
            entity[key] = val.strip() if isinstance(val, str) else val
        entities_list.append(entity)

    if not entities_list:
        print(f"No entities to merge for batch at offset {offset}.")
        offset += batch_size
        continue

    # --- Merge into ODK Central ---
    try:
        client = Client(
            config_path="/app/tmp/pyodk_config.ini",
            cache_path="/app/tmp/pyodk_cache.ini"
        )

        client.open()

        #Connect to ODK central server and use the merge command
        client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")

        client.open()

        client.entities.merge(entities_list, entity_list_name='registration_trees', project_id=1, match_keys=['ecosia_site_id'], add_new_properties=True, update_matched=True, delete_not_matched=False, source_label_key='label', source_keys=None,create_source=None, source_size=None)


        print(f"Batch at offset {offset} processed.")

    except Exception as e:
        print(f"ODK merge failed for batch at offset {offset}: {e}")
        # Optionally: implement retry logic here


    finally:
        if client is not None:
            client.close()  # <-- Always close if it was created

    offset += batch_size


conn.commit()
cur.close()
conn.close()
