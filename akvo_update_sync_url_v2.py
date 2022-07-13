# This script needs to be executed after a full download.
# It is used to populate the PG database with them
# first NextSyncUrl that is read from the pickle file (sync_urls_PG.pkl)

import requests
import json
import sqlite3
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
import os
import cloudpickle


#connect to Postgresql databases
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')

cur = conn.cursor()

# Frist create the PG table to store the NextSyncUrl
cur.execute('''
DROP TABLE IF EXISTS url_latest_sync;
CREATE TABLE url_latest_sync (id SMALLSERIAL, sync_url TEXT);
INSERT INTO url_latest_sync(id) VALUES(DEFAULT);''')

conn.commit()

# read the NextSyncUrl from the pickle file and store it into the PG table.
with open('sync_urls_PG.pkl', 'rb') as f:
    open_sync_url = cloudpickle.load(f)
    print("url that goes into database: ", open_sync_url)


cur.execute('''UPDATE url_latest_sync SET sync_url = %s WHERE id = %s;''', (open_sync_url, 1))

conn.commit()
