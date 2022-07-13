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

cur.execute('''
DROP TABLE IF EXISTS url_latest_sync;
CREATE TABLE url_latest_sync (id SMALLSERIAL, sync_url TEXT);
INSERT INTO url_latest_sync(id) VALUES(DEFAULT);''')

conn.commit()

with open('sync_urls_PG.pkl', 'rb') as f:
    open_sync_url = cloudpickle.load(f)
    print("url that goes into database: ", open_sync_url)


cur.execute('''UPDATE url_latest_sync SET sync_url = %s WHERE id = %s;''', (open_sync_url, 1))

conn.commit()
