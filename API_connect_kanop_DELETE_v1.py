import requests
import json
import psycopg2
import re
import geojson
import os
from datetime import datetime
import geopandas as gpd
import pandas as pd
from shapely import wkt

#connect to Heroku Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

CLIENT_ID = os.environ["CLIENT_ID_KANOP"]
CLIENT_SECRET = os.environ["PASSWORD_KANOP"]

# ------ Login to KANOP
token = CLIENT_SECRET
root = 'https://main.api.kanop.io'
headers = {"Authorization": f"Bearer {token}", "Accept-version": "v1"}
references = requests.get(f"{root}/projects", headers = headers)
projects_dict = references.json()


delete_project_id = input('what project_id must be deleted?: ')

# Populate your project with one or more polygons. Done by sending raw data.
delete_polygons = requests.delete(f"https://api.kanop.io/projects/{delete_project_id}")
