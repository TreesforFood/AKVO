import base64
import glob
import json
import zlib
from typing import Any
import segno
from PIL import Image, ImageDraw, ImageFont, ImageOps
from pyodk.client import Client
import pandas as pd
import requests
import re
import json
import psycopg2
import os
import sys
import boto3
from io import BytesIO
import io


# Retrieve environment variables from Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_odk_users_table = os.environ["URL_AIRTABLE_USERNAMES"]
response = requests.get(url_odk_users_table, headers=headers)
data_contracts = response.json()

# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()


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
        contract_airtable = y['fields']['ID']
        id_airtable = y['id']

        cur.execute('''SELECT contract, SUM("Total number of trees registered at t=0"),
        SUM("total tree number in t=1"),
        SUM("total tree number in t=2"),
        SUM("total tree number in t=>3") FROM superset_ecosia_contract_overview
        WHERE contract = %s
        group by contract''', (contract_airtable,))

        rows = cur.fetchall()

        for row in rows:
            print(row)

            try:
                ss_t0 = int(row[1])
            except (TypeError, ValueError):
                ss_t0 = 0  # default or fallback value

            try:
                ss_t1 = int(row[2])
            except (TypeError, ValueError):
                ss_t1 = 0  # default or fallback value

            try:
                ss_t2 = int(row[3])
            except (TypeError, ValueError):
                ss_t2 = 0  # default or fallback value

            try:
                ss_t3 = int(row[4])
            except (TypeError, ValueError):
                ss_t3 = 0  # default or fallback value

            row_airtable_to_update = f"https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts/{id_airtable}"

            # Set the new field values for the record
            update_t0_airtable = {'fields':{'ss_t0': ss_t0}}
            update_t1_airtable = {'fields':{'ss_t1': ss_t1}}
            update_t2_airtable = {'fields':{'ss_t2': ss_t2}}
            update_t3_airtable = {'fields':{'ss_t3': ss_t3}}

            # Send your request to update the record and parse the response
            response_airtable_t0 = requests.patch(row_airtable_to_update, headers=headers, json=update_t0_airtable)
            response_airtable_t1 = requests.patch(row_airtable_to_update, headers=headers, json=update_t1_airtable)
            response_airtable_t2 = requests.patch(row_airtable_to_update, headers=headers, json=update_t2_airtable)
            response_airtable_t3 = requests.patch(row_airtable_to_update, headers=headers, json=update_t3_airtable)
            #data = json.loads(response_airtable.text)
