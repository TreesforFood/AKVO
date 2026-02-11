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
        contract_id_airtable = y['fields']['ID']

        cur.execute('''SELECT contract, SUM("Total number of trees registered at t=0") FROM superset_ecosia_contract_overview
        WHERE contract = %s
        group by contract''', (contract_id_airtable,))

        rows = cur.fetchall()

        for row in rows:
            print(row)
            ss_t0 = str(row[1])

            row_airtable_to_update = f"https://api.airtable.com/v0/appkx2PPsqz3axWDy/Contracts/{contract_id_airtable}"

            # Set the new field values for the record
            update_t0_airtable = {'fields':{'ss_t0': ss_t0}}

            # Send your request to update the record and parse the response
            response_airtable = requests.patch(row_airtable_to_update, headers=headers, json=update_t0_airtable)
            data = json.loads(response_airtable.text)
