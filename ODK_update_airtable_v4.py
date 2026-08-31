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
import time


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


# Upload to Contracts tab in Airtable
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

        cur.execute('''SELECT
        contract,
        SUM("Total number of trees registered at t=0"),
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



# Pagination function to parse through all Airtable pages (Each Airtable page has 100 rows).
# Upload to Monitoring tab in Airtable
global offset_monitoring
offset_monitoring = '0'
result_monitoring = []
desired_users = {}

while True:
    url = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Monitoring"
    try:
        response = requests.get(url + '?offset=' + offset_monitoring, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_monitoring = response.json()

        records = response_monitoring.get('records', [])
        if not records:
            break  # No more records to fetch

        result_monitoring.append(records)

        # Update offset for next page
        offset_monitoring = response_monitoring.get('offset')
        if not offset_monitoring:
            break  # No more pages

        # Add a small delay to avoid rate-limiting
        time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching records: {e}")
        break
    except Exception as e:
        print(f"Unexpected error: {e}")
        break

count = 0
# Get the results from the pagination function
for x in result_monitoring:
    for y in x:
        monitoring_identifier_airtable = y['fields']['identifier']
        id_airtable = y['id']

        # Fetch data from your database
        cur.execute('''
            WITH check_monitoring_status AS (
                SELECT identifier_akvo, MAX(label_strata) AS monitoring_status
                FROM superset_ecosia_tree_monitoring
                GROUP BY identifier_akvo
            )
            SELECT
                b.identifier_akvo,
                MAX(a.organisation) AS organisation,
                MAX(a.contract) AS contract,
                CASE
                    WHEN b.monitoring_status = 0 THEN 'no monitoring carried out'
                    WHEN b.monitoring_status IN (180, 360, 540) THEN 't=1'
                    WHEN b.monitoring_status IN (720, 900) THEN 't=2'
                    ELSE 't=3'
                END AS monitoring_status
            FROM superset_ecosia_tree_monitoring a
            LEFT JOIN check_monitoring_status b ON a.identifier_akvo = b.identifier_akvo
            WHERE b.identifier_akvo = %s
            GROUP BY b.identifier_akvo, b.monitoring_status
        ''', (monitoring_identifier_airtable,))

        rows = cur.fetchall()

        for row in rows:
            try:
                organisation = row if row is not None else ''
            except (TypeError, IndexError):
                organisation = ''

            try:
                contract = float(row) if row is not None else 0
            except (TypeError, ValueError, IndexError):
                contract = 0

            try:
                monitoring_status = row if row is not None else 'no monitoring carried out'
            except (TypeError, IndexError):
                monitoring_status = 'no monitoring carried out'

            print(organisation, contract, monitoring_status)

            # Combine all updates into a single PATCH request
            row_airtable_to_update = f"https://api.airtable.com/v0/appkx2PPsqz3axWDy/Monitoring/{id_airtable}"
            update_data = {
                'fields': {
                    'Partner': organisation,
                    'Contract': contract,
                    't=? monitoring': monitoring_status
                }
            }

            try:
                response = requests.patch(
                    row_airtable_to_update,
                    headers=headers,
                    json=update_data
                )
                response.raise_for_status()
                print(f"Updated record {id_airtable}: Status Code {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Error updating record {id_airtable}: {e}")

        count += 1



# Pagination function to parse through all Airtable pages (Each Airtable page has 100 rows).
global offset_partnercode
offset_partnercode = '0'
result_partnercode = []
desired_users = {}


# Upload Partnercode to Partner tab in Airtable
while True :
    url = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/Partners"

    try :
        response= requests.get(url +'?offset=' + offset_partnercode, headers=headers)
        response_partnercode = response.json()
        records = list(response_partnercode['records'])
        result_partnercode.append(records)
        #print(records[0]['fields']['Username'] , len(records))

        try :
            offset_partnercode = response_partnercode['offset']

        except Exception as ex:
            break

    except error as e:
        print(e)

count = 0



# Get the results from the pagination function
for x in result_partnercode:
    for y in x:
        try:
            monitoring_partnername_airtable = y['fields']['System name'].lower()
        except KeyError:
            monitoring_partnername_airtable = 'no name...?'

        #print('monitoring_partnername_airtable:', monitoring_partnername_airtable)

        id_airtable = y['id']

        #print('monitoring_partnername_airtable = ', monitoring_partnername_airtable, 'id_airtable = ', id_airtable)

        cur.execute('''

        SELECT
        DISTINCT LOWER(organisation),
        partnercode_main

        FROM superset_ecosia_tree_registration
        WHERE organisation = %s''', (monitoring_partnername_airtable,))

        rows = cur.fetchall()

        for row in rows:
            #print(row)

            try:
                partnercode_main = str(row[1])
                #print('partnercode_main:', partnercode_main)
            except (TypeError, ValueError):
                partnercode_main = ''  # default or fallback value

            #print(partnercode_main, id_airtable)

            row_airtable_to_update = f"https://api.airtable.com/v0/appkx2PPsqz3axWDy/Partners/{id_airtable}"

            # Set the new field values for the record
            update_partnercode_airtable = {'fields':{'partnercode_main': partnercode_main}}


            # Send your request to update the record and parse the response
            response_airtable_partnercode = requests.patch(row_airtable_to_update, headers=headers, json=update_partnercode_airtable)
