"""
App User Provisioner

Put a series of user names (one on each line) in a file named `users.csv` in the same
directory as this script. The script will create App Users for each user, using the
project, forms, and other configurations set below. The outputs are one PNG for each
provisioned App User, and a `users.pdf` file with all the App User PNGs in the folder.

Install requirements for this script in `requirements.txt`. The specified versions are
those that were current when the script was last updated, though it should work with
more recent versions. Install these with `pip install -r requirements.txt`.

To run the script, use `python app_user_provisioner.py`.
"""

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
from dotenv import load_dotenv, find_dotenv
import os
from akvo_api_config import Config
import sys
import boto3
from io import BytesIO
import io

print(boto3.__version__)
print(dir(boto3))

config = Config()

# Connect to Postgresql database
conn = psycopg2.connect(host= config.CONF["HOST_PSTGRS"],database= config.CONF["DATABASE_PSTGRS"],user= config.CONF["USER_PSTGRS"],password= config.CONF["PASSWORD_PSTGRS"])
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS GetODK_QR_codes;''')

cur.execute('''CREATE TABLE GetODK_QR_codes (organisation_name TEXT, QR_code BYTEA);''')
conn.commit()


# Connect to Airtable
auth_token = "patSIiImh5J6aEMSX.5ab8f3d1764644d2e7ec6fb37ef5903840d70b9e1a79730668a51b1c0ab94cf0"
headers = {"Authorization": f"Bearer {auth_token}"}


# define the needed AWS credentials and bucket name
AWS_ACCESS_KEY_ID = "AKIAY6QVY5ISOXJBAKNW"
AWS_SECRET_ACCESS_KEY = "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"
S3_BUCKET = 'getodk-qr-codes'

# establish the AWS client connection to the the S3 service of AWS
s3_client = boto3.client(service_name="s3", region_name="eu-north-1", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
print(s3_client)


# Settings for QR code
PROJECT_ID = 1
FORMS_TO_ACCESS = ['planting_site_reporting', 'nursery_reporting']
PROJECT_NAME = 'ecosia'
ADMIN_PASSWORD = "ecosia_change_settings"


# Pagination function to parse through all Airtable pages (Each Airtable page has 100 rows).
global offset
offset = '0'
result = []
desired_users = {}

while True :
    url = "https://api.airtable.com/v0/appkx2PPsqz3axWDy/ODK users"

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
        users = y['fields']['Username']
        count += 1
        print(count, users)
        users = users.lower()
        id_airtable = y['id']
        desired_users[id_airtable] = users


def get_settings(server_url: str, project_name: str, username: str) -> dict[str, Any]:
    """Template for the settings to encode in the QR image. Customise as needed."""
    return {
        "general": {
            "form_update_mode": "match_exactly",
            "autosend": "wifi_and_cellular",
            "delete_send": True,
            "server_url": server_url,
            "username": username,
            "reference_layer": "https://drive.google.com/drive/folders/1XoDDGYV5wZ_aEOX6RmoCAB5c5avVFwe7"
        },
        "admin": {
            "admin_pw": ADMIN_PASSWORD,
            "change_server": False,
            "automatic_update": False,
            "change_autosend": False,
        },

        "project": {"name": project_name, "color": "#ffffff", "icon": "✅"},} # icon is emoji symbol } #ffeb3b

# Check that the Roboto font used for the QR images is available (e.g. on Linux / Win).
try:
    ImageFont.truetype("Roboto-Regular.ttf", 24)
except OSError:
    print(
        "Font file 'Roboto-Regular.ttf' not found. This can be downloaded "
        "from Google, or copied from the Examples directory. "
        "Source: https://fonts.google.com/specimen/Roboto/about"
    )


#Connect to ODK central server and use the merge command
client = Client(config_path="config.toml", cache_path="pyodk_cache.toml")
client.open()

def create_url_friendly_filename(filename):
    # Replace spaces with hyphens (or underscores)
    filename = filename.replace(' ', '+')
    return filename


for key, value in desired_users.items():
    # Create an Airtable username list here so that through very loop iteration,
    # the list is empty again. So there will be 1 username in each iteration,
    # that is connected with the specific Airtable ID
    list_user_name_airtable = []
    id_airtable = key
    list_user_name_airtable.append(value)
    provisioned_users = client.projects.create_app_users(display_names=list_user_name_airtable, forms=FORMS_TO_ACCESS, project_id=PROJECT_ID)

    ## Generate the QR codes.
    for user in provisioned_users:
        collect_settings = get_settings(
            server_url=f"{client.session.base_url}key/{user.token}/projects/{PROJECT_ID}",
            project_name=f"{PROJECT_NAME}: {user.displayName}",
            username=user.displayName,
        )
        qr_data = base64.b64encode(
            zlib.compress(json.dumps(collect_settings).encode("utf-8"))
        )

        code = segno.make(qr_data, micro=False) # from SEGNO library
        code.save("settings.png", scale=4) # from SEGNO library. Saves QR code as png image

        png = Image.open("settings.png") # from PIL library. Opens the QR.png image
        png = png.convert("RGB")
        text_anchor = png.height
        png = ImageOps.expand(png, border=(10, 10, 10, 60), fill=(255, 255, 255))
        draw = ImageDraw.Draw(png)
        font = ImageFont.truetype("Roboto-Regular.ttf", 24)
        draw.text((20, text_anchor - 10), "GetODK QR code for:\n" + user.displayName, font=font, fill=(0, 0, 0))
        in_mem_file = io.BytesIO()
        png.save(in_mem_file, "PNG")

        in_mem_file.seek(0)

        # THIS ONE WORKS! response = s3_client.upload_fileobj(in_mem_file, 'getodk-qr-codes','test.png')
        response = s3_client.upload_fileobj(in_mem_file, 'getodk-qr-codes',f"settings-{user.displayName}.png")

        get_bucket_location = s3_client.get_bucket_location(Bucket=S3_BUCKET)['LocationConstraint']

        name = f"settings-{user.displayName}.png"

        url_s3 = "https://s3-%s.amazonaws.com/%s/%s" % (get_bucket_location, 'getodk-qr-codes', name)

        url_s3 = create_url_friendly_filename(url_s3)
        #https://getodk-qr-codes.s3.eu-north-1.amazonaws.com/settings-fundacion+dia_user_12.png
        #print(url_s3)

        # Upload url to Airtable
        row_airtable_to_update = f"https://api.airtable.com/v0/appkx2PPsqz3axWDy/ODK users/{id_airtable}"

        # Set the new field values for the record
        update_fields_airtable = {'fields':{'QR code': url_s3}}

        # Send your request to update the record and parse the response
        response_airtable = requests.patch(row_airtable_to_update, headers=headers, json=update_fields_airtable)
        data = json.loads(response_airtable.text)
        #print(data)
