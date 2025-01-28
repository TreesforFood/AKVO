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
import os
import sys
import boto3
from io import BytesIO
import io

# Retrieve environment variables from GetODK
base_url = "https://ecosia.getodk.cloud"
username = os.environ["ODK_CENTRAL_USERNAME"]
password = os.environ["ODK_CENTRAL_PASSWORD"]
default_project_id = 1

# Define the GetODK file content
file_content = f"""[central]
base_url = "{base_url}"
username = "{username}"
password = "{password}"
default_project_id = {default_project_id}
"""

# Define a writable path for GetODK (/app/tmp is a writable directory on Heroku)
file_path = "/app/tmp/pyodk_config.ini"

# Create the GetODK directory if it doesn't exist
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Write the GetODK configuration to the file
with open(file_path, "w") as file:
    file.write(file_content)

# Retrieve environment variables from Airtable
auth_token = os.environ["TOKEN_AIRTABLE"]
headers = {"Authorization": f"Bearer {auth_token}"}
url_odk_users_table = os.environ["URL_AIRTABLE_USERNAMES"]
response = requests.get(url_odk_users_table, headers=headers)
data_contracts = response.json()

# Retrieve environment variables from Amazone S3
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]

# Settings for QR code
PROJECT_ID = 1
FORMS_TO_ACCESS = ['planting_site_reporting']
PROJECT_NAME = 'ecosia'
#ADMIN_PASSWORD = "D*#maZ7*QW8Nfj6%"
ADMIN_PASSWORD = "ecosia_change_settings"


# establish the AWS client connection to the the S3 service of AWS
s3_client = boto3.client(service_name="s3", region_name="eu-north-1", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
print(s3_client)

#desired_users = []
desired_users = {}

# Get the usernames from Airtable.
for username in data_contracts['records']:
    #print(username)
    try:
        if username['fields']['Username'] is not None:
            users = username['fields']['Username']
            id_airtable = username['id']
            #desired_users.append(users)
            desired_users[id_airtable] = users
            #print(desired_users)

        else:
            continue
    except KeyError:
        continue

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

        "project": {"name": project_name, "color": "#ffffff", "icon": "✅"}, # icon is emoji symbol
        }
#ffeb3b


# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")
client.open()

def create_url_friendly_filename(filename):
    # Replace spaces with hyphens (or underscores)
    filename = filename.replace(' ', '+')
    return filename


for key, value in desired_users.items():
    # Create an Airtable username list here so that through very loop iteration,
    # the list is empty again. So there will be 1 username in each iteration,
    #that is connected with the specific Airtable ID
    list_user_name_airtable = []
    id_airtable = key
    list_user_name_airtable.append(value)
    provisioned_users = client.projects.create_app_users(display_names=list_user_name_airtable, forms=FORMS_TO_ACCESS, project_id=PROJECT_ID)
    #print(id_airtable, provisioned_users)

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

        #png.save(f"settings-{user.displayName}.png", format="PNG")
        #png.save(byte_io, 'PNG')
        #png = img_byte_arr.getvalue()
        in_mem_file.seek(0)
        #file_name = '/Users/edmond/Documents/Python_scripts/GetODK/settings-veritree_melissa hartog.png'
        #response = s3_client.upload_file(Filename=png, Bucket=S3_BUCKET,Key=f"settings-{user.displayName}.png")
        #response = s3_client.put_object(Filename=img_byte_arr, Bucket=S3_BUCKET,Key=f"settings-{user.displayName}.png")
        #response = s3_client.put_object(Filename=img_byte_arr, Bucket=S3_BUCKET,Key=f"settings-{user.displayName}.png")

        # DEZE WERK! response = s3_client.upload_fileobj(in_mem_file, 'getodk-qr-codes','test.png')
        response = s3_client.upload_fileobj(in_mem_file, 'getodk-qr-codes',f"settings-{user.displayName}.png")

        get_bucket_location = s3_client.get_bucket_location(Bucket=S3_BUCKET)['LocationConstraint']

        name = f"settings-{user.displayName}.png"

        url_s3 = "https://s3-%s.amazonaws.com/%s/%s" % (get_bucket_location, 'getodk-qr-codes', name)

        url_s3 = create_url_friendly_filename(url_s3)
        #https://getodk-qr-codes.s3.eu-north-1.amazonaws.com/settings-fundacion+dia_user_12.png
        print(url_s3)

        # get_url = s3_client.get_object_url('getodk-qr-codes', f"settings-{user.displayName}.png")
        # print(get_url)

        # Upload url to Airtable
        row_airtable_to_update = f"https://api.airtable.com/v0/appkx2PPsqz3axWDy/ODK users/{id_airtable}"

        # Set the new field values for the record
        update_fields_airtable = {'fields':{'QR code': url_s3}}
        #data_airtable = {'fields': update_fields_airtable}

        # Send your request to update the record and parse the response
        response_airtable = requests.patch(row_airtable_to_update, headers=headers, json=update_fields_airtable)
        data = json.loads(response_airtable.text)
        print(data)
