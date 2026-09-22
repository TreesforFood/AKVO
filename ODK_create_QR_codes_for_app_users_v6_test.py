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
#central_email = 'edmond.muller@ecosia.org'
default_project_id = 1

# Define the GetODK file content
file_content = f"""[central]
base_url = "{base_url}"
username = "{username}"
password = "{password}"
default_project_id = {default_project_id}
"""


# Settings for QR code
PROJECT_ID = 1
PROJECT_NAME = 'ecosia'
ADMIN_PASSWORD = "ecosia_change_settings"


# Define a writable path for GetODK (/app/tmp is a writable directory on Heroku)
file_path = "/app/tmp/pyodk_config.ini"

# Create the GetODK directory if it doesn't exist
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Write the GetODK configuration to the file
with open(file_path, "w") as file:
    file.write(file_content)


# Set the property value to each app user for organisation so that it can be used as a filter for the entity list
# First get the app user id:
with Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini"):


    response_app_users = client.get(f"https://ecosia.getodk.cloud/projects/{PROJECT_ID}/app-users")

    for app_user in response_app_users.json():
        print(app_user)
        app_user_id = app_user['id']
        print('app_user_id: ', app_user_id)

        # Get the organisation name from the username:
        organisation = app_user['displayName']

        # Set the property value 'organisation' for the user:
        respons_user_id = client.patch(
        f"https://ecosia.getodk.cloud/projects/{PROJECT_ID}/app-users/{app_user_id}",
        json={
            "properties": {
                "organisation": {organisation}}},)
    #print(response.json())
