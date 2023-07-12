import requests
import json
import psycopg2
import re
import geojson
import geodaisy.converters as convert
from area import area
import os

config = Config()

#form_monitoring_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=11980001'
devices_api = 'https://api-auth0.akvo.org/flow/orgs/ecosia/devices'

# get the token from AKVO
data = {"client_id": os.environ["CLIENT_ID"], "username" : os.environ["USERNAME"], "password": os.environ["PASSWORD"], "grant_type": os.environ["GRANT_TYPE"], "scope": os.environ["SCOPE"]}
response = requests.post("https://akvofoundation.eu.auth0.com/oauth/token", data=data)

if response.status_code in [200]: # in case good response from AKVO server
    tok_dict = json.loads(response.text)
    expires_in = tok_dict["expires_in"]
    token_type = tok_dict["token_type"]
    access_token = tok_dict["access_token"]
    token_id = tok_dict["id_token"]
else: # in case of error from AKVO server
    print(response.text)

headers = {'Authorization': "Bearer {}".format(token_id), 'Accept': 'application/vnd.akvo.flow.v2+json'}

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

# Create list with first url from registration form
url_list = list()
url_list.append(devices_api) # this one is needed to add the first url to the url list

counting_pages = 0

# Add other next-URL's to the list of registration forms
for all_pages in url_list:
    counting_pages = counting_pages + 1
    start = time.time()
    print("URL retrieved for listing: ", all_pages)
    print('Total processed urls = ', counting_pages)
    load_page = requests.get(all_pages, headers=headers).content
    page_decode = load_page.decode()
    try:
        json_instance = json.loads(page_decode)
        #print("OUTPUT JSON: ", json_instance)

    except json.decoder.JSONDecodeError:
        print('A json file seems to be empty')
    #json_instance = json.loads(page_decode)
    if (json_instance.get('nextPageUrl') is None): #can also try this: if (json_dict['nextPageUrl'] is None ) : continue
        url_list.append(all_pages) # This is needed to add the last instances at the last url page
        break
    else:
        url_subseq_page = json_instance.get('nextPageUrl')
        url_list.append(url_subseq_page)
        end = time.time()
        #print("end time to collect urls: ", end)
        if (end - start) > 60:
            print("It seems that the script hangs too long")

def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]

count_pages_registration_data = 0

for all_data in url_list:
    start = time.time()
    load_page = requests.get(all_data, headers=headers).content
    page_decode = load_page.decode()
    json_dict = json.loads(page_decode)
    count_page = count_pages_registration_data + 1
    print("Nr. processed pages registration data: ", count_page)
    end = time.time()
    #print(json_dict)
    if (end - start) > 60:
        print("It seems that the script hangs too long")

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS superset_ecosia_new_devices;

CREATE TABLE superset_ecosia_new_devices (id_new_device TEXT, last_contact DATE, device_group TEXT);

''')

for x in json_dict['devices']:
    if not x['deviceGroup']:
        id_new_device = x['deviceId']
        last_contact = x['lastContact']
        device_group = x['deviceGroup']
        print(id_new_device, last_contact, device_group)
    else:
        continue

    cur.execute('''INSERT INTO superset_ecosia_new_devices (id_new_device, last_contact, device_group)
    VALUES (%s,%s,%s)''', (id_new_device, last_contact, device_group))

    cur.execute('''SELECT * FROM superset_ecosia_new_devices ORDER BY last_contact DESC''')

    conn.commit()
