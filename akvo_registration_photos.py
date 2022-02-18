import requests
import json
import sqlite3
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
import os

form_response_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=48090001'
initial_sync_request = 'https://api-auth0.akvo.org/flow/orgs/ecosia/sync?initial=true' # with this link, you get the first page with the NextUrl in it. This URL is being send by an api call.

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

# The code block below must only be executed during a full download of the database. The NextSyncUrl that restuls from the Initial Request is used
# to update (sync) the database. As can be seen, thsi URL is written to a file (sync_urls.txt) that is used to store the url for later sync requests.
get_initial_syncurl = requests.get(initial_sync_request, headers = headers)
converturltotxt = get_initial_syncurl.text
converttojson = json.loads(converturltotxt)
initial_syncurl = converttojson.get('nextSyncUrl',"Ecosia: No sync url was found in this instance")
with open('sync_urls_PG.txt', 'w') as f:
    f.write(initial_syncurl)

with open('sync_initial_url_after download_PG.txt', 'w') as f:
    f.write(initial_syncurl)

print("This is the first NextSyncUrl after the initial request: ", initial_syncurl)


# Create list with first url from registration form
url_list = list()
url_list.append(form_response_tree) # this one is needed to add the first url to the url list


# Add other next-URL's to the list of registration forms
for all_pages in url_list:
    load_page = requests.get(all_pages, headers=headers).content
    page_decode = load_page.decode()
    json_instance = json.loads(page_decode)
    if (json_instance.get('nextPageUrl') is None): #can also try this: if (json_dict['nextPageUrl'] is None ) : continue
        url_list.append(all_pages) # This is needed to add the last instances at the last url page
        break
    else:
        url_subseq_page = json_instance.get('nextPageUrl')
        url_list.append(url_subseq_page)
        print('urls loaded')

#connect to Postgresql database
#conn = psycopg2.connect(host= os.environ["HOST_PSTGRS"],database= os.environ["DATABASE_PSTGRS"],user= os.environ["USER_PSTGRS"],password= os.environ["PASSWORD_PSTGRS"])
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_registration_photos;

CREATE TABLE AKVO_Tree_registration_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_location geography(POINT, 4326), FOREIGN KEY (identifier_akvo) REFERENCES AKVO_Tree_registration_areas (identifier_akvo) ON DELETE CASCADE);

''')

def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]


for all_data in url_list:
    load_page = requests.get(all_data, headers=headers).content
    page_decode = load_page.decode()
    json_dict = json.loads(page_decode)
    print('jsons loaded')


    # Get all the tree registration data
    for level1 in json_dict['formInstances']:
        identifier = level1['identifier']
        instance = level1['id']

        try:
            level1['responses']['1960007']
        except KeyError:
            photo_r4_location = None
            photo_r4_url = None
        else:
            for photo in level1['responses']['1960007']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
                photo.pop('5900011', None)
                photo.pop('50110008', None)
                photo.pop('25860015', None)
                photo_items4 = list(photo.values())


                for url4 in photo_items4:
                    photo_r4_url = url4['filename']

                    try: #print(photo_url) # print multiple rows well up to here with only urls
                        if url4['location'] is not None:
                            photo_lat = url4['location']['latitude']
                            photo_lon = url4['location']['longitude']
                            photo_lat = str(photo_lat)
                            photo_lon = str(photo_lon)
                            photo_r4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                    except:
                        photo_lat = None
                        photo_lon = None
                        photo_r4_location = None

                    #print('photo:',photo_url, photo_location, identifier, count) Prints well multiple photos and id's up to here.
                    cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_location)
                    VALUES (%s,%s,%s,%s)''', (identifier, instance, photo_r4_url, photo_r4_location))

                    conn.commit()


        try:
            for photo in level1['responses']['3990009']: # Get other 36 photos from registration form. This loop was tested in file: AKVO_database_download_v7_test_rest_36_reg_photos.py
                photo_items36 = list(photo.values())
                for url36 in photo_items36:
                    try:
                        photo_r36_url = url36['filename']
                    except KeyError:
                        photo_r36_url = None
                    else:
                        photo_r36_url = url36['filename']
                        try:
                            if url36['location'] is not None:
                                photo_lat = url36['location']['latitude']
                                photo_lat = str(photo_lat)
                                photo_lon = url36['location']['longitude']
                                photo_lon = str(photo_lon)
                                photo_r36_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                            else:
                                photo_lat = None
                                photo_lon = None
                                photo_r36_location = None

                        except KeyError:
                            photo_lat = None
                            photo_lon = None
                            photo_r36_location = None



                    cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_location)
                    VALUES (%s,%s,%s,%s)''', (identifier, instance, photo_r36_url, photo_r36_location))

                    conn.commit()

        except (IndexError,KeyError):
            photo = ''
            photo_r36_url = ''
            photo_r36_location = ''
