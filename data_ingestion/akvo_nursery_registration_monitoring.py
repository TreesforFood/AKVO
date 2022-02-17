import requests
import json
import re
import geojson
import psycopg2
import geodaisy.converters as convert
from area import area
import os



# AKVO url entry levels. Modify the id to get the right folder/survey:
form_registration_nursery = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=38030003&form_id=30050006'
form_monitoring_nursery = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=38030003&form_id=6070006'

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


# Create list with first url from registration form
url_list = list()
url_list.append(form_registration_nursery) # this one is needed to add the first url to the url list


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

#print(url_list)
#connect to Postgresql database
#conn = psycopg2.connect(host= os.environ["HOST_PSTGRS"],database= os.environ["DATABASE_PSTGRS"],user= os.environ["USER_PSTGRS"],password= os.environ["PASSWORD_PSTGRS"])
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()


cur.execute('''
DROP TABLE IF EXISTS AKVO_Nursery_registration_photos;
DROP TABLE IF EXISTS AKVO_Nursery_monitoring_photos;
DROP TABLE IF EXISTS AKVO_Nursery_monitoring_tree_species;
DROP TABLE IF EXISTS AKVO_Nursery_monitoring;
DROP TABLE IF EXISTS AKVO_Nursery_registration;


CREATE TABLE AKVO_Nursery_registration (identifier_akvo TEXT PRIMARY KEY, display_name TEXT, device_id TEXT,
instance INTEGER, submission DATE, submission_year NUMERIC,
submitter TEXT, country TEXT, test TEXT, organisation TEXT, nursery_type TEXT, nursery_name TEXT, newly_established TEXT,
full_tree_capacity NUMERIC, lat_y REAL, lon_x REAL, elevation REAL, centroid_coord geography(POINT, 4326));

CREATE TABLE AKVO_Nursery_registration_photos (identifier_akvo TEXT, photo_url TEXT, centroid_coord geography(POINT, 4326));

CREATE TABLE AKVO_Nursery_monitoring (identifier_akvo TEXT, instance INTEGER, submission_date DATE, submission_time TEXT,
submitter TEXT, name_nursery_manager TEXT, test TEXT, gender_nursery_manager TEXT, challenges_nursery TEXT,
number_trees_produced_currently NUMERIC, month_planting_stock TEXT, nr_working_personel NUMERIC);

CREATE TABLE AKVO_Nursery_monitoring_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, centroid_coord geography(POINT, 4326));

CREATE TABLE AKVO_Nursery_monitoring_tree_species (identifier_akvo TEXT, instance NUMERIC, tree_species_latin TEXT,
tree_species_local TEXT);

''');


def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]


for all_data in url_list:
    load_page = requests.get(all_data, headers=headers).content
    page_decode = load_page.decode()
    json_dict = json.loads(page_decode)



    # Get all the tree registration data
    for level1 in json_dict['formInstances']:
        modifiedat = level1['modifiedAt']
        formversion = level1['formVersion']
        identifier = level1['identifier']
        displayname = level1['displayName']
        deviceidentifier = level1['deviceIdentifier']
        instance = level1['id']
        submissiondate = level1['submissionDate']
        submissiontime = mid(submissiondate, 11,19)

        try:
            submissiondate_trunc = left(submissiondate,10)
        except:
            submissiondate_trunc = None

        try:
            submissiondate_trunc_year = left(submissiondate,4)
        except:
            submissiondate_trunc_year = None


        submitter = level1['submitter']

        try:
            country = level1['responses']['10050016'][0]['14200004'][0]['name']
        except (KeyError, IndexError):
            country = ''

        try:
            test = level1['responses']['10050016'][0]['6410091'][0]['text']
        except (KeyError, IndexError):
            test = ''

        try:
            name_organisation = level1['responses']['10050016'][0]['14200004'][1]['name']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            name_organisation = ''

        try:
            nursery_type = level1['responses']['10050016'][0]['2120005'][0]['text']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            nursery_type = ''

        try:
            nursery_name = level1['responses']['10050016'][0]['170003']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            nursery_name = ''

        try:
            new_established = level1['responses']['10050016'][0]['12210002'][0]['text']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            new_established = ''

        try:
            tree_production_full_cap = level1['responses']['10050016'][0]['24170005']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            tree_production_full_cap = None

        try:
            lat_centr = level1['responses']['10050016'][0]['30140002']['lat']
            lat_centr_conv = str(lat_centr)
        except (KeyError, IndexError):
            lat_centr = None

        try:
            lon_centr = level1['responses']['10050016'][0]['30140002']['long']
            lon_centr_conv = str(lon_centr)
        except (KeyError, IndexError):
            lon_centr = None

        try:
            centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'
        except (KeyError, IndexError):
            centroid_coord = None

        try:
            elevation = level1['responses']['10050016'][0]['30140002']['elev']
        except (KeyError, IndexError):
            elevation = None


        # Populate the tree registration table
        cur.execute('''INSERT INTO AKVO_Nursery_registration (identifier_akvo, display_name, device_id, instance, submission, submission_year,
        submitter, country, test, organisation, nursery_type, nursery_name, newly_established, full_tree_capacity, lat_y, lon_x, elevation, centroid_coord)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, displayname, deviceidentifier, instance, submissiondate_trunc, submissiondate_trunc_year, submitter, country, test, name_organisation,
        nursery_type, nursery_name, new_established, tree_production_full_cap, lat_centr, lon_centr, elevation, centroid_coord))

        conn.commit()

        try:
            level1['responses']['46400174']
        except KeyError:
            photo_location = None
            photo_url = None
        else:
            for photo in level1['responses']['46400174']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
                photo_list = list(photo.values())
                for photo in photo_list:
                    photo_url = photo['filename']
                    try: #print(photo_url) # print multiple rows well up to here with only urls
                        if photo['location'] is not None:
                            photo_lat = photo['location']['latitude']
                            photo_lon = photo['location']['longitude']
                            photo_lat = str(photo_lat)
                            photo_lon = str(photo_lon)
                            photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                    except:
                        photo_lat = None
                        photo_lon = None
                        photo_location = None


                    cur.execute('''INSERT INTO AKVO_Nursery_registration_photos (identifier_akvo, photo_url, centroid_coord) VALUES (%s,%s,%s)''', (identifier, photo_url, photo_location))

                    conn.commit()


# Create list with first url from monitoring form
url_list_m = list()
url_list_m.append(form_monitoring_nursery) # this one is needed to add the first url to the url list


# Add other next-URL's to the list of registration forms
for all_pages_m in url_list_m:
    load_page_m = requests.get(all_pages_m, headers=headers).content
    page_decode_m = load_page_m.decode()
    json_instance_m = json.loads(page_decode_m)

    if (json_instance_m.get('nextPageUrl') is None): #can also try this: if (json_dict['nextPageUrl'] is None ) : continue
        url_list_m.append(all_pages_m) # This is needed to add the last instances at the last url page
        break
    else:
        url_subseq_page_m = json_instance_m.get('nextPageUrl')
        url_list_m.append(url_subseq_page_m)


for all_data_m in url_list_m:
    load_page_m = requests.get(all_data_m, headers=headers).content
    page_decode_m = load_page_m.decode()
    json_dict_m = json.loads(page_decode_m)

    counting = 0

    # Get all the nursery monitoring data
    for level1_m in json_dict_m['formInstances']:
        identifier = level1_m['identifier']
        displayname = level1_m['displayName']
        deviceidentifier = level1_m['deviceIdentifier']
        instance = level1_m['id']
        submission_date = level1_m['submissionDate']
        submission_time = mid(submissiondate, 11,19)
        submitter = level1_m['submitter']


        try:
            name_nursery_manager = level1_m['responses']['28140007'][0]['24170016']
        except (KeyError, IndexError):
            name_nursery_manager = ''

        try:
            test = level1_m['responses']['28020008'][0]['33540002']
        except (KeyError, IndexError):
            test = ''

        try:
            gender_nursery_manager = level1_m['responses']['28140007'][0]['38140012'][0]['text']
        except (KeyError, IndexError):
            gender_nursery_manager = ''

        try:
            challenges_nursery = level1_m['responses']['28140007'][0]['6170011']
        except (KeyError, IndexError):
            challenges_nursery = ''

        try:
            number_trees_produced_currently = level1_m['responses']['28020008'][0]['6060029']
        except (KeyError, IndexError):
            number_trees_produced_currently = None

        try:
            for list_month_planting_stock in level1_m['responses']['28020008'][0]['196392790']:
                select_value = list_month_planting_stock['code']
                print_month_planting_stock +=  select_value + ' '

        except (KeyError, IndexError):
            print_month_planting_stock = ''

        try:
            nr_working_personel = level1_m['responses']['28020008'][0]['26180008']
        except (KeyError, IndexError):
            nr_working_personel = None

        cur.execute('''INSERT INTO AKVO_Nursery_monitoring (identifier_akvo, instance, submission_date, submission_time,
        submitter, name_nursery_manager, test, gender_nursery_manager, challenges_nursery,
        number_trees_produced_currently, month_planting_stock, nr_working_personel)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, submission_date, submission_time,
        submitter, name_nursery_manager, test, gender_nursery_manager, challenges_nursery,
        number_trees_produced_currently, print_month_planting_stock, nr_working_personel))

        conn.commit()

        try:
            level1_m['responses']['28020008']

        except KeyError:
            photo_location_m = None
            photo_url_m = None
        else:
            for photo_m in level1_m['responses']['28020008']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
                photo_m.pop('26180008', None)
                photo_m.pop('6060029', None)
                photo_m.pop('196392790', None)
                photo_list_m = list(photo_m.values())

                for photo_m in photo_list_m:
                    photo_url_m = photo_m['filename']

                    try: #print(photo_url) # print multiple rows well up to here with only urls
                        if photo_m['location'] is not None:
                            photo_lat_m = photo_m['location']['latitude']
                            photo_lon_m = photo_m['location']['longitude']
                            photo_lat_m = str(photo_lat_m)
                            photo_lon_m = str(photo_lon_m)
                            photo_location_m = 'POINT('+ photo_lon_m + ' ' + photo_lat_m + ')'

                    except:
                        photo_lat_m = None
                        photo_lon_m = None
                        photo_location_m = None


                    cur.execute('''INSERT INTO AKVO_Nursery_monitoring_photos (identifier_akvo, instance, photo_url, centroid_coord)
                    VALUES (%s,%s,%s,%s)''', (identifier, instance, photo_url_m, photo_location_m))

                    conn.commit()

        try:
            level1_m['responses']['16220016']

        except KeyError:
            species_latin_m = ''
            species_local_m = ''

        else:
            for x in level1_m['responses']['16220016']:
                try:
                    species_latin_m = x['28130014'][1]['code']

                except (KeyError, IndexError):
                    species_latin_m = None
                else:
                    species_latin_m = x['28130014'][1]['code']

                try:
                    species_local_m = x['28130014'][1]['name']

                except (KeyError, IndexError):
                    species_local_m = None
                else:
                    species_local_m = x['28130014'][1]['name']

                cur.execute('''INSERT INTO AKVO_Nursery_monitoring_tree_species (identifier_akvo, instance, tree_species_latin, tree_species_local)
                VALUES (%s,%s,%s,%s)''', (identifier, instance, species_latin_m, species_local_m))

                conn.commit()
