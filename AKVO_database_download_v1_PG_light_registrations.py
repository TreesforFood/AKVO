import requests
import json
import psycopg2
import re
import geojson
import geodaisy.converters as convert
from area import area
import os
from akvo_api_config import Config

config = Config()

#form_monitoring_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=11980001'
form_response_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=211852238&form_id=242817396&page_size=200'

# get the token from AKVO
data = {"client_id": config.CONF["CLIENT_ID"], "username" : config.CONF["USERNAME"], "password": config.CONF["PASSWORD"], "grant_type": config.CONF["GRANT_TYPE"], "scope": config.CONF["SCOPE"]}
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
url_list.append(form_response_tree) # this one is needed to add the first url to the url list

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


#connect to Postgresql database
conn = psycopg2.connect(host= config.CONF["HOST_PSTGRS"],database= config.CONF["DATABASE_PSTGRS"],user= config.CONF["USER_PSTGRS"],password= config.CONF["PASSWORD_PSTGRS"])

cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_registration_locations_light_version;
DROP TABLE IF EXISTS AKVO_Tree_registration_species_light_version;
DROP TABLE IF EXISTS AKVO_Tree_registration_photos_light_version;

CREATE TABLE AKVO_Tree_registration_locations_light_version (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER,
submission DATE, submitter TEXT, AKVO_form_version NUMERIC(10,2), country TEXT, test TEXT, organisation TEXT, contract_number NUMERIC(20,2),
id_planting_site TEXT, name_village TEXT, name_owner TEXT, remark TEXT, planting_date TEXT, tree_number INTEGER, planting_distance NUMERIC(20,2),
only_location TEXT, lat_y REAL, lon_x REAL, centroid_coord geography(POINT, 4326));

CREATE TABLE AKVO_Tree_registration_photos_light_version (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_geotag_location geography(POINT, 4326));

CREATE TABLE AKVO_Tree_registration_species_light_version (identifier_akvo TEXT, instance INTEGER, lat_name_species TEXT, local_name_species TEXT, number_species INTEGER);

''')

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
    if (end - start) > 60:
        #message_load_registration_data = client.messages.create(body = "It seems that the script hangs too long",
        #from_ = "+16614853992", to = "+310640569655")
        #message_load_registration_data.sid
        print("It seems that the script hangs too long")


    for registration_light in json_dict['formInstances']:
        #print(registration_light)
        try:
            identifier_akvo = registration_light['identifier']
        except (IndexError,KeyError):
            identifier_akvo = None
        else:
            identifier_akvo = registration_light['identifier']
            #print(identifier_akvo)

        try:
            displayname = registration_light['displayName']
        except (IndexError,KeyError):
            displayname = None
        else:
            displayname = registration_light['displayName']
            #print(displayname)

        try:
            device_id = registration_light['deviceIdentifier']
        except (IndexError,KeyError):
            device_id = None
        else:
            device_id = registration_light['deviceIdentifier']
            #print(device_id)

        try:
            instance = registration_light['id']
        except (IndexError,KeyError):
            instance = None
        else:
            instance = registration_light['id']
            #print(instance)

        try:
            submitter = registration_light['submitter']
        except (IndexError,KeyError):
            submitter = None
        else:
            submitter = registration_light['submitter']
            #print(submitter)

        try:
            submission_date = registration_light['submissionDate']
        except (IndexError,KeyError):
            submission_date = None
        else:
            submission_date = registration_light['submissionDate']
            submission_date_trunc = left(submission_date,10)
            #print(submission_date)

        try:
            form_version = registration_light['formVersion']
        except (IndexError,KeyError):
            form_version = None
        else:
            form_version = registration_light['formVersion']
            form_version = str(form_version)

        try:
            country = registration_light['responses']['252306883'][0]['241550309'][0]['name']
        except (IndexError,KeyError):
            country = None
        else:
            country = registration_light['responses']['252306883'][0]['241550309'][0].get('name')

        try:
            organisation = registration_light['responses']['252306883'][0]['241550309'][1]['name']
        except (IndexError,KeyError):
            organisation = None
        else:
            organisation = registration_light['responses']['252306883'][0]['241550309'][1].get('name')


        test = registration_light['responses']['252306883'][0]['241550308'][0].get('text')
        #print(test)

        contract_number = registration_light['responses']['252306883'][0].get('241550306')
        #print(contract_number)

        name_village = registration_light['responses']['252306883'][0].get('264400049')
        #print(name_village)

        planting_site_id = registration_light['responses']['252306883'][0].get('241550310')
        #print(planting_site_id)

        name_owner = registration_light['responses']['252306883'][0].get('241550307')
        #print(name_owner)

        number_trees_registered = registration_light['responses']['252306883'][0].get('241550312')
        #print(number_trees_registered)

        try:
            lat_y = registration_light['responses']['252306885'][0]['205357333']['lat']
        except (IndexError,KeyError):
            lat_y_str = None
        else:
            lat_y = registration_light['responses']['252306885'][0]['205357333']['lat']
            lat_y_str = str(lat_y)

        try:
            lon_x = registration_light['responses']['252306885'][0]['205357333']['long']
        except (IndexError,KeyError):
            lon_x_str = None
        else:
            lon_x = registration_light['responses']['252306885'][0]['205357333']['long']
            lon_x_str = str(lon_x)

        if lat_y is None or lon_x is None:
            centroid_coord = None
        elif lat_y == 'None' or lon_x == 'None':
            centroid_coord = None
        else:
            lat_y_coord = str(lat_y)
            lon_x_coord = str(lon_x)
            centroid_coord = 'POINT (' + lon_x_coord +' '+ lat_y_coord +')'


        avg_planting_distance = registration_light['responses']['252306885'][0].get('245710152')
        #print(avg_planting_distance)

        planting_date = registration_light['responses']['252306885'][0].get('258591057')
        planting_date_trunc = left(planting_date,10)
        #print(planting_date_trunc)

        remark = registration_light['responses']['264300438'][0].get('264360226')
        #print(remark)

        multiple_planting_locations = registration_light['responses']['264300438'][0]['250980826'][0].get('text')
        #print(multiple_planting_locations)

        # Populate the tree registration table
        cur.execute('''INSERT INTO AKVO_Tree_registration_locations_light_version (identifier_akvo, display_name, device_id, instance, submission, submitter, AKVO_form_version, country, test, organisation, contract_number, id_planting_site, name_village, name_owner, remark, planting_date, tree_number, planting_distance, only_location, lat_y, lon_x, centroid_coord)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (identifier_akvo, displayname, device_id, instance, submission_date_trunc, submitter, form_version, country, test, organisation, contract_number, planting_site_id, name_village, name_owner, remark, planting_date_trunc, number_trees_registered, avg_planting_distance, multiple_planting_locations, lat_y_str, lon_x_str, centroid_coord))

        conn.commit()

        # populate database with the 10 photos taken
        for k,v in registration_light['responses']['252306886'][0].items():
            photo_urls = v.get('filename')
            if v['location'] is not None:
                    photo_geotag_lat = v.get('location')['latitude']
                    photo_lat = str(photo_geotag_lat)
                    photo_geotag_lon = v.get('location')['longitude']
                    photo_lon = str(photo_geotag_lon)
                    photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                    photo_geotag_location = None

            else:
                photo_lat = None
                photo_lon = None
                photo_geotag_location = None

            cur.execute('''INSERT INTO AKVO_Tree_registration_photos_light_version (identifier_akvo, instance, photo_url, photo_geotag_location)
            VALUES (%s,%s,%s,%s)''', (identifier_akvo, instance, photo_urls, photo_geotag_location))

            conn.commit()


        # populate database with species and species number
        try:
            registration_light['responses']['252306884']

        except KeyError:
            species_latin = ''
            species_local = ''
            number_species = 0

        else:
            for x in registration_light['responses']['252306884']:
                try:
                    species_latin = x['248220146'][1]['code']
                except (KeyError, IndexError):
                    try:
                        species_latin = x['248220146'][0]['code']
                    except (KeyError, IndexError):
                        species_latin = None
                    else:
                        species_latin = x['248220146'][0]['code']
                else:
                    species_latin = x['248220146'][1]['code']

                try:
                    species_local = x['248220146'][1]['name']
                except (KeyError, IndexError):
                    try:
                        species_local = x['248220146'][0]['name']
                    except (KeyError, IndexError):
                        species_local = None
                    else:
                        species_local = x['248220146'][0]['name']
                else:
                    species_local = x['248220146'][1]['name']

                number_trees_planted_per_species = x.get('248220145', 0)
                #print(number_trees_planted_per_species)
                #print(species_latin, species_local)

                cur.execute('''INSERT INTO AKVO_Tree_registration_species_light_version (identifier_akvo, instance, lat_name_species, local_name_species, number_species)
                VALUES (%s,%s,%s,%s,%s)''', (identifier_akvo, instance, species_latin, species_local, number_trees_planted_per_species))

                conn.commit()
