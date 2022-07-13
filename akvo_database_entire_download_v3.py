# This script downloads the entire AKVO database into a local Postgres (PG) database.
# The PG database is then dumped onto Heroku. Reaseon for this approach is that the AWS where AKVO is running
# given a too long time-out while the script is running. This causes Heroku to stop the script.

import requests
import json
import sqlite3
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
from hanging_threads import start_monitoring
start_monitoring(seconds_frozen=10, test_interval=100)
from dotenv import load_dotenv, find_dotenv
import os
from akvo_api_config import Config
import cloudpickle

config = Config()

form_monitoring_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=11980001'
form_response_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=48090001'
initial_sync_request = 'https://api-auth0.akvo.org/flow/orgs/ecosia/sync?initial=true' # with this link, you get the first page with the NextUrl in it. This URL is being send by an api call.

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

# The code block below must only be executed during a full download of the database. The NextSyncUrl that restuls from the Initial Request is used
# to update (sync) the database. As can be seen, thsi URL is written to a file (sync_urls.txt) that is used to store the url for later sync requests.
get_initial_syncurl = requests.get(initial_sync_request, headers = headers)
converturltotxt = get_initial_syncurl.text
converttojson = json.loads(converturltotxt)
initial_syncurl = converttojson.get('nextSyncUrl',"Ecosia: No sync url was found in this instance")
with open('sync_urls_PG.pkl', 'wb') as f:
    cloudpickle.dump(initial_syncurl,f)

with open('sync_initial_url_after download_PG.pkl', 'wb') as f:
    cloudpickle.dump(initial_syncurl,f)

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

#connect to Postgresql database
conn = psycopg2.connect(host= config.CONF["HOST_PSTGRS"],database= config.CONF["DATABASE_PSTGRS"],user= config.CONF["USER_PSTGRS"],password= config.CONF["PASSWORD_PSTGRS"])

cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_registration_species;
DROP TABLE IF EXISTS AKVO_Tree_registration_photos;
DROP TABLE IF EXISTS AKVO_Tree_registration_areas;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_counts;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_pcq;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_photos;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_areas;


CREATE TABLE AKVO_Tree_registration_areas (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER, submission DATE, submission_year SMALLINT, submissiontime TEXT, submitter TEXT, modifiedAt TEXT, AKVO_form_version TEXT, country TEXT, test TEXT, organisation TEXT, contract_number NUMERIC(20,2), id_planting_site TEXT, land_title TEXT, name_village TEXT, name_region TEXT, name_owner TEXT, gender_owner TEXT, objective_site TEXT, site_preparation TEXT, planting_technique TEXT, planting_system TEXT, remark TEXT, nr_trees_option TEXT, planting_date TEXT, tree_number INTEGER, estimated_area NUMERIC(20,3), calc_area NUMERIC(20,3), lat_y REAL, lon_x REAL, number_coord_polygon INTEGER, centroid_coord geography(POINT, 4326), polygon geography(POLYGON, 4326), multipoint geography(MULTIPOINT, 4326));

CREATE TABLE AKVO_Tree_registration_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_location geography(POINT, 4326));

CREATE TABLE AKVO_Tree_registration_species (identifier_akvo TEXT, instance INTEGER, lat_name_species TEXT, local_name_species TEXT, number_species INTEGER);

CREATE TABLE AKVO_Tree_monitoring_areas (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER, submission DATE, submission_year SMALLINT, submitter TEXT, AKVO_form_version TEXT, site_impression TEXT, test TEXT, avg_tree_height REAL, number_living_trees INTEGER, method_selection TEXT, avg_circom_tree_count TEXT, avg_circom_tree_pcq TEXT);

CREATE TABLE AKVO_Tree_monitoring_counts (identifier_akvo TEXT, instance INTEGER, name_species TEXT, loc_name_spec TEXT, number_species INTEGER, avg_circom_tree NUMERIC(20,2), units_circom TEXT);

CREATE TABLE AKVO_Tree_monitoring_pcq (identifier_akvo TEXT, instance INTEGER, lat_pcq_sample REAL, lon_pcq_sample REAL, height_pcq_sample NUMERIC(20,2), units_circom TEXT, Q1_dist NUMERIC(20,2), Q1_hgt NUMERIC(20,2), Q1_circom NUMERIC(20,2), Q1_spec TEXT, Q2_dist NUMERIC(20,2), Q2_hgt NUMERIC(20,2), Q2_circom NUMERIC(20,2), Q2_spec TEXT, Q3_dist NUMERIC(20,2), Q3_hgt NUMERIC(20,2), Q3_circom NUMERIC(20,2), Q3_spec TEXT, Q4_dist NUMERIC(20,2), Q4_hgt NUMERIC(20,2), Q4_circom NUMERIC(20,2), Q4_spec TEXT, pcq_location geography(POINT, 4326));

CREATE TABLE AKVO_Tree_monitoring_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_location geography(POINT, 4326));

''')

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
            submissiondate_trunc = ''

        try:
            submissiondate_trunc_year = left(submissiondate,4)
        except:
            submissiondate_trunc_year = ''


        submitter = level1['submitter']

        try:
            country = level1['responses']['1960001'][0]['42120001'][0]['name']
        except (KeyError, IndexError):
            country = ''

        try:
            test = level1['responses']['1960001'][0]['50100002'][0]['text']
        except (KeyError, IndexError):
            test = ''

        try:
            name_organisation = level1['responses']['1960001'][0]['42120001'][1]['name']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            name_organisation = ''

        contract_number = level1['responses']['1960001'][0].get('52070068', 0.0)
        id_planting_site = level1['responses']['1960001'][0].get('58000002','')

        try:
            land_title = level1['responses']['1960001'][0]['52070069'][0]['text']
        except (KeyError, IndexError): # Since landtitle has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
            land_title = ''

        name_village = level1['responses']['1960001'][0].get('61910570','')
        name_region = level1['responses']['1960001'][0].get('44110002','')
        name_owner = level1['responses']['1960001'][0].get('54050003','')

        try:
            gender_owner = level1['responses']['1960001'][0]['42120003'][0]['text']
        except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
            gender_owner = ''

        try:
            landuse_objective = level1['responses']['1960001'][0]['25860010'][0]['text']
        except (KeyError, IndexError): # Has option 'other' so build in keyerror option
            landuse_objective = ''

        try:
            site_preparation = level1['responses']['1960001'][0]['15910006'][0]['text']
        except (KeyError, IndexError): # Since site preparation has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
            site_preparation = ''

        try:
            planting_technique = level1['responses']['1960001'][0]['42120008'][0]['text']
        except (KeyError, IndexError): # Has option 'other' so build in keyerror option
            planting_technique = ''

        try:
            planting_system = level1['responses']['1960001'][0]['50100006'][0]['text']
        except (KeyError, IndexError): # Has option 'other' so build in keyerror option
            planting_system = ''

        try:
            remark = level1['responses']['1960001'][0].get('50120004','')
        except (KeyError, IndexError):
            remark = ''

        try:
            more_less_200_trees = level1['responses']['1960001'][0]['50120005'][0].get('text','')
        except (KeyError, IndexError):
            more_less_200_trees = ''

        try:
            nr_trees_planted = level1['responses']['56230114'][0].get('3990003')
        except (KeyError, IndexError):
            nr_trees_planted = None

        try:
            if level1['responses']['56230114'][0].get('54050005','') != None:
                planting_date = level1['responses']['56230114'][0].get('54050005','')
                planting_date_trunc = left(planting_date,10)
            else:
                planting_date = ''
        except (KeyError, IndexError):
            planting_date = ''


        try:
            estimated_area = level1['responses']['56230114'][0].get('39860006')
        except (KeyError, IndexError):
            estimated_area = None

        try:
            lat_centr = level1['responses']['1960007'][0]['25860015']['lat'] # It seems that Lat can be None, so the key will be found and 'None' is parsed into a string...?
        except (KeyError, IndexError):
            lat_centr = None
        else:
            lat_centr = level1['responses']['1960007'][0]['25860015']['lat']

        try:
            lon_centr = level1['responses']['1960007'][0]['25860015']['long']
        except (KeyError, IndexError):
            lon_centr = None
        else:
            lon_centr = level1['responses']['1960007'][0]['25860015']['long']

        if lat_centr is None or lon_centr is None:
            centroid_coord = None
        elif lat_centr == 'None' or lon_centr == 'None':
            centroid_coord = None
        else:
            lat_centr_conv = str(lat_centr)
            lon_centr_conv = str(lon_centr)
            centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'

        try:
            geom_get = level1['responses']['1960007'][0]['50110008'] # Up to this level it can go wrong (due to empty entry)
            if geom_get != None:
                geom_get = level1['responses']['1960007'][0]['50110008']['features'][0].get('geometry','')
                area_ha = area(geom_get)
                area_ha = round((area_ha/10000),3)
                geometry = convert.geojson_to_wkt(geom_get)
                get_geom_type = geometry.split(' ',1)
                if get_geom_type[0] == 'POLYGON':
                    polygon_check = convert.geojson_to_wkt(geom_get)
                    coord = re.findall('\s', polygon_check)
                    number_coord_pol = int((len(coord)/2)-1)
                    if number_coord_pol < 3:
                        polygon = None
                    else:
                        polygon = polygon_check
                        multipoint = None

                elif get_geom_type[0] == 'MULTIPOINT':
                    multipoint = convert.geojson_to_wkt(geom_get)
                    polygon = None

            else:
                geom_get = None
                area_ha = None

        except (IndexError, KeyError):
            polygon = None
            coord = None
            number_coord_pol = None
            multipoint = None
            geom_get = None
            area_ha = None
            geometry = None
            get_geom_type = None


        # Populate the tree registration table
        cur.execute('''INSERT INTO AKVO_Tree_registration_areas (identifier_akvo, display_name, device_id, instance, submission, submission_year, submissiontime, submitter, modifiedAt, AKVO_form_version, country, test, organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, gender_owner, objective_site, site_preparation, planting_technique, planting_system, remark, nr_trees_option, planting_date, tree_number, estimated_area, calc_area, lat_y, lon_x, number_coord_polygon, centroid_coord, polygon, multipoint)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, displayname, deviceidentifier, instance, submissiondate_trunc, submissiondate_trunc_year, submissiontime, submitter, modifiedat, formversion, country, test, name_organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, gender_owner, landuse_objective, site_preparation, planting_technique, planting_system, remark, more_less_200_trees, planting_date_trunc, nr_trees_planted, estimated_area, area_ha, lat_centr, lon_centr, number_coord_pol, centroid_coord, polygon, multipoint))

        conn.commit()

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
                print('Check fotos 1: ', photo)
                for photo_value in photo.values():
                    photo_items4=[]
                    if photo_value is not None:
                        print('Check fotos 2: ',photo_value)
                        photo_items4.append(photo_value)

                    for url4 in photo_items4:
                        print('Check fotos 3: ', url4)
                        if url4['filename'] is not None:
                            photo_r4_url = url4['filename']
                            try:
                                url4['location']
                            except KeyError:
                                photo_r4_location = None
                            else:
                                if url4['location'] is not None:
                                    photo_lat1 = url4['location']['latitude']
                                    photo_lon1 = url4['location']['longitude']
                                    photo_lat = str(photo_lat1)
                                    photo_lon = str(photo_lon1)
                                    photo_r4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                                else:
                                    photo_r4_location = None

                        else:
                            photo_r4_url = None
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


        # Create table for the registration of tree species and number per species. This is a loop within the instance
        try:
            level1['responses']['50330190']

        except KeyError:
            species_latin = ''
            species_local = ''
            number_species = 0

        else:
            for x in level1['responses']['50330190']:
                try:
                    species_latin = x['50340047'][1]['code']
                except (KeyError, IndexError):
                    try:
                        species_latin = x['50340047'][0]['code']
                    except (KeyError, IndexError):
                        species_latin = None
                    else:
                        species_latin = x['50340047'][0]['code']
                else:
                    species_latin = x['50340047'][1]['code']

                try:
                    species_local = x['50340047'][1]['name']
                except (KeyError, IndexError):
                    try:
                        species_local = x['50340047'][0]['name']
                    except (KeyError, IndexError):
                        species_local = None
                    else:
                        species_local = x['50340047'][0]['name']
                else:
                    species_local = x['50340047'][1]['name']

                number_species = x.get('50530001', 0)
                #print(code, name, get_number_species)

                cur.execute('''INSERT INTO AKVO_Tree_registration_species (identifier_akvo, instance, lat_name_species, local_name_species, number_species)
                VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, species_latin, species_local, number_species))

                conn.commit()



# Create list with first url from monitoring form
url_list_monitoring = list()
url_list_monitoring.append(form_monitoring_tree)


# Add other next-URL's to the list of monitoring forms
for all_pages_monitoring in url_list_monitoring:
    load_page_monitoring = requests.get(all_pages_monitoring, headers=headers).content
    page_decode_monitoring = load_page_monitoring.decode()
    json_monitoring = json.loads(page_decode_monitoring)

    if (json_monitoring.get('nextPageUrl') is None): #can also try this: if (json_dict['nextPageUrl'] is None ) : continue
        url_list_monitoring.append(all_pages_monitoring)
        break
    else:
        url_subseq_page_monitoring = json_monitoring.get('nextPageUrl')
        url_list_monitoring.append(url_subseq_page_monitoring)

def left(var, amount):
    return var[:amount]

for all_data_monitoring in url_list_monitoring:
    load_page_monitoring = requests.get(all_data_monitoring, headers=headers).content
    page_decode_monitoring = load_page_monitoring.decode()
    json_dict_monitoring = json.loads(page_decode_monitoring)

    for level1_monitoring in json_dict_monitoring['formInstances']:

        submitter_m = level1_monitoring['submitter']
        identifier_m = level1_monitoring['identifier']
        displayname_m = level1_monitoring['displayName']
        device_id_m = level1_monitoring['deviceIdentifier']
        instance_m = level1_monitoring['id']
        submissiondate_m = level1_monitoring['submissionDate']
        try:
            submissiondate_trunc_m = left(submissiondate_m,10)
        except:
            submissiondate_trunc_m = ''

        try:
            submission_year_m = left(submissiondate_m,4)
        except:
            submission_year_m = ''

        formversion_m = level1_monitoring['formVersion']

        try:
            impression_site = level1_monitoring['responses']['50110001'][0]['5900001']
        except KeyError:
            impression_site = ''

        try:
            testing_m = level1_monitoring['42120002'][0]['text']
        except (IndexError,KeyError):
            testing_m = ''

        # Part of the Raw Data sheet questions: Count Method
        try:
            avg_tree_height = level1_monitoring['responses']['50110001'][0]['40300003']
        except (IndexError,KeyError):
            avg_tree_height = None

        try:
            tot_nr_trees_estimate = level1_monitoring['responses']['50110001'][0]['25860003']
        except (IndexError,KeyError):
            tot_nr_trees_estimate = None

        try:
            select_method = level1_monitoring['responses']['50110001'][0]['25860004'][0]['text']
        except (IndexError,KeyError):
            select_method = ''

        try:
            circom_indication_tree_pcq = level1_monitoring['responses']['50110001'][0]['183530001'][0]['text']
        except (IndexError,KeyError):
            circom_indication_tree_pcq = ''

        try:
            circom_indication_tree_count = level1_monitoring['responses']['50110001'][0]['176761123'][0]['text']
        except (IndexError,KeyError):
            circom_indication_tree_count = ''

        try:
            units_circom_pcq = level1_monitoring['responses']['50110001'][0]['183520002'][0]['code']
        except (IndexError,KeyError):
            units_circom_pcq = ''

        try:
            units_circom_count = level1_monitoring['responses']['50110001'][0]['184541091'][0]['code']
        except (IndexError,KeyError):
            units_circom_count = ''


        # Create the tree monitoring raw table
        cur.execute('''INSERT INTO AKVO_Tree_monitoring_areas (identifier_akvo, display_name, device_id, instance, submission, submission_year, submitter, AKVO_form_version, site_impression, test, avg_tree_height, number_living_trees, method_selection, avg_circom_tree_count, avg_circom_tree_pcq)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, displayname_m, device_id_m, instance_m, submissiondate_trunc_m, submission_year_m, submitter_m, formversion_m, impression_site, testing_m, avg_tree_height, tot_nr_trees_estimate, select_method, circom_indication_tree_count, circom_indication_tree_pcq))

        conn.commit()

        #get the 4 first photos in N, S, E, W direction from monitoring
        try:
            level1_monitoring['responses']['50110007']
        except KeyError:
            photo_m4_url = ''
            photo_m4_location = ''
        else:
            try:
                level1_monitoring['responses']['50110007'][0]['40300008']
            except (IndexError,KeyError):
                photo = level1_monitoring['responses']['50110007'][0]
                photo_items_4 = list(photo.values())
            else:
                photo = level1_monitoring['responses']['50110007'][0]
                del photo['40300008']
                photo_items_4 = list(photo.values())

            for url in photo_items_4:
                try:
                    url['filename']
                except KeyError:
                    photo_m4_url = None
                else:
                    photo_m4_url = url['filename']
                    if url['location'] is not None:
                        photo_lat = url['location']['latitude']
                        photo_lon = url['location']['longitude']
                        photo_lat = str(photo_lat)
                        photo_lon = str(photo_lon)
                        photo_m4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                    else:
                        photo_lat = None
                        photo_lon = None
                        photo_m4_location = None

                    cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_url, photo_location)
                    VALUES (%s,%s,%s,%s)''', (identifier_m, instance_m, photo_m4_url, photo_m4_location))

                    conn.commit()


        # get the 36 other photos from the monitoring form
        try:
            for photo in level1_monitoring['responses']['40300009']:
                photo_items_36 = list(photo.values())
                for url in photo_items_36:
                    try:
                        photo_m36_url = url['filename']
                        if url['location'] is not None:
                            photo_lat = url['location']['latitude']
                            photo_lon = url['location']['longitude']
                            photo_lat = str(photo_lat)
                            photo_lon = str(photo_lon)
                            photo_m36_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                        else:
                            photo_lat = None
                            photo_lon = None
                            photo_m36_location = None

                        cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_url, photo_location)
                        VALUES (%s,%s,%s,%s)''', (identifier_m, instance_m, photo_m36_url, photo_m36_location))
                        conn.commit()

                    except (IndexError,KeyError):
                            photo_url = ''

        except (IndexError,KeyError):
            photo = ''


        # Part of the Group 2 sheet questions: Counting trees method
        try:
            for species_count in level1_monitoring['responses']['40310002']:

                try:
                    tree_lat_species_name = species_count['50120001'][1]['code']
                except (IndexError,KeyError):
                    tree_lat_species_name = ''
                try:
                    tree_loc_species_name = species_count['50120001'][1]['name']
                except (IndexError,KeyError):
                    tree_loc_species_name = ''
                try:
                    tree_number_count = species_count['68330044']
                except (IndexError,KeyError):
                    tree_number_count = None
                try:
                    avg_tree_circom_count = level1_monitoring['169290073']
                except (IndexError,KeyError):
                    avg_tree_circom_count = None


                    cur.execute('''INSERT INTO AKVO_Tree_monitoring_counts (identifier_akvo, instance, name_species, loc_name_spec, number_species, avg_circom_tree)
                    VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_m, instance_m, tree_lat_species_name, tree_loc_species_name, tree_number_count, avg_tree_circom_count))

                    conn.commit()

        except (IndexError,KeyError):
            tree_species_count = ''
            tree_number_count = ''
            avg_tree_circom_count = ''
            tree_loc_species_count = ''


        # Part of the Group 2 sheet questions: PCQ method
        try:
            if len(identifier_m) > 0:
                for pcq_results in level1_monitoring['responses']['39860004']:
                    try:
                        lat_sample_pcq = pcq_results['54050004']['lat']
                    except (IndexError,KeyError):
                        lat_sample_pcq = None
                    else:
                        lat_sample_pcq = pcq_results['54050004']['lat']
                        lat_sample_pcq_str = str(lat_sample_pcq)

                    try:
                        lon_sample_pcq = pcq_results['54050004']['long']
                    except (IndexError,KeyError):
                        lon_sample_pcq = None
                    else:
                        lon_sample_pcq = pcq_results['54050004']['long']
                        lon_sample_pcq_str = str(lon_sample_pcq)

                    if not lat_sample_pcq_str or not lon_sample_pcq_str:
                        pcq_location = None
                    else:
                        pcq_location = 'POINT('+ lon_sample_pcq_str + ' ' + lat_sample_pcq_str + ')'

                    try:
                        elev_sample_pcq = pcq_results['54050004']['elev']
                    except (IndexError,KeyError):
                        elev_sample_pcq = None
                    try:
                        Q1_distance = pcq_results['15920003']
                    except (IndexError,KeyError):
                        Q1_distance = None
                    try:
                        Q1_height = pcq_results['11980002']
                    except (IndexError,KeyError):
                        Q1_height = None
                    try:
                        Q1_circom = pcq_results['183410039']
                    except (IndexError,KeyError):
                        Q1_circom = None
                    try:
                        pcq_results['40310003'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['40310003'][0]['name']
                        except (IndexError,KeyError):
                            Q1_species = ''
                        else:
                            Q1_species = pcq_results['40310003'][0]['name']
                    else:
                        Q1_species = pcq_results['40310003'][1]['code']
                    try:
                        Q2_distance = pcq_results['40300005']
                    except (IndexError,KeyError):
                        Q2_distance = None
                    try:
                        Q2_height = pcq_results['21860001']
                    except (IndexError,KeyError):
                        Q2_height = None
                    try:
                        Q2_circom = pcq_results['183440030']
                    except (IndexError,KeyError):
                        Q2_circom = None

                    try:
                        pcq_results['40300006'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['40300006'][0]['name']
                        except (IndexError,KeyError):
                            Q2_species = ''
                        else:
                            Q2_species = pcq_results['40300006'][0]['name']
                    else:
                        Q2_species = pcq_results['40300006'][1]['code']

                    try:
                        Q3_distance = pcq_results['25860009']
                    except (IndexError,KeyError):
                        Q3_distance = None
                    try:
                        Q3_height = pcq_results['5900005']
                    except (IndexError,KeyError):
                        Q3_height = None
                    try:
                        Q3_circom = pcq_results['190350004']
                    except (IndexError,KeyError):
                        Q3_circom = None

                    try:
                        pcq_results['48090004'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['48090004'][0]['name']
                        except (IndexError,KeyError):
                            Q3_species = ''
                        else:
                            Q3_species = pcq_results['48090004'][0]['name']
                    else:
                        Q3_species = pcq_results['48090004'][1]['code']

                    try:
                        Q4_distance = pcq_results['54040005']
                    except (IndexError,KeyError):
                        Q4_distance = None
                    try:
                        Q4_height = pcq_results['50110005']
                    except (IndexError,KeyError):
                        Q4_height = None
                    try:
                        Q4_circom = pcq_results['173040032']
                    except (IndexError,KeyError):
                        Q4_circom = None

                    try:
                        pcq_results['11990005'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['11990005'][0]['name']
                        except (IndexError,KeyError):
                            Q4_species = ''
                        else:
                            Q4_species = pcq_results['11990005'][0]['name']
                    else:
                        Q4_species = pcq_results['11990005'][1]['code']


                    # Create the tree monitoring pcq method table
                    cur.execute('''INSERT INTO AKVO_Tree_monitoring_pcq (identifier_akvo, instance, lat_pcq_sample, lon_pcq_sample, height_pcq_sample, units_circom, Q1_dist, Q1_hgt, Q1_circom, Q1_spec, Q2_dist, Q2_hgt, Q2_circom, Q2_spec, Q3_dist, Q3_hgt, Q3_circom, Q3_spec, Q4_dist, Q4_hgt, Q4_circom, Q4_spec, pcq_location)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, instance_m, lat_sample_pcq, lon_sample_pcq, elev_sample_pcq, units_circom_pcq, Q1_distance, Q1_height, Q1_circom, Q1_species, Q2_distance, Q2_height, Q2_circom, Q2_species, Q3_distance, Q3_height, Q3_circom, Q3_species, Q4_distance, Q4_height, Q4_circom, Q4_species, pcq_location))

                    conn.commit()

        except KeyError:
            pcq_results = ''
