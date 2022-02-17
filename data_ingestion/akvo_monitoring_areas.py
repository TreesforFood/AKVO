import requests
import json
import sqlite3
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
import os

form_monitoring_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=11980001'
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

#connect to Postgresql database
#conn = psycopg2.connect(host= os.environ["HOST_PSTGRS"],database= os.environ["DATABASE_PSTGRS"],user= os.environ["USER_PSTGRS"],password= os.environ["PASSWORD_PSTGRS"])
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_monitoring_counts;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_pcq;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_photos;
DROP TABLE IF EXISTS AKVO_Tree_monitoring_areas;

CREATE TABLE AKVO_Tree_monitoring_areas (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER PRIMARY KEY, submission TEXT, submission_year SMALLINT, submitter TEXT, AKVO_form_version TEXT, site_impression TEXT, test TEXT, avg_tree_height REAL, number_living_trees INTEGER, method_selection TEXT, avg_circom_tree_count TEXT, avg_circom_tree_pcq TEXT);

CREATE TABLE AKVO_Tree_monitoring_counts (identifier_akvo TEXT, instance INTEGER, name_species TEXT, loc_name_spec TEXT, number_species INTEGER, avg_circom_tree NUMERIC(20,2), units_circom TEXT);

CREATE TABLE AKVO_Tree_monitoring_pcq (identifier_akvo TEXT, instance INTEGER, lat_pcq_sample REAL, lon_pcq_sample REAL, height_pcq_sample NUMERIC(20,2), units_circom TEXT, Q1_dist NUMERIC(20,2), Q1_hgt NUMERIC(20,2), Q1_circom NUMERIC(20,2), Q1_spec TEXT, Q2_dist NUMERIC(20,2), Q2_hgt NUMERIC(20,2), Q2_circom NUMERIC(20,2), Q2_spec TEXT, Q3_dist NUMERIC(20,2), Q3_hgt NUMERIC(20,2), Q3_circom NUMERIC(20,2), Q3_spec TEXT, Q4_dist NUMERIC(20,2), Q4_hgt NUMERIC(20,2), Q4_circom NUMERIC(20,2), Q4_spec TEXT, pcq_location geography(POINT, 4326));

CREATE TABLE AKVO_Tree_monitoring_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_location geography(POINT, 4326));

''')

def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]


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
            circom_indication_tree_pcq = None

        try:
            circom_indication_tree_count = level1_monitoring['responses']['50110001'][0]['176761123'][0]['text']
        except (IndexError,KeyError):
            circom_indication_tree_count = None

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
            create_Qlist = list()
            Q1 = '15920003'
            Q2 = '40300005'
            Q3 = '25860009'
            Q4 = '54040005'

            for pcq_results in level1_monitoring['responses']['39860004']:
                if (Q1 in pcq_results.keys() or Q2 in pcq_results.keys() or Q3 in pcq_results.keys() or Q4 in pcq_results.keys()):

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
