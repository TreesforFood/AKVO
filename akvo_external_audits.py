import requests
import json
import re
import geojson
import psycopg2
import geodaisy.converters as convert
from area import area
import os


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
conn = psycopg2.connect(host= os.environ["HOST_PSTGRS"],database= os.environ["DATABASE_PSTGRS"],user= os.environ["USER_PSTGRS"],password= os.environ["PASSWORD_PSTGRS"])

cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_external_audits_areas;
DROP TABLE IF EXISTS AKVO_Tree_external_audits_pcq;

CREATE TABLE AKVO_Tree_external_audits_areas (identifier_akvo TEXT, instance INTEGER, submitter TEXT, test TEXT, farmer_reported_tree_nr NUMERIC, farmer_reported_nr_tree_species NUMERIC, farmer_reported_died_trees NUMERIC, audit_reported_trees NUMERIC, option_tree_count TEXT, manual_tree_count NUMERIC, display_name TEXT, impression_site TEXT, calc_area REAL, polygon geography(POLYGON, 4326));

CREATE TABLE AKVO_Tree_external_audits_pcq (identifier_akvo TEXT, instance INTEGER, lat_pcq_sample REAL, lon_pcq_sample REAL, pcq_location geography(POINT, 4326), height_pcq_sample TEXT, Q1_dist REAL, Q1_hgt REAL, Q1_spec TEXT, Q2_dist REAL, Q2_hgt REAL, Q2_spec TEXT, Q3_dist REAL, Q3_hgt REAL, Q3_spec TEXT, Q4_dist REAL, Q4_hgt REAL, Q4_spec TEXT);

''')


for all_data in url_list:
    load_page = requests.get(all_data, headers=headers).content
    page_decode = load_page.decode()
    json_dict = json.loads(page_decode)
    #print(json_dict)

    # Get all the tree registration data
    for level1_exaudits in json_dict['formInstances']:
        identifier = level1_exaudits['identifier']
        instance = level1_exaudits['id']
        submitter = level1_exaudits['submitter']
        display_name = level1_exaudits['displayName']

        try:
            impression_site = level1_exaudits['responses']['145310001'][0]['160020001']
        except (KeyError, IndexError):
            impression_site = 'No comment'

        try:
            test = level1_exaudits['responses']['145310001'][0]['135610001'][0]['text']
        except (KeyError, IndexError):
            test = ''

        try:
            farmer_reported_tree_nr = level1_exaudits['responses']['145310001'][0]['219340289']
            farmer_reported_tree_nr = int(farmer_reported_tree_nr)
        except (KeyError, IndexError):
            farmer_reported_tree_nr = None

        try:
            farmer_reported_tree_species = level1_exaudits['responses']['145310001'][0]['236450229']
            farmer_reported_tree_species = int(farmer_reported_tree_species)
        except (KeyError, IndexError):
            farmer_reported_tree_species = None

        try:
            farmer_reported_died_trees = level1_exaudits['responses']['145310001'][0]['217360306']
            farmer_reported_died_trees = int(farmer_reported_died_trees)
        except (KeyError, IndexError):
            farmer_reported_died_trees = None

        try:
            audit_reported_trees = level1_exaudits['207590361'][0]['229630303']
            audit_reported_trees = int(audit_reported_trees)
        except (KeyError, IndexError):
            audit_reported_trees = None

        try:
            option_tree_count = level1_exaudits['responses']['145310001'][0]['217360308'][0]['text']
        except (KeyError, IndexError):
            option_tree_count = ''

        try:
            manual_tree_count = level1_exaudits['responses']['207590361'][0]['229630303']
            manual_tree_count = int(manual_tree_count)
        except (KeyError, IndexError):
            manual_tree_count = None



        #living trees_found = level1_exaudits['identifier']229630303

        try:
            geom_get = level1_exaudits['responses']['145680002'][0]['223780044'] # Up to this level it can go wrong (due to empty entry)
            if geom_get != None:
                geom_get = level1_exaudits['responses']['145680002'][0]['223780044']['features'][0].get('geometry','')
                area_ha = area(geom_get)
                area_ha = round((area_ha/10000),3)
                geometry = convert.geojson_to_wkt(geom_get)
                get_geom_type = geometry.split(' ',1)
                if get_geom_type[0] == 'POLYGON':
                    polygon = convert.geojson_to_wkt(geom_get)
                    coord = re.findall('\s', polygon)
                    number_coord_pol = int((len(coord)/2)-1)
                    multipoint = None

                elif get_geom_type[0] == 'MULTIPOINT':
                    multipoint = convert.geojson_to_wkt(geom_get)
                    polygon = None

            else:
                geom_get = None

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
        #cur.execute('''INSERT INTO Tree_external_audits_areas (identifier_akvo, instance, submitter, test, farmer_reported_tree_nr, farmer_reported_nr_tree_species, farmer_reported_died_trees, audit_reported_trees, option_tree_count, manual_tree_count, display_name, impression_site, calc_area, polygon)
        #VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, submitter, test, farmer_reported_tree_nr, farmer_reported_tree_species, farmer_reported_died_trees, audit_reported_trees, option_tree_count, manual_tree_count, display_name, impression_site, area_ha, polygon))


        #cur.execute('''INSERT INTO Tree_external_audits_areas(polygon) VALUES (ST_GeomFromText(%s))''', (polygon,))

        cur.execute('''INSERT INTO AKVO_Tree_external_audits_areas (identifier_akvo, instance, submitter, test, farmer_reported_tree_nr, farmer_reported_nr_tree_species, farmer_reported_died_trees, audit_reported_trees, option_tree_count, manual_tree_count, display_name, impression_site, calc_area, polygon)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, submitter, test, farmer_reported_tree_nr, farmer_reported_tree_species, farmer_reported_died_trees, audit_reported_trees, option_tree_count, manual_tree_count, display_name, impression_site, area_ha, polygon))

        conn.commit()

        # Part of the Group 2 sheet questions: PCQ method
        try:
            create_Qlist = list()
            Q1 = '156360017'
            Q2 = '158160026'
            Q3 = '137590011'
            Q4 = '154610120'

            for pcq_results in level1_exaudits['responses']['158130010']:
                if (Q1 in pcq_results.keys() or Q2 in pcq_results.keys() or Q3 in pcq_results.keys() or Q4 in pcq_results.keys()):

                    try:
                        lat_sample_pcq = pcq_results['158130011']['lat']
                    except (IndexError,KeyError):
                        lat_sample_pcq = None
                    try:
                        lon_sample_pcq = pcq_results['158130011']['long']
                    except (IndexError,KeyError):
                        lon_sample_pcq = None

                    if lat_sample_pcq is not None:
                        lat_sample_pcq_str = str(lat_sample_pcq)
                    else:
                        lat_sample_pcq_str = ''

                    if lon_sample_pcq is not None:
                        lon_sample_pcq_str = str(lon_sample_pcq)
                    else:
                        lon_sample_pcq_str = ''

                    if not lat_sample_pcq_str or not lon_sample_pcq_str:
                        pcq_location = None
                    else:
                        pcq_location = 'POINT('+ lon_sample_pcq_str + ' ' + lat_sample_pcq_str + ')'

                    try:
                        elev_sample_pcq = pcq_results['158130011']['elev']
                    except (IndexError,KeyError):
                        elev_sample_pcq = None
                    try:
                        Q1_distance = pcq_results['156360017']
                    except (IndexError,KeyError):
                        Q1_distance = None
                    try:
                        Q1_height = pcq_results['137590009']
                    except (IndexError,KeyError):
                        Q1_height = None

                    try:
                        pcq_results['160020007'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['160020007'][0]['name']
                        except (IndexError,KeyError):
                            Q1_species = ''
                        else:
                            Q1_species = pcq_results['160020007'][0]['name']
                    else:
                        Q1_species = pcq_results['160020007'][1]['code']

                    try:
                        Q2_distance = pcq_results['158160026']
                    except (IndexError,KeyError):
                        Q2_distance = None
                    try:
                        Q2_height = pcq_results['115380039']
                    except (IndexError,KeyError):
                        Q2_height = None

                    try:
                        pcq_results['119150011'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['119150011'][0]['name']
                        except (IndexError,KeyError):
                            Q2_species = ''
                        else:
                            Q2_species = pcq_results['119150011'][0]['name']
                    else:
                        Q2_species = pcq_results['119150011'][1]['code']

                    try:
                        Q3_distance = pcq_results['137590011']
                    except (IndexError,KeyError):
                        Q3_distance = None
                    try:
                        Q3_height = pcq_results['119150013']
                    except (IndexError,KeyError):
                        Q3_height = None

                    try:
                        pcq_results['156350009'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['156350009'][0]['name']
                        except (IndexError,KeyError):
                            Q3_species = ''
                        else:
                            Q3_species = pcq_results['156350009'][0]['name']
                    else:
                        Q3_species = pcq_results['156350009'][1]['code']

                    try:
                        Q4_distance = pcq_results['154610120']
                    except (IndexError,KeyError):
                        Q4_distance = None
                    try:
                        Q4_height = pcq_results['137630011']
                    except (IndexError,KeyError):
                        Q4_height = None

                    try:
                        pcq_results['137630012'][1]['code']
                    except (IndexError,KeyError):
                        try:
                            pcq_results['137630012'][0]['name']
                        except (IndexError,KeyError):
                            Q4_species = ''
                        else:
                            Q4_species = pcq_results['137630012'][0]['name']
                    else:
                        Q4_species = pcq_results['137630012'][1]['code']


                    # Create the tree monitoring pcq method table
                    cur.execute('''INSERT INTO AKVO_Tree_external_audits_pcq (identifier_akvo, instance, lat_pcq_sample, lon_pcq_sample, pcq_location, height_pcq_sample, Q1_dist, Q1_hgt, Q1_spec, Q2_dist, Q2_hgt, Q2_spec, Q3_dist, Q3_hgt, Q3_spec, Q4_dist, Q4_hgt, Q4_spec)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, lat_sample_pcq, lon_sample_pcq, pcq_location, elev_sample_pcq, Q1_distance, Q1_height, Q1_species, Q2_distance, Q2_height, Q2_species, Q3_distance, Q3_height, Q3_species, Q4_distance, Q4_height, Q4_species))

                    conn.commit()

        except KeyError:
            pcq_results = ''
