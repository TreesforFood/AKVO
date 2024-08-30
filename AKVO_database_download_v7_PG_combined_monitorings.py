import requests
import json
import psycopg2
import re
import geojson
import geodaisy.converters as convert
from area import area
import os
from osgeo import ogr
import sys

def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]

count_pages_registration_data = 0

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


################################## GET FIRST DOWNLOAD URL FROM AKVO (MONITORING DATA)

initial_url_monitoring_data = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=11980001&page_size=200'

################################## STORE FIRST DOWNLOAD URL IN DATABASE
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS temporary_url_download_table_monitorings(id SERIAL PRIMARY KEY, download_url TEXT);''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS test_number_identifiers_monitorings(identifier TEXT);''')
conn.commit()

################################## CHECK IF THE TABLE HAS DOWNLOAD URLS. IF THE TABLE IS EMPTY,
################################## IT MEANS THE DOWNLOAD HAS TO START FROM SCRATCH AND SO THE INITIAL DOWNLOAD URL IS ADDED.
################################## IF THE TABLE HAS AN URL, IT MEANS THAT THE FULL DOWNLOAD IS STILL IN PROGRES AND NEEDS TO CONTINUE FROM THE LATEST URL
cur.execute('''SELECT EXISTS (SELECT * FROM temporary_url_download_table_monitorings)''')
fetch_download_url = cur.fetchall()
if fetch_download_url[0][0] != True: # Output from this is Boolean! If the result is 'True' then the table is empty (no rows). In that case it will be populated with the initial URL download request
    cur.execute('''INSERT INTO temporary_url_download_table_monitorings(download_url) VALUES (%s)''', (initial_url_monitoring_data,))
    conn.commit()
    print('Message 1: The URL table does not have any rows with URLs. The script will start downloading with the initial AKVO download URL.')

    ################################## DOWNLOAD IS STARTED FROM SCRATCH, SO ALL TABLES ARE DROPPED AND REBUILD FIRST
    cur.execute('''
    DROP TABLE IF EXISTS AKVO_Tree_monitoring_counts;
    DROP TABLE IF EXISTS AKVO_Tree_monitoring_pcq;
    DROP TABLE IF EXISTS AKVO_Tree_monitoring_photos;
    DROP TABLE IF EXISTS AKVO_Tree_monitoring_areas;
    DROP TABLE IF EXISTS AKVO_Tree_monitoring_remapped_areas;''')

    conn.commit()


    cur.execute('''CREATE TABLE AKVO_Tree_monitoring_areas (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER, submission DATE, submission_year SMALLINT, submitter TEXT, AKVO_form_version TEXT, location_monitoring geography(POINT, 4326), site_impression TEXT, test TEXT, avg_tree_height REAL, number_living_trees INTEGER, method_selection TEXT, avg_circom_tree_count TEXT, avg_circom_tree_pcq TEXT);

    CREATE TABLE AKVO_Tree_monitoring_remapped_areas (identifier_akvo TEXT, instance INTEGER, submission DATE, submitter TEXT, polygon_remapped geography(POLYGON, 4326), calc_area_remapped NUMERIC(20,3), number_coord_polygon_remapped INTEGER);

    CREATE TABLE AKVO_Tree_monitoring_counts (identifier_akvo TEXT, instance INTEGER, name_species TEXT, loc_name_spec TEXT, number_species INTEGER, avg_circom_tree NUMERIC(20,2), units_circom TEXT);

    CREATE TABLE AKVO_Tree_monitoring_pcq (identifier_akvo TEXT, instance INTEGER, lat_pcq_sample REAL, lon_pcq_sample REAL, height_pcq_sample NUMERIC(20,2), units_circom TEXT, Q1_dist NUMERIC(20,2), Q1_hgt NUMERIC(20,2), Q1_circom NUMERIC(20,2), Q1_spec TEXT, Q2_dist NUMERIC(20,2), Q2_hgt NUMERIC(20,2), Q2_circom NUMERIC(20,2), Q2_spec TEXT, Q3_dist NUMERIC(20,2), Q3_hgt NUMERIC(20,2), Q3_circom NUMERIC(20,2), Q3_spec TEXT, Q4_dist NUMERIC(20,2), Q4_hgt NUMERIC(20,2), Q4_circom NUMERIC(20,2), Q4_spec TEXT, pcq_location geography(POINT, 4326));

    CREATE TABLE AKVO_Tree_monitoring_photos (identifier_akvo TEXT, instance INTEGER, photo_source TEXT, photo_url TEXT, photo_location geography(POINT, 4326));

    ''')

    conn.commit()

else:
    print('Message 2: The URL table has at least 1 row with a URL. The script will continue with the existing URLs in the table to download remaining data')

################################## OPEN THE LASTEST DOWNLOAD URL IN THE TABLE AS A FIRST CHECK.
cur.execute('''SELECT download_url FROM temporary_url_download_table_monitorings order by id DESC LIMIT 1;''')
fetch_latest_url = cur.fetchone() # Output is a tuple with only 1 URL value
fetch_latest_url = fetch_latest_url[0] # Get the URL value from the tuple
if fetch_latest_url == None:
    print('Message 3: In the json page there is no nextPageUrl available. Script has stopped')
    cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_monitorings;''')
    end_message = cur.fetchone()[0]
    print('Message 4: A total of ', end_message, ' registration instances was harvested so far. Check if this is all data andf decide if you want to proceed downloading later or start download from beginning')
    message_delete_table = input('Message 5: Do you want start downloading from the beginning and delete the temporary_url_download_table_monitorings with all its URLs? Write yes or no: ')
    if (message_delete_table == 'yes'):
        cur.execute('''TRUNCATE TABLE temporary_url_download_table_monitorings''') # deletes all rows in the table but preserves the table
        conn.commit()
        print('All rows in table temporary_url_download_table_monitorings have been deleted')
    else:
        print('TABLE temporary_url_download_table_monitorings has been preserved')
else:

    count_identifiers = 0
    count_pages = 0

    for first_download_url in fetch_latest_url: # Parse through the nextPageUrl pages. Get the following nextPageUrl and store it in the table
        cur.execute('''SELECT download_url FROM temporary_url_download_table_monitorings order by id DESC LIMIT 1;''')
        first_download_url = cur.fetchone()[0]
        load_page = requests.get(first_download_url, headers=headers).content
        page_decode = load_page.decode() # decode from byte to string
        json_dict = json.loads(page_decode) # convert from string to json dictionary
        count_pages += 1
        #print('Message 6: start harvesting from next url page: ', count_pages)
        #print('INPUT JSONDICT:', json_dict)


        for level1_monitoring in json_dict['formInstances']:
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
                testing_m = level1_monitoring['responses']['50110001'][0]['42120002'][0]['text']
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

            try:
                polygon_remapped = level1_monitoring['responses']['243590074'][0]['252970039']['features'][0].get('geometry',None) # Up to this level it can go wrong (due to empty entry)
                if polygon_remapped != None:
                    polygon_remapped = level1_monitoring['responses']['243590074'][0]['252970039']['features'][0].get('geometry')
                    area_ha = area(polygon_remapped)
                    area_ha_remapped = round((area_ha/10000),3)
                    geometry_remapped = convert.geojson_to_wkt(polygon_remapped)
                    get_geom_type_remapped = geometry_remapped.split(' ',1)
                    if get_geom_type_remapped[0] == 'POLYGON':
                        polygon_check = convert.geojson_to_wkt(polygon_remapped)
                        coord = re.findall('\s', polygon_check)
                        number_coord_pol_remapped = int((len(coord)/2)-1)
                        if number_coord_pol_remapped < 3:
                            polygon_remapped = None
                            area_ha_remapped = None
                            number_coord_pol_remapped = None
                        else:
                            polygon_remapped = polygon_check
                            multipoint = None

                else:
                    polygon_remapped = None
                    area_ha_remapped = None
                    number_coord_pol_remapped = None

            except (IndexError, KeyError):
                polygon_remapped = None
                number_coord_pol_remapped = None
                area_ha_remapped = None

            try:
                monitoring_lat = level1_monitoring['responses']['50110001'][0]['234680047']['lat']
                monitoring_long = level1_monitoring['responses']['50110001'][0]['234680047']['long']
            except (IndexError,KeyError):
                monitoring_lat = None
                monitoring_long = None
            else:
                monitoring_lat = level1_monitoring['responses']['50110001'][0]['234680047']['lat']
                monitoring_long = level1_monitoring['responses']['50110001'][0]['234680047']['long']

            if monitoring_lat is None or monitoring_long is None:
                centroid_coord_monitoring = None
            elif monitoring_lat == 'None' or monitoring_long == 'None':
                centroid_coord_monitoring = None
            else:
                monitoring_lat_conv = str(monitoring_lat)
                monitoring_lon_conv = str(monitoring_long)
                centroid_coord_monitoring = 'POINT (' + monitoring_lon_conv +' '+ monitoring_lat_conv +')'

            # Create the tree monitoring raw table. Remapped polygons are excluded from this table and stored in a seperate table called "tree_monitoring_remapped_areas"
            cur.execute('''INSERT INTO AKVO_Tree_monitoring_areas (identifier_akvo, display_name, device_id, instance, submission, submission_year, submitter, AKVO_form_version, location_monitoring, site_impression, test, avg_tree_height, number_living_trees, method_selection, avg_circom_tree_count, avg_circom_tree_pcq)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, displayname_m, device_id_m, instance_m, submissiondate_trunc_m, submission_year_m, submitter_m, formversion_m, centroid_coord_monitoring, impression_site, testing_m, avg_tree_height, tot_nr_trees_estimate, select_method, circom_indication_tree_count, circom_indication_tree_pcq))


            # Create the "tree_monitoring_remapped_areas" table with remapped polygons. It is seperated from the "tree monitoring raw table" to prevent the "tree monitoring raw table" appears as a geometry table in QGIS.
            cur.execute('''INSERT INTO AKVO_Tree_monitoring_remapped_areas (identifier_akvo, instance, submission, submitter, polygon_remapped, calc_area_remapped, number_coord_polygon_remapped)
            VALUES (%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, instance_m, submissiondate_trunc_m, submitter_m, polygon_remapped, area_ha_remapped, number_coord_pol_remapped))


            #get the 4 first photos in N, S, E, W direction from monitoring
            try:
                level1_monitoring['responses']['50110007']
            except KeyError:
                pass
            else:
                for y in level1_monitoring['responses']['50110007']:
                    for k,v in y.items():
                        if isinstance(v, dict):
                            try:
                                photo_m4_url = v['filename']
                            except KeyError:
                                continue
                            else:
                                photo_m4_url = v['filename']
                                photo_source_m4 = '4 photos location'
                        else:
                            continue

                        if isinstance(v, dict):
                            try:
                                photo_m4_location = v['location']
                            except KeyError:
                                photo_m4_location = None
                            else:
                                if v['location'] is not None:
                                    photo_lat = photo_m4_location['latitude']
                                    photo_lon = photo_m4_location['longitude']
                                    photo_lat = str(photo_lat)
                                    photo_lon = str(photo_lon)
                                    photo_m4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                                else:
                                    photo_m4_location = None

                        cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_source, photo_url, photo_location)
                        VALUES (%s,%s,%s,%s,%s)''', (identifier_m, instance_m, photo_source_m4, photo_m4_url, photo_m4_location))

            # get the 36 other photos from the monitoring form
            try:
                level1_monitoring['responses']['40300009']
            except KeyError:
                pass
            else:
                for y in level1_monitoring['responses']['40300009']:
                    for k,v in y.items():
                        if isinstance(v, dict):
                            try:
                                photo_m36_url = v['filename']
                            except KeyError:
                                continue
                            else:
                                photo_m36_url = v['filename']
                                photo_source_m36 = '36 photos location'
                        else:
                            continue

                        if isinstance(v, dict):
                            try:
                                photo_m36_location = v['location']
                            except KeyError:
                                photo_m36_location = None
                            else:
                                if v['location'] is not None:
                                    photo_lat = photo_m36_location['latitude']
                                    photo_lon = photo_m36_location['longitude']
                                    photo_lat = str(photo_lat)
                                    photo_lon = str(photo_lon)
                                    photo_m36_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                                else:
                                    photo_m36_location = None


                        cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_source, photo_url, photo_location)
                        VALUES (%s,%s,%s,%s,%s)''', (identifier_m, instance_m, photo_source_m36, photo_m36_url, photo_m36_location))

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


            except (IndexError,KeyError):
                tree_species_count = ''
                tree_number_count = ''
                avg_tree_circom_count = ''
                tree_loc_species_count = ''


            # Part of the Group 2 sheet questions: PCQ method
            try:
                level1_monitoring['responses']['39860004']
            except KeyError:
                pass
            else:
                for pcq_results in level1_monitoring['responses']['39860004']:
                    try:
                        lat_sample_pcq = pcq_results['54050004']['lat']
                    except (IndexError,KeyError):
                        lat_sample_pcq = None
                        lat_sample_pcq_str = None
                    else:
                        lat_sample_pcq = pcq_results['54050004']['lat']
                        lat_sample_pcq_str = str(lat_sample_pcq)

                    try:
                        lon_sample_pcq = pcq_results['54050004']['long']
                    except (IndexError,KeyError):
                        lon_sample_pcq = None
                        lon_sample_pcq_str = None
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

                    for k, v in pcq_results.items(): # Retrieve the key, value pairs for each dictionary

                        if isinstance(v, dict):
                            try:
                                url_photo_pcq = v['filename']
                            except KeyError:
                                continue
                            else:
                                url_photo_pcq = v['filename']
                                photo_source_pcq = 'pcq photos'
                        else:
                            continue

                        cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_source, photo_url, photo_location)
                        VALUES (%s,%s,%s,%s,%s)''', (identifier_m, instance_m, photo_source_pcq, url_photo_pcq, pcq_location))



        # WHEN ALL DATA FROM A PAGE IS PROCESSED SUCCESFULLY, THE nextPageUrl IS ADDED TO THE DATABASE.
        # IF INTERRUPTED (by an error), THE nextPageUrl IS NOT ADDED. THEN IN A NEXT RUN, THE PREVIOUS nextPageUrl WILL BE USED AS A START.
        get_nextPageUrl = json_dict.get('nextPageUrl', None) # get the nextPageUrl from the latest page
        if get_nextPageUrl == None:
            print('Message 7: No nextPageUrl was found. Either all data was harvested or a page was empty. The script ended')
            cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_monitorings;''')
            end_message_unique = cur.fetchone()[0]
            cur.execute('''SELECT COUNT(*) FROM test_number_identifiers_monitorings;''')
            end_message_total = cur.fetchone()[0]
            print('Message 8: A total of ', end_message_unique, ' UNIQUE registration instances was harvested so far within a total of ', end_message_total, ' instances. Check if this is all data')
            message_delete_table = input('Message 9: Do you want to delete the temporary_url_download_table_monitorings? Write yes or no: ')

            if message_delete_table == 'yes':
                cur.execute('''TRUNCATE TABLE temporary_url_download_table_monitorings''')
                conn.commit()
                cur.execute('''TRUNCATE TABLE test_number_identifiers_monitorings''')
                conn.commit()
                sys.exit()
            elif message_delete_table == 'no':
                sys.exit()
            else:
                sys.exit()

        else:
            cur.execute('''INSERT INTO temporary_url_download_table_monitorings(download_url) VALUES (%s)''', (get_nextPageUrl,))
            conn.commit()
