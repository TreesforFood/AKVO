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

########################################################################################################################################
# TREE REGISTRATION DATA DOWNLOAD STARTS HERE

initial_url_registration_data = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=48090001&page_size=200'
initial_sync_request = 'https://api-auth0.akvo.org/flow/orgs/ecosia/sync?initial=true' # with this link, you get the first page with the NextUrl in it. This URL is being send by an api call.


################################## STORE FIRST DOWNLOAD URL IN DATABASE
#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS temporary_url_download_table (id SERIAL PRIMARY KEY, download_url TEXT);''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS test_number_identifiers(identifier TEXT);''')
conn.commit()

################################## CHECK IF THE TABLE HAS DOWNLOAD URLS. IF THE TABLE IS EMPTY,
################################## IT MEANS THE DOWNLOAD HAS TO START FROM SCRATCH AND SO THE INITIAL DOWNLOAD URL IS ADDED.
################################## IF THE TABLE HAS AN URL, IT MEANS THAT THE FULL DOWNLOAD IS STILL IN PROGRES AND NEEDS TO CONTINUE FROM THE LATEST URL
cur.execute('''SELECT EXISTS (SELECT * FROM temporary_url_download_table)''')
fetch_download_url = cur.fetchall()
if fetch_download_url[0][0] != True: # Output from this is Boolean! If the result is 'True' then the table is empty (no rows). In that case it will be populated with the initial URL download request
    cur.execute('''INSERT INTO temporary_url_download_table(download_url) VALUES (%s)''', (initial_url_registration_data,))
    conn.commit()
    get_initial_syncurl = requests.get(initial_sync_request, headers = headers)
    converturltotxt = get_initial_syncurl.text
    converttojson = json.loads(converturltotxt)
    initial_syncurl = converttojson.get('nextSyncUrl',"Ecosia: No sync url was found in this instance")
    cur.execute('''UPDATE url_latest_sync SET sync_url = %s WHERE id = %s;''', (initial_syncurl, 1))
    conn.commit()
    print('Message 1: The URL table does not have any rows with URLs. The script will start downloading with the initial AKVO download URL. Also the following syncURl has been stored in the database for syncing: ', initial_syncurl)

    ################################## DOWNLOAD IS STARTED FROM SCRATCH, SO ALL TABLES ARE DROPPED AND REBUILD FIRST
    cur.execute('''
    DROP TABLE IF EXISTS AKVO_Tree_registration_species;
    DROP TABLE IF EXISTS AKVO_Tree_registration_photos;
    DROP TABLE IF EXISTS AKVO_Tree_registration_areas;
    DROP TABLE IF EXISTS AKVO_Tree_registration_areas_baseline_counts;
    DROP TABLE IF EXISTS AKVO_Tree_registration_areas_baseline_pcq;''')

    conn.commit()

    cur.execute('''CREATE TABLE AKVO_Tree_registration_areas (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER, submission DATE,
    submission_year SMALLINT, submissiontime TEXT, submitter TEXT, modifiedAt TEXT, AKVO_form_version NUMERIC(10,2), country TEXT, test TEXT,
    organisation TEXT, contract_number NUMERIC(20,2), id_planting_site TEXT, land_title TEXT, name_village TEXT, name_region TEXT, name_owner TEXT,
    photo_owner TEXT, gender_owner TEXT, objective_site TEXT, site_preparation TEXT, planting_technique TEXT, planting_system TEXT, remark TEXT,
    nr_trees_option TEXT, planting_date TEXT, tree_number INTEGER, estimated_area NUMERIC(20,3), calc_area NUMERIC(20,3), lat_y REAL, lon_x REAL,
    number_coord_polygon INTEGER, centroid_coord geography(POINT, 4326), polygon geography(POLYGON, 4326), multipoint geography(MULTIPOINT, 4326));

    CREATE TABLE AKVO_Tree_registration_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_geotag_location geography(POINT, 4326), photo_gps_location geography(POINT, 4326));

    CREATE TABLE AKVO_Tree_registration_species (identifier_akvo TEXT, instance INTEGER, lat_name_species TEXT, local_name_species TEXT, number_species INTEGER);

    CREATE TABLE AKVO_Tree_registration_areas_baseline_counts (identifier_akvo TEXT, instance INTEGER, number_mature_trees INTEGER, avg_diameter_dbh NUMERIC(20,3), avg_height NUMERIC(20,3), example_photo TEXT);

    CREATE TABLE AKVO_Tree_registration_areas_baseline_pcq (identifier_akvo TEXT, instance INTEGER, lat_pcq_sample REAL, lon_pcq_sample REAL, height_pcq_sample NUMERIC(20,2), units_circom TEXT, Q1_dist NUMERIC(20,2), Q1_hgt NUMERIC(20,2), Q1_circom NUMERIC(20,2), Q1_spec TEXT, Q2_dist NUMERIC(20,2), Q2_hgt NUMERIC(20,2), Q2_circom NUMERIC(20,2), Q2_spec TEXT, Q3_dist NUMERIC(20,2), Q3_hgt NUMERIC(20,2), Q3_circom NUMERIC(20,2), Q3_spec TEXT, Q4_dist NUMERIC(20,2), Q4_hgt NUMERIC(20,2), Q4_circom NUMERIC(20,2), Q4_spec TEXT, pcq_location geography(POINT, 4326));

    ''')

    conn.commit()

else:
    print('Message 2: The URL table has at least 1 row with a URL. The script will continue with the existing URLs in the table to download remaining data')

################################## OPEN THE LASTEST DOWNLOAD URL IN THE TABLE AS A FIRST CHECK.
cur.execute('''SELECT download_url FROM temporary_url_download_table order by id DESC LIMIT 1;''')
fetch_latest_url = cur.fetchone() # Output is a tuple with only 1 URL value
fetch_latest_url = fetch_latest_url[0] # Get the URL value from the tuple
print('Latest NextURL used as input :', fetch_latest_url)
if fetch_latest_url == None:
    print('Message 3: In the json page there is no nextPageUrl available. Script has stopped')
    cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers;''')
    end_message = cur.fetchone()[0]
    print('Message 4: A total of ', end_message, ' registration instances was harvested so far. Check if this is all data andf decide if you want to proceed downloading later or start download from beginning')
    message_delete_table = input('Message 5: Do you want start downloading from the beginning and delete the temporary_url_download_table with all its URLs? Write yes or no: ')
    if (message_delete_table == 'yes'):
        cur.execute('''TRUNCATE TABLE temporary_url_download_table''') # deletes all rows in the table but preserves the table
        conn.commit()
        print('All rows in table temporary_url_download_table have been deleted')
    else:
        print('TABLE temporary_url_download_table has been preserved')
else:

    count_identifiers = 0
    count_pages = 0

    for first_download_url in fetch_latest_url: # Parse through the nextPageUrl pages. Get the following nextPageUrl and store it in the table
        cur.execute('''SELECT download_url FROM temporary_url_download_table order by id DESC LIMIT 1;''')
        first_download_url = cur.fetchone()[0]
        load_page = requests.get(first_download_url, headers=headers).content
        page_decode = load_page.decode() # decode from byte to string
        json_dict = json.loads(page_decode) # convert from string to json dictionary
        count_pages += 1
        print('Message x6: start harvesting from next url page: ', count_pages)
        print(json_dict)

        for level1 in json_dict['formInstances']:
            count_identifiers += 1
            print('nr identifiers downloaded so far: ', count_identifiers, ': ', level1['identifier'])
            modifiedat = level1['modifiedAt']
            formversion = level1['formVersion']
            identifier = level1['identifier']
            cur.execute('''INSERT INTO TEST_NUMBER_IDENTIFIERS(identifier) VALUES (%s)''',(identifier,))
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

            try:
                #contract_number = level1['responses']['1960001'][0].get('52070068', 0.0)
                contract_number = level1['responses']['1960001'][0]['52070068']
            except KeyError:
                contract_number = None

            try:
                id_planting_site = level1['responses']['1960001'][0]['58000002']
            except KeyError:
                id_planting_site = ''

            try:
                land_title = level1['responses']['1960001'][0]['52070069'][0]['text']
            except (KeyError, IndexError): # Since landtitle has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
                land_title = ''

            try:
                name_village = level1['responses']['1960001'][0]['61910570']
            except (KeyError, IndexError): # Since landtitle has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
                name_village = ''

            try:
                name_region = level1['responses']['1960001'][0]['44110002']
            except (KeyError, IndexError): # Since landtitle has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
                name_region = ''

            try:
                name_owner = level1['responses']['1960001'][0]['54050003']
            except (KeyError, IndexError): # Since landtitle has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
                name_owner = ''

            try:
                photo_owner = level1['responses']['1960001'][0]['31840003']['filename']
            except (KeyError, IndexError):
                photo_owner = ''

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
                        polygon_z = ogr.CreateGeometryFromWkt(polygon_check)
                        polygon_z.FlattenTo2D()
                        polygon_z.ExportToWkt()
                        polygon_z = str(polygon_z.ExportToWkt())
                        coord = re.findall('\s', polygon_check)
                        number_coord_pol = int((len(coord)/2)-1)
                        if number_coord_pol < 3:
                            polygon = None
                            multipoint = None
                        else:
                            polygon = polygon_z
                            multipoint = None

                    elif get_geom_type[0] == 'MULTIPOINT':
                        multipoint = convert.geojson_to_wkt(geom_get)
                        polygon = None

                else:
                    geom_get = None
                    area_ha = None
                    multipoint = None
                    polygon = None

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
            cur.execute('''INSERT INTO AKVO_Tree_registration_areas (identifier_akvo, display_name, device_id, instance, submission, submission_year, submissiontime, submitter, modifiedAt, AKVO_form_version, country, test, organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, photo_owner, gender_owner, objective_site, site_preparation, planting_technique, planting_system, remark, nr_trees_option, planting_date, tree_number, estimated_area, calc_area, lat_y, lon_x, number_coord_polygon, centroid_coord, polygon, multipoint)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, displayname, deviceidentifier, instance, submissiondate_trunc, submissiondate_trunc_year, submissiontime, submitter, modifiedat, formversion, country, test, name_organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, photo_owner, gender_owner, landuse_objective, site_preparation, planting_technique, planting_system, remark, more_less_200_trees, planting_date_trunc, nr_trees_planted, estimated_area, area_ha, lat_centr, lon_centr, number_coord_pol, centroid_coord, polygon, multipoint))

            #conn.commit()

            try:
                level1['responses']['1960007']
                #print('Check photo urls: ', level1)

            except KeyError:
                photo_r4_location = None
                photo_r4_url = None
                photo_r4_gps_location = None
            else:
                for photo in level1['responses']['1960007']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
                    photo.pop('5900011', None)
                    photo.pop('50110008', None)
                    photo.pop('25860015', None)

                    for photo_value in photo.values():
                        photo_items4=[]
                        if photo_value is not None:
                            photo_items4.append(photo_value)

                        for url4 in photo_items4:

                            if url4['filename'] is not None:
                                photo_r4_url = url4['filename']
                                try:
                                    url4['location']
                                except KeyError:
                                    photo_r4_location = None
                                    photo_r4_gps_location = None

                                else:
                                    if url4['location'] is not None:
                                        photo_lat1 = url4['location']['latitude']
                                        photo_lon1 = url4['location']['longitude']
                                        photo_lat = str(photo_lat1)
                                        photo_lon = str(photo_lon1)
                                        photo_r4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                                        photo_r4_gps_location = None
                                    else:
                                        photo_r4_location = None
                                        photo_r4_gps_location = None

                            else:
                                photo_r4_url = None
                                photo_r4_location = None
                                photo_r4_gps_location = None

                            #print('photo:',photo_url, photo_location, identifier, count) Prints well multiple photos and id's up to here.
                            cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_geotag_location, photo_gps_location)
                            VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, photo_r4_url, photo_r4_location, photo_r4_gps_location))

                            #conn.commit()

            try:
                for photo in level1['responses']['3990009']: # Get other 36 photos from registration form. This loop was tested in file: AKVO_database_download_v7_test_rest_36_reg_photos.py
                    photo_items36 = list(photo.values())

                    for url36 in photo_items36:
                        try:
                            photo_r36_url = url36['filename']
                        except KeyError:
                            photo_r36_url = None
                            photo_r36_location = None
                            photo_r36_gps_location = None
                        else:
                            photo_r36_url = url36['filename']
                            try:
                                if url36['location'] is not None:
                                    photo_lat = url36['location']['latitude']
                                    photo_lat = str(photo_lat)
                                    photo_lon = url36['location']['longitude']
                                    photo_lon = str(photo_lon)
                                    photo_r36_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                                    photo_r36_gps_location = None

                                else:
                                    photo_lat = None
                                    photo_lon = None
                                    photo_r36_location = None
                                    photo_r36_gps_location = None

                            except KeyError:
                                photo_lat = None
                                photo_lon = None
                                photo_r36_location = None
                                photo_r36_gps_location = None

                        cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_geotag_location, photo_gps_location)
                        VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, photo_r36_url, photo_r36_location, photo_r36_gps_location))

                        #conn.commit()


            except (IndexError,KeyError):
                photo = None
                photo_r36_url = None
                photo_r36_location = None

            # Include photos from the GPS points. These questions were introduced from the form version 130 onwards
            try:
                x = level1['responses']['282760413']
                #print('Check photo urls: ', x)

            except (KeyError):
                photo_repeat_url = ''
                photo_repeat_gps_point = ''

            else:

                photo_list_gps = []

                for photo_gps in level1['responses'].get('282760413'):
                    photo_list_gps.clear()

                    try:
                        note_answer = photo_gps['293111364']
                    except(KeyError):
                        note_answer = None
                    try:
                        photo_repeat_url_get1 = photo_gps['277761190']['filename']
                        photo_list_gps.append(photo_repeat_url_get1)
                    except(KeyError):
                        photo_repeat_url_get1 = None
                    try:
                        photo_repeat_url_get2 = photo_gps['287121276']['filename']
                        photo_list_gps.append(photo_repeat_url_get2)
                    except(KeyError):
                        photo_repeat_url_get2 = None
                    try:
                        photo_repeat_url_get3 = photo_gps['290900474']['filename']
                        photo_list_gps.append(photo_repeat_url_get3)
                    except(KeyError):
                        photo_repeat_url_get3 = None
                    try:
                        photo_repeat_url_get4 = photo_gps['304590496']['filename']
                        photo_list_gps.append(photo_repeat_url_get4)
                    except(KeyError):
                        photo_repeat_url_get4 = None
                    try:
                        gps_lon = photo_gps['308430745']['long']
                    except(KeyError):
                        gps_lon = None
                    try:
                        gps_lat = photo_gps['308430745']['lat']
                    except(KeyError):
                        gps_lat = None

                    if gps_lon is not None and gps_lat is not None:
                        photo_repeat_gps_point_y = str(gps_lat)
                        photo_repeat_gps_point_x = str(gps_lon)
                        photo_gps_location = 'POINT('+ photo_repeat_gps_point_x + ' ' + photo_repeat_gps_point_y + ')'
                    else:
                        photo_gps_location = None

                    for populate_photos_gps in photo_list_gps:
                        #print(populate_photos_gps, photo_repeat_gps_point)
                        photo_repeat_url = populate_photos_gps
                        photo_geotag_location = None

                        cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_geotag_location, photo_gps_location)
                        VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, photo_repeat_url, photo_geotag_location, photo_gps_location))

                        #conn.commit()


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

                    #conn.commit()

            # Create table for the PCQ data from the baseline measurement
            try:
                level1['responses']['264220236']
            except KeyError:
                pass

            else:
                for pcq_results_baseline in level1['responses']['264220236']:
                    try:
                        lat_sample_pcq = pcq_results_baseline['264250152']['lat']
                    except (IndexError,KeyError):
                        lat_sample_pcq = None
                        lat_sample_pcq_str = None
                    else:
                        lat_sample_pcq = pcq_results_baseline['264250152']['lat']
                        lat_sample_pcq_str = str(lat_sample_pcq)

                    try:
                        lon_sample_pcq = pcq_results_baseline['264250152']['long']
                    except (IndexError,KeyError):
                        lon_sample_pcq = None
                        lon_sample_pcq_str = None
                    else:
                        lon_sample_pcq = pcq_results_baseline['264250152']['long']
                        lon_sample_pcq_str = str(lon_sample_pcq)

                    if not lat_sample_pcq_str or not lon_sample_pcq_str:
                        pcq_location = None
                    else:
                        pcq_location = 'POINT('+ lon_sample_pcq_str + ' ' + lat_sample_pcq_str + ')'

                    try:
                        Q1_distance = pcq_results_baseline['243640004']
                    except (IndexError,KeyError):
                        Q1_distance = None
                    try:
                        Q1_height = pcq_results_baseline['264220326']
                    except (IndexError,KeyError):
                        Q1_height = None
                    try:
                        Q1_circom = pcq_results_baseline['266050247']
                    except (IndexError,KeyError):
                        Q1_circom = None

                    try:
                        Q1_species = pcq_results_baseline['262700255']
                    except (IndexError,KeyError):
                        Q1_species = ''

                    try:
                        Q2_distance = pcq_results_baseline['248370163']
                    except (IndexError,KeyError):
                        Q2_distance = None
                    try:
                        Q2_height = pcq_results_baseline['254340003']
                    except (IndexError,KeyError):
                        Q2_height = None
                    try:
                        Q2_circom = pcq_results_baseline['250980255']
                    except (IndexError,KeyError):
                        Q2_circom = None

                    try:
                        Q2_species = pcq_results_baseline['205600055']
                    except (IndexError,KeyError):
                        Q2_species = ''

                    try:
                        Q3_distance = pcq_results_baseline['251000184']
                    except (IndexError,KeyError):
                        Q3_distance = None
                    try:
                        Q3_height = pcq_results_baseline['243630282']
                    except (IndexError,KeyError):
                        Q3_height = None
                    try:
                        Q3_circom = pcq_results_baseline['247430289']
                    except (IndexError,KeyError):
                        Q3_circom = None

                    try:
                        Q3_species = pcq_results_baseline['262710003']
                    except (IndexError,KeyError):
                        Q3_species = ''

                    try:
                        Q4_distance = pcq_results_baseline['264220238']
                    except (IndexError,KeyError):
                        Q4_distance = None
                    try:
                        Q4_height = pcq_results_baseline['245670203']
                    except (IndexError,KeyError):
                        Q4_height = None
                    try:
                        Q4_circom = pcq_results_baseline['232710250']
                    except (IndexError,KeyError):
                        Q4_circom = None

                    try:
                        Q4_species = pcq_results_baseline['256740291']
                    except (IndexError,KeyError):
                        Q4_species = ''


                    cur.execute('''INSERT INTO AKVO_Tree_registration_areas_baseline_pcq (identifier_akvo, instance, lat_pcq_sample, lon_pcq_sample, Q1_dist, Q1_hgt, Q1_circom, Q1_spec, Q2_dist, Q2_hgt, Q2_circom, Q2_spec, Q3_dist, Q3_hgt, Q3_circom, Q3_spec, Q4_dist, Q4_hgt, Q4_circom, Q4_spec, pcq_location)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, lat_sample_pcq, lon_sample_pcq, Q1_distance, Q1_height, Q1_circom, Q1_species, Q2_distance, Q2_height, Q2_circom, Q2_species, Q3_distance, Q3_height, Q3_circom, Q3_species, Q4_distance, Q4_height, Q4_circom, Q4_species, pcq_location))

                    #conn.commit()

            # Create table for the COUNT data from the baseline measurement
            try:
                level1['responses']['215991355']
            except KeyError:
                pass

            else:
                for x in level1['responses']['215991355']:

                    try:
                        number_mature_trees = x.get('264811956')
                    except (IndexError,KeyError):
                        number_mature_trees = None

                    try:
                        avg_diameter_dbh = x.get('264851349')
                    except (IndexError,KeyError):
                        avg_diameter_dbh = None

                    try:
                        avg_height = x.get('260610793')
                    except (IndexError,KeyError):
                        avg_height = None

                    try:
                        example_photo = x['253251438']['filename']
                    except (IndexError,KeyError):
                        example_photo = None


                    cur.execute('''INSERT INTO AKVO_Tree_registration_areas_baseline_counts (identifier_akvo, instance, number_mature_trees,
                    avg_diameter_dbh, avg_height, example_photo)
                    VALUES (%s,%s,%s,%s,%s,%s)''', (identifier, instance, number_mature_trees,
                    avg_diameter_dbh, avg_height, example_photo))

                    #conn.commit()

        # WHEN ALL DATA FROM A PAGE IS PROCESSED SUCCESFULLY, THE nextPageUrl IS ADDED TO THE DATABASE.
        # IF INTERRUPTED (by an error), THE nextPageUrl IS NOT ADDED. THEN IN A NEXT RUN, THE PREVIOUS nextPageUrl WILL BE USED AS A START.
        get_nextPageUrl = json_dict.get('nextPageUrl', None) # get the nextPageUrl from the latest page
        if get_nextPageUrl == None:
            print('Message 7: No nextPageUrl was found. Either all data was harvested or a page was empty. The script ended')
            cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers;''')
            end_message_unique = cur.fetchone()[0]
            cur.execute('''SELECT COUNT(*) FROM test_number_identifiers;''')
            end_message_total = cur.fetchone()[0]
            print('Message 8: A total of ', end_message_unique, ' UNIQUE registration instances was harvested so far within a total of ', end_message_total, ' instances. Check if this is all data')

            cur.execute('''DROP TABLE temporary_url_download_table''')
            conn.commit()
            cur.execute('''DROP TABLE test_number_identifiers''')
            conn.commit()
            sys.exit()


        else:
            cur.execute('''INSERT INTO temporary_url_download_table(download_url) VALUES (%s)''', (get_nextPageUrl,))
            conn.commit()
