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

################################## GET FIRST DOWNLOAD URL FROM AKVO (REGISTRATION DATA)

initial_url_nursery_registration = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=38030003&form_id=30050006&page_size=200'
initial_url_nursery_monitoring = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=38030003&form_id=6070006&page_size=200'

################################## STORE FIRST DOWNLOAD URL IN DATABASE
#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

#cur.execute('''DROP TABLE temporary_url_download_table_nurseries;''')
#conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS temporary_url_download_table_nurseries(id SERIAL PRIMARY KEY, download_url TEXT);''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS test_number_identifiers_nurseries(identifier TEXT);''')
conn.commit()

################################## CHECK IF THE TABLE HAS DOWNLOAD URLS. IF THE TABLE IS EMPTY,
################################## IT MEANS THE DOWNLOAD HAS TO START FROM SCRATCH AND SO THE INITIAL DOWNLOAD URL IS ADDED.
################################## IF THE TABLE HAS AN URL, IT MEANS THAT THE FULL DOWNLOAD IS STILL IN PROGRES AND NEEDS TO CONTINUE FROM THE LATEST URL
cur.execute('''SELECT EXISTS (SELECT * FROM temporary_url_download_table_nurseries)''')
fetch_download_url = cur.fetchall()
if fetch_download_url[0][0] != True: # Output from this is Boolean! If the result is 'True' then the table is empty (no rows). In that case it will be populated with the initial URL download request
    cur.execute('''INSERT INTO temporary_url_download_table_nurseries(download_url) VALUES (%s)''', (initial_url_nursery_registration,))
    conn.commit()
    print('Message 1: The URL table does not have any rows with URLs. The script will start downloading with the initial AKVO download URL.')

    ################################## DOWNLOAD IS STARTED FROM SCRATCH, SO ALL TABLES ARE DROPPED AND REBUILD FIRST
    cur.execute('''
    DROP TABLE IF EXISTS AKVO_Nursery_registration_photos;
    DROP TABLE IF EXISTS AKVO_Nursery_registration;''')
    conn.commit()

    cur.execute('''CREATE TABLE AKVO_Nursery_registration (identifier_akvo TEXT, display_name TEXT, device_id TEXT,
    instance INTEGER, submission DATE, submission_year NUMERIC,
    submitter TEXT, country TEXT, test TEXT, organisation TEXT, nursery_type TEXT, nursery_name TEXT, newly_established TEXT,
    full_tree_capacity NUMERIC, lat_y REAL, lon_x REAL, elevation REAL, centroid_coord geography(POINT, 4326));

    CREATE TABLE AKVO_Nursery_registration_photos (identifier_akvo TEXT, instance NUMERIC, photo_url TEXT, centroid_coord geography(POINT, 4326));
    ''')
    conn.commit()

else:
    print('Message 2: The URL table has at least 1 row with a URL. The script will continue with the existing URLs in the table to download remaining data')

################################## OPEN THE LASTEST DOWNLOAD URL IN THE TABLE AS A FIRST CHECK.
cur.execute('''SELECT download_url FROM temporary_url_download_table_nurseries order by id DESC LIMIT 1;''')
fetch_latest_url = cur.fetchone() # Output is a tuple with only 1 URL value
fetch_latest_url = fetch_latest_url[0] # Get the URL value from the tuple
if fetch_latest_url == None:
    print('Message 3: In the json page there is no nextPageUrl available. Script has stopped')
    cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_nurseries_monitoring;''')
    end_message = cur.fetchone()[0]
    print('Message 4: A total of ', end_message, ' registration instances was harvested so far. Check if this is all data andf decide if you want to proceed downloading later or start download from beginning')
    message_delete_table = input('Message 5: Do you want start downloading from the beginning and delete the temporary_url_download_table_nurseries with all its URLs? Write yes or no: ')
    if (message_delete_table == 'yes'):
        cur.execute('''TRUNCATE TABLE temporary_url_download_table_nurseries''') # deletes all rows in the table but preserves the table
        conn.commit()
        print('All rows in table temporary_url_download_table_nurseries have been deleted')
    else:
        print('TABLE temporary_url_download_table_nurseries has been preserved')
else:

    count_identifiers = 0
    count_pages = 0

    for first_download_url in fetch_latest_url: # Parse through the nextPageUrl pages. Get the following nextPageUrl and store it in the table
        cur.execute('''SELECT download_url FROM temporary_url_download_table_nurseries order by id DESC LIMIT 1;''')
        first_download_url = cur.fetchone()[0]
        load_page = requests.get(first_download_url, headers=headers).content
        page_decode = load_page.decode() # decode from byte to string
        json_dict = json.loads(page_decode) # convert from string to json dictionary
        count_pages += 1
        print('Message 6: start harvesting from next url page: ', count_pages)

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
                lon_centr = level1['responses']['10050016'][0]['30140002']['long']
                print(lat_centr, lon_centr)
            except (KeyError, IndexError, ValueError):
                centroid_coord = None
            else:
                lat_centr = level1['responses']['10050016'][0]['30140002']['lat']
                lat_centr_conv = str(lat_centr)
                lon_centr = level1['responses']['10050016'][0]['30140002']['long']
                lon_centr_conv = str(lon_centr)
                centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'

            try:
                elevation = level1['responses']['10050016'][0]['30140002']['elev']
            except (KeyError, IndexError):
                elevation = None


            # Populate the tree registration table
            cur.execute('''INSERT INTO AKVO_Nursery_registration (identifier_akvo, display_name, device_id, instance, submission, submission_year,
            submitter, country, test, organisation, nursery_type, nursery_name, newly_established, full_tree_capacity, lat_y, lon_x, elevation, centroid_coord)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, displayname, deviceidentifier, instance, submissiondate_trunc, submissiondate_trunc_year, submitter, country, test, name_organisation,
            nursery_type, nursery_name, new_established, tree_production_full_cap, lat_centr, lon_centr, elevation, centroid_coord))

            try:
                level1['responses']['46400174']
            except KeyError:
                photo_location = None
                photo_url = None
            else:
                for photo in level1['responses']['46400174']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
                    photo_list = list(photo.values())
                    for photo in photo_list:
                        try:
                            photo_url = photo['filename']
                        except KeyError:
                            photo_url = None
                        else:
                            photo_url = photo['filename']
                            try: #print(photo_url) # print multiple rows well up to here with only urls
                                if photo['location'] is not None:
                                    photo_lat = photo['location']['latitude']
                                    photo_lon = photo['location']['longitude']
                                    photo_lat = str(photo_lat)
                                    photo_lon = str(photo_lon)
                                    photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                                else:
                                    photo_location = None

                            except:
                                photo_lat = None
                                photo_lon = None
                                photo_location = None


                            cur.execute('''INSERT INTO AKVO_Nursery_registration_photos (identifier_akvo, instance, photo_url, centroid_coord) VALUES (%s,%s,%s,%s)''', (identifier, instance, photo_url, photo_location))



        # WHEN ALL DATA FROM A PAGE IS PROCESSED SUCCESFULLY, THE nextPageUrl IS ADDED TO THE DATABASE.
        # IF INTERRUPTED (by an error), THE nextPageUrl IS NOT ADDED. THEN IN A NEXT RUN, THE PREVIOUS nextPageUrl WILL BE USED AS A START.
        get_nextPageUrl = json_dict.get('nextPageUrl', None) # get the nextPageUrl from the latest page
        if get_nextPageUrl == None:
            print('Message 7: No nextPageUrl was found. Either all data was harvested or a page was empty. The script ended')
            cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_nurseries;''')
            end_message_unique = cur.fetchone()[0]
            cur.execute('''SELECT COUNT(*) FROM test_number_identifiers_nurseries;''')
            end_message_total = cur.fetchone()[0]
            print('Message 8: A total of ', end_message_unique, ' UNIQUE registration instances was harvested so far within a total of ', end_message_total, ' instances. Check if this is all data')
            message_delete_table = input('Message 9: Do you want to delete the temporary_url_download_table_nurseries? Write yes or no: ')
            if message_delete_table == 'yes':
                cur.execute('''DROP TABLE temporary_url_download_table_nurseries''')
                conn.commit()
                cur.execute('''DROP TABLE test_number_identifiers_nurseries''')
                conn.commit()
                break
            elif message_delete_table == 'no':
                break
            else:
                break

        else:
            cur.execute('''INSERT INTO temporary_url_download_table_nurseries(download_url) VALUES (%s)''', (get_nextPageUrl,))
            conn.commit()

########################################################################################################################################
# MONITORING STARTS HERE

cur.execute('''CREATE TABLE IF NOT EXISTS temporary_url_download_table_nurseries_monitoring(id SERIAL PRIMARY KEY, download_url TEXT);''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS test_number_identifiers_nurseries_monitoring(identifier TEXT);''')
conn.commit()

################################## CHECK IF THE TABLE HAS DOWNLOAD URLS. IF THE TABLE IS EMPTY,
################################## IT MEANS THE DOWNLOAD HAS TO START FROM SCRATCH AND SO THE INITIAL DOWNLOAD URL IS ADDED.
################################## IF THE TABLE HAS AN URL, IT MEANS THAT THE FULL DOWNLOAD IS STILL IN PROGRES AND NEEDS TO CONTINUE FROM THE LATEST URL
cur.execute('''SELECT EXISTS (SELECT * FROM temporary_url_download_table_nurseries_monitoring)''')
fetch_download_url = cur.fetchall()
if fetch_download_url[0][0] != True: # Output from this is Boolean! If the result is 'True' then the table is empty (no rows). In that case it will be populated with the initial URL download request
    cur.execute('''INSERT INTO temporary_url_download_table_nurseries_monitoring(download_url) VALUES (%s)''', (initial_url_nursery_monitoring,))
    conn.commit()
    print('Message 1: The URL table does not have any rows with URLs. The script will start downloading with the initial AKVO download URL.')

    ################################## DOWNLOAD IS STARTED FROM SCRATCH, SO ALL TABLES ARE DROPPED AND REBUILD FIRST
    cur.execute('''
    DROP TABLE IF EXISTS AKVO_Nursery_monitoring_photos;
    DROP TABLE IF EXISTS AKVO_Nursery_monitoring_tree_species;
    DROP TABLE IF EXISTS AKVO_Nursery_monitoring;''')
    conn.commit()

    cur.execute('''CREATE TABLE AKVO_Nursery_monitoring (identifier_akvo TEXT, instance INTEGER, submission_date DATE, submission_time TEXT,
    submitter TEXT, name_nursery_manager TEXT, test TEXT, gender_nursery_manager TEXT, challenges_nursery TEXT,
    number_trees_produced_currently NUMERIC, month_planting_stock TEXT, nr_working_personel NUMERIC);

    CREATE TABLE AKVO_Nursery_monitoring_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, centroid_coord geography(POINT, 4326));

    CREATE TABLE AKVO_Nursery_monitoring_tree_species (identifier_akvo TEXT, instance NUMERIC, tree_species_latin TEXT,
    tree_species_local TEXT);
    ''')
    conn.commit()

else:
    print('Message 2: The URL table has at least 1 row with a URL. The script will continue with the existing URLs in the table to download remaining data')

################################## OPEN THE LASTEST DOWNLOAD URL IN THE TABLE AS A FIRST CHECK.
cur.execute('''SELECT download_url FROM temporary_url_download_table_nurseries_monitoring order by id DESC LIMIT 1;''')
fetch_latest_url = cur.fetchone() # Output is a tuple with only 1 URL value
fetch_latest_url = fetch_latest_url[0] # Get the URL value from the tuple
if fetch_latest_url == None:
    print('Message 3: In the json page there is no nextPageUrl available. Script has stopped')
    cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_nurseries_monitoring;''')
    end_message = cur.fetchone()[0]
    print('Message 4: A total of ', end_message, ' registration instances was harvested so far. Check if this is all data andf decide if you want to proceed downloading later or start download from beginning')
    message_delete_table = input('Message 5: Do you want start downloading from the beginning and delete the temporary_url_download_table_nurseries_monitoring with all its URLs? Write yes or no: ')
    if (message_delete_table == 'yes'):
        cur.execute('''TRUNCATE TABLE temporary_url_download_table_nurseries_monitoring''') # deletes all rows in the table but preserves the table
        conn.commit()
        print('All rows in table temporary_url_download_table_nurseries_monitoring have been deleted')
    else:
        print('TABLE temporary_url_download_table_nurseries_monitoring has been preserved')
else:

    count_identifiers = 0
    count_pages = 0

    for first_download_url in fetch_latest_url: # Parse through the nextPageUrl pages. Get the following nextPageUrl and store it in the table
        cur.execute('''SELECT download_url FROM temporary_url_download_table_nurseries_monitoring order by id DESC LIMIT 1;''')
        first_download_url = cur.fetchone()[0]
        load_page = requests.get(first_download_url, headers=headers).content
        page_decode = load_page.decode() # decode from byte to string
        json_dict = json.loads(page_decode) # convert from string to json dictionary
        count_pages += 1
        print('Message 6: start harvesting from next url page: ', count_pages)

        # Get all the nursery monitoring data
        for level1_m in json_dict['formInstances']:
            identifier = level1_m['identifier']
            displayname = level1_m['displayName']
            deviceidentifier = level1_m['deviceIdentifier']
            instance = level1_m['id']

            submission_date = level1_m['submissionDate']

            try:
                submission_time = mid(submissiondate, 11,19)
            except (KeyError, IndexError):
                submission_time = ''

            submitter = level1_m['submitter']

            try:
                name_nursery_manager = level1_m['responses']['28140007'][0]['24170016']
            except (KeyError, IndexError):
                name_nursery_manager = ''

            try:
                test = level1_m['responses']['28140007'][0]['33540002'][0].get('text')
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
                print_month_planting_stock = ''
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
                        try:
                            photo_url_m = photo_m['filename']
                        except KeyError:
                            photo_url_m = None
                        else:
                            photo_url_m = photo_m['filename']

                            try: #print(photo_url) # print multiple rows well up to here with only urls
                                if photo_m['location'] is not None:
                                    photo_lat_m = photo_m['location']['latitude']
                                    photo_lon_m = photo_m['location']['longitude']
                                    photo_lat_m = str(photo_lat_m)
                                    photo_lon_m = str(photo_lon_m)
                                    photo_location_m = 'POINT('+ photo_lon_m + ' ' + photo_lat_m + ')'

                                else:
                                    photo_location_m = None

                            except:
                                photo_lat_m = None
                                photo_lon_m = None
                                photo_location_m = None


                            cur.execute('''INSERT INTO AKVO_Nursery_monitoring_photos (identifier_akvo, instance, photo_url, centroid_coord)
                            VALUES (%s,%s,%s,%s)''', (identifier, instance, photo_url_m, photo_location_m))

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


        # WHEN ALL DATA FROM A PAGE IS PROCESSED SUCCESFULLY, THE nextPageUrl IS ADDED TO THE DATABASE.
        # IF INTERRUPTED (by an error), THE nextPageUrl IS NOT ADDED. THEN IN A NEXT RUN, THE PREVIOUS nextPageUrl WILL BE USED AS A START.
        get_nextPageUrl = json_dict.get('nextPageUrl', None) # get the nextPageUrl from the latest page
        if get_nextPageUrl == None:
            print('Message 7: No nextPageUrl was found. Either all data was harvested or a page was empty. The script ended')
            cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_nurseries_monitoring;''')
            end_message_unique = cur.fetchone()[0]
            cur.execute('''SELECT COUNT(*) FROM test_number_identifiers_nurseries_monitoring;''')
            end_message_total = cur.fetchone()[0]
            print('Message 8: A total of ', end_message_unique, ' UNIQUE registration instances was harvested so far within a total of ', end_message_total, ' instances. Check if this is all data')
            message_delete_table = input('Message 9: Do you want to delete the temporary_url_download_table_nurseries_monitoring? Write yes or no: ')
            if message_delete_table == 'yes':
                cur.execute('''DROP TABLE temporary_url_download_table_nurseries_monitoring''')
                conn.commit()
                cur.execute('''DROP TABLE test_number_identifiers_nurseries_monitoring''')
                conn.commit()
                sys.exit()
            elif message_delete_table == 'no':
                sys.exit()
            else:
                sys.exit()

        else:
            cur.execute('''INSERT INTO temporary_url_download_table_nurseries_monitoring(download_url) VALUES (%s)''', (get_nextPageUrl,))
            conn.commit()
