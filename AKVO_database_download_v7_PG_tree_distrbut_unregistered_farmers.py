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

initial_url_tree_distribution_unregfar = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=215246811&form_id=252327848&page_size=250'
initial_url_tree_registration_unregfar = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=215246811&form_id=258280042&page_size=250'

################################## STORE FIRST DOWNLOAD URL IN DATABASE
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS temporary_url_table_tree_distribution_unregfar(id SERIAL PRIMARY KEY, download_url TEXT);''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS test_number_identifiers_tree_distribution_unregfar(identifier TEXT);''')
conn.commit()

################################## CHECK IF THE TABLE HAS DOWNLOAD URLS. IF THE TABLE IS EMPTY,
################################## IT MEANS THE DOWNLOAD HAS TO START FROM SCRATCH AND SO THE INITIAL DOWNLOAD URL IS ADDED.
################################## IF THE TABLE HAS AN URL, IT MEANS THAT THE FULL DOWNLOAD IS STILL IN PROGRES AND NEEDS TO CONTINUE FROM THE LATEST URL
cur.execute('''SELECT EXISTS (SELECT * FROM temporary_url_table_tree_distribution_unregfar)''')
fetch_download_url = cur.fetchall()
if fetch_download_url[0][0] != True: # Output from this is Boolean! If the result is 'True' then the table is empty (no rows). In that case it will be populated with the initial URL download request
    cur.execute('''INSERT INTO temporary_url_table_tree_distribution_unregfar(download_url) VALUES (%s)''', (initial_url_tree_distribution_unregfar,))
    conn.commit()
    print('Message 1: The URL table does not have any rows with URLs. The script will start downloading with the initial AKVO download URL.')

    cur.execute('''
    DROP TABLE IF EXISTS AKVO_Tree_distribution_unregistered_farmers;
    DROP TABLE IF EXISTS AKVO_Tree_distribution_unregistered_tree_species;''')

    conn.commit()

    cur.execute('''CREATE TABLE AKVO_tree_distribution_unregistered_farmers (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER,
    submission DATE, submitter TEXT, AKVO_form_version NUMERIC(10,2), country TEXT, test TEXT, organisation TEXT, contract_number NUMERIC(20,2),
    name_tree_receiver TEXT, gender_tree_receiver TEXT, check_ownership_trees TEXT, check_ownership_land TEXT, url_photo_receiver_trees TEXT,
    url_photo_id_card_tree_receiver TEXT, location_house_tree_receiver TEXT, name_site_id_tree_planting TEXT, confirm_planting_location TEXT,
    total_number_trees_received INTEGER, url_signature_tree_receiver TEXT);

    CREATE TABLE AKVO_Tree_distribution_unregistered_tree_species (identifier_akvo TEXT, display_name TEXT,
    device_id TEXT, instance INTEGER, species_lat TEXT, species_local TEXT, number_species INTEGER);''')

    conn.commit()

else:
    print('Message 2: The URL table has at least 1 row with a URL. The script will continue with the existing URLs in the table to download remaining data')

################################## OPEN THE LASTEST DOWNLOAD URL IN THE TABLE AS A FIRST CHECK.
cur.execute('''SELECT download_url FROM temporary_url_table_tree_distribution_unregfar order by id DESC LIMIT 1;''')
fetch_latest_url = cur.fetchone() # Output is a tuple with only 1 URL value
fetch_latest_url = fetch_latest_url[0] # Get the URL value from the tuple
if fetch_latest_url == None:
    print('Message 3: In the json page there is no nextPageUrl available. Script has stopped')
    cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_tree_distribution_unregfar;''')
    end_message = cur.fetchone()[0]
    print('Message 4: A total of ', end_message, ' registration instances was harvested so far. Check if this is all data andf decide if you want to proceed downloading later or start download from beginning')
    message_delete_table = input('Message 5: Do you want start downloading from the beginning and delete the temporary_url_table_tree_distribution_unregfar with all its URLs? Write yes or no: ')
    if (message_delete_table == 'yes'):
        cur.execute('''TRUNCATE TABLE temporary_url_table_tree_distribution_unregfar''') # deletes all rows in the table but preserves the table
        conn.commit()
        print('All rows in table temporary_url_table_tree_distribution_unregfar have been deleted')
    else:
        print('TABLE temporary_url_table_tree_distribution_unregfar has been preserved')
else:

    count_identifiers = 0
    count_pages = 0

    for first_download_url in fetch_latest_url: # Parse through the nextPageUrl pages. Get the following nextPageUrl and store it in the table
        cur.execute('''SELECT download_url FROM temporary_url_table_tree_distribution_unregfar order by id DESC LIMIT 1;''')
        first_download_url = cur.fetchone()[0]
        load_page = requests.get(first_download_url, headers=headers).content
        page_decode = load_page.decode() # decode from byte to string
        json_dict = json.loads(page_decode) # convert from string to json dictionary
        count_pages += 1
        print('Message 6: start harvesting from next url page: ', count_pages)


        for registration_distributed_trees in json_dict['formInstances']:
            #print(registration_light)
            try:
                identifier_akvo = registration_distributed_trees['identifier']
            except (IndexError,KeyError):
                identifier_akvo = None

            try:
                displayname = registration_distributed_trees['displayName']
            except (IndexError,KeyError):
                displayname = None

            try:
                device_id = registration_distributed_trees['deviceIdentifier']
            except (IndexError,KeyError):
                device_id = None

            try:
                instance = registration_distributed_trees['id']
            except (IndexError,KeyError):
                instance = None

            try:
                submitter = registration_distributed_trees['submitter']
            except (IndexError,KeyError):
                submitter = None

            try:
                submission_date = registration_distributed_trees['submissionDate']
            except (IndexError,KeyError):
                submission_date = None

            try:
                form_version = registration_distributed_trees['formVersion']
            except (IndexError,KeyError):
                form_version = None


            try:
                country = registration_distributed_trees['responses']['241552098'][0]['248221840'][0].get('name')
                print(country)

            except (IndexError,KeyError):
                country = None

            try:
                organisation = registration_distributed_trees['responses']['241552098'][0]['248221840'][1].get('name')
                print(organisation)
            except (IndexError,KeyError):
                organisation = None

            try:
                contract_number = registration_distributed_trees['responses']['241552098'][0].get('211873508')
                print(contract_number)
            except (IndexError,KeyError):
                contract_number = None

            try:
                test = registration_distributed_trees['responses']['241552098'][0]['315384624'][0].get('text')
                print(test)
            except (IndexError,KeyError):
                test = None

            try:
                name_tree_receiver = registration_distributed_trees['responses']['241552098'][0].get('239313615')
                print(name_tree_receiver)
            except (IndexError,KeyError):
                name_tree_receiver = None

            try:
                gender_tree_receiver = registration_distributed_trees['responses']['241552098'][0]['266122082'][0].get('text')
                print(gender_tree_receiver)
            except (IndexError,KeyError):
                gender_tree_receiver = None

            try:
                check_ownership_trees = registration_distributed_trees['responses']['241552098'][0]['262782006'][0].get('text')
                print(check_ownership_trees)
            except (IndexError,KeyError):
                check_ownership_trees = None

            try:
                check_ownership_land = registration_distributed_trees['responses']['241552098'][0]['258620407'][0].get('text')
                print(check_ownership_land)
            except (IndexError,KeyError):
                check_ownership_land = None

            try:
                photo_receiver_trees = registration_distributed_trees['responses']['241552098'][0]['244788272'].get('filename')
                print(photo_receiver_trees)
            except (IndexError,KeyError):
                photo_receiver_trees = None

            try:
                photo_id_card_tree_receiver = registration_distributed_trees['responses']['241552098'][0]['213993970'].get('filename')
                print(photo_id_card_tree_receiver)
            except (IndexError,KeyError):
                photo_id_card_tree_receiver = None

            try:
                location_house_tree_receiver = registration_distributed_trees['responses']['241552098'][0].get('231082082')
                print(location_house_tree_receiver)
            except (IndexError,KeyError):
                location_house_tree_receiver = None

            try:
                name_site_id_tree_planting = registration_distributed_trees['responses']['241552098'][0].get('278476531')
                print(name_site_id_tree_planting)
            except (IndexError,KeyError):
                name_site_id_tree_planting = None

            try:
                confirm_planting_location = registration_distributed_trees['responses']['241552098'][0]['258620407'][0].get('text')
                print(confirm_planting_location)
            except (IndexError,KeyError):
                confirm_planting_location = None

            try:
                total_number_trees_received = registration_distributed_trees['responses']['237550277'][0]['231056596']
                print(total_number_trees_received)
            except (IndexError,KeyError):
                total_number_trees_received = None

            try:
                signature_tree_receiver = registration_distributed_trees['responses']['237550277'][0]['52318270'].get('filename')
            except (IndexError,KeyError):
                signature_tree_receiver = None

            cur.execute('''INSERT INTO AKVO_Tree_distribution_unregistered_farmers (identifier_akvo, display_name, device_id, instance,
            submission, submitter, AKVO_form_version, country, test, organisation, contract_number,
            name_tree_receiver, gender_tree_receiver, check_ownership_trees, check_ownership_land, url_photo_receiver_trees,
            url_photo_id_card_tree_receiver, location_house_tree_receiver, name_site_id_tree_planting, confirm_planting_location,
            total_number_trees_received, url_signature_tree_receiver)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (identifier_akvo, displayname, device_id, instance, submission_date, submitter, form_version, country,
            test, organisation, contract_number, name_tree_receiver, gender_tree_receiver,
            check_ownership_trees, check_ownership_land, photo_receiver_trees, photo_id_card_tree_receiver, location_house_tree_receiver,
            name_site_id_tree_planting, confirm_planting_location, total_number_trees_received, signature_tree_receiver))

            try:
                registration_distributed_trees['responses']['242845264']
            except (IndexError,KeyError):
                pass
            else:
                for species in registration_distributed_trees['responses']['242845264']:
                    print('Species :', species)
                    try:
                        species['258032656']
                    except KeyError:
                        number_species = None
                    else:
                        number_species = species['258032656']

                    try:
                        species['211873502'][1].get('name')
                    except (IndexError,KeyError):
                        name_species_local = None
                    else:
                        name_species_local = species['211873502'][1].get('name')

                    try:
                        species['211873502'][1].get('code')
                    except (IndexError,KeyError):
                        name_species_latin = None
                    else:
                        name_species_latin = species['211873502'][1].get('code')


                    cur.execute('''INSERT INTO AKVO_Tree_distribution_unregistered_tree_species (identifier_akvo, display_name,
                    device_id, instance, species_lat, species_local, number_species)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                    (identifier_akvo, displayname, device_id, instance, name_species_latin, name_species_local, number_species))


        # WHEN ALL DATA FROM A PAGE IS PROCESSED SUCCESFULLY, THE nextPageUrl IS ADDED TO THE DATABASE.
        # IF INTERRUPTED (by an error), THE nextPageUrl IS NOT ADDED. THEN IN A NEXT RUN, THE PREVIOUS nextPageUrl WILL BE USED AS A START.
        get_nextPageUrl = json_dict.get('nextPageUrl', None) # get the nextPageUrl from the latest page
        if get_nextPageUrl == None:
            print('Message 7: No nextPageUrl was found. Either all data was harvested or a page was empty. The script ended')
            cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_tree_distribution_unregfar;''')
            end_message_unique = cur.fetchone()[0]
            cur.execute('''SELECT COUNT(*) FROM test_number_identifiers_tree_distribution_unregfar;''')
            end_message_total = cur.fetchone()[0]
            print('Message 8: A total of ', end_message_unique, ' UNIQUE registration instances was harvested so far within a total of ', end_message_total, ' instances. Check if this is all data')
            message_delete_table = input('Message 9: Do you want to delete the temporary_url_table_tree_distribution_unregfar? Write yes or no: ')
            if message_delete_table == 'yes':
                cur.execute('''DROP TABLE temporary_url_table_tree_distribution_unregfar''')
                conn.commit()
                cur.execute('''DROP TABLE test_number_identifiers_tree_distribution_unregfar''')
                conn.commit()
                break
            elif message_delete_table == 'no':
                break
            else:
                break

        else:
            cur.execute('''INSERT INTO temporary_url_table_tree_distribution_unregfar(download_url) VALUES (%s)''', (get_nextPageUrl,))
            conn.commit()

########################################################################################################################################
# REGISTRATION STARTS HERE

cur.execute('''CREATE TABLE IF NOT EXISTS temporary_url_table_tree_registration_unregfar(id SERIAL PRIMARY KEY, download_url TEXT);''')
conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS test_number_identifiers_tree_registration_unregfar(identifier TEXT);''')
conn.commit()

################################## CHECK IF THE TABLE HAS DOWNLOAD URLS. IF THE TABLE IS EMPTY,
################################## IT MEANS THE DOWNLOAD HAS TO START FROM SCRATCH AND SO THE INITIAL DOWNLOAD URL IS ADDED.
################################## IF THE TABLE HAS AN URL, IT MEANS THAT THE FULL DOWNLOAD IS STILL IN PROGRES AND NEEDS TO CONTINUE FROM THE LATEST URL
cur.execute('''SELECT EXISTS (SELECT * FROM temporary_url_table_tree_registration_unregfar)''')
fetch_download_url = cur.fetchall()
if fetch_download_url[0][0] != True: # Output from this is Boolean! If the result is 'True' then the table is empty (no rows). In that case it will be populated with the initial URL download request
    cur.execute('''INSERT INTO temporary_url_table_tree_registration_unregfar(download_url) VALUES (%s)''', (initial_url_tree_registration_unregfar,))
    conn.commit()
    print('Message 1: The URL table does not have any rows with URLs. The script will start downloading with the initial AKVO download URL.')

    cur.execute('''
    DROP TABLE IF EXISTS AKVO_site_registration_distributed_trees;
    DROP TABLE IF EXISTS AKVO_site_registration_distributed_trees_photos;
    DROP TABLE IF EXISTS AKVO_site_registration_distributed_trees_species;''')

    conn.commit()

    cur.execute('''CREATE TABLE AKVO_site_registration_distributed_trees (identifier_akvo TEXT, displayname TEXT, device_id TEXT,
    instance INTEGER, submitter TEXT, submission_date DATE, form_version NUMERIC(20,3), test TEXT,
    name_region_village_planting_site TEXT, name_owner_planting_site TEXT, gender_owner_planting_site TEXT,
    photo_owner_planting_site TEXT, nr_trees_received INTEGER, confirm_plant_location_own_land TEXT,
    one_multiple_planting_sites TEXT, nr_trees_given_away INTEGER, more_less_200_trees TEXT, date_tree_planting TEXT,
    centroid_coord geography(POINT, 4326),
    polygon geography(POLYGON, 4326), number_coord_pol INTEGER, area_ha NUMERIC(20,3), avg_tree_distance NUMERIC(20,3), estimated_area NUMERIC(20,3), unit_estimated_area TEXT,
    estimated_tree_number_planted INTEGER, confirm_additional_photos TEXT, comment_enumerator TEXT);

    CREATE TABLE AKVO_site_registration_distributed_trees_photos (identifier_akvo TEXT, displayname TEXT, device_id TEXT,
    instance INTEGER, photo_url_4 TEXT, photo_location geography(POINT, 4326));

    CREATE TABLE AKVO_site_registration_distributed_trees_species (identifier_akvo TEXT, displayname TEXT, device_id TEXT,
    instance INTEGER, species_lat TEXT, species_local TEXT);''')

    conn.commit()

else:
    print('Message 2: The URL table has at least 1 row with a URL. The script will continue with the existing URLs in the table to download remaining data')

################################## OPEN THE LASTEST DOWNLOAD URL IN THE TABLE AS A FIRST CHECK.
cur.execute('''SELECT download_url FROM temporary_url_table_tree_registration_unregfar order by id DESC LIMIT 1;''')
fetch_latest_url = cur.fetchone() # Output is a tuple with only 1 URL value
fetch_latest_url = fetch_latest_url[0] # Get the URL value from the tuple
if fetch_latest_url == None:
    print('Message 3: In the json page there is no nextPageUrl available. Script has stopped')
    cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_tree_registration_unregfar;''')
    end_message = cur.fetchone()[0]
    print('Message 4: A total of ', end_message, ' registration instances was harvested so far. Check if this is all data andf decide if you want to proceed downloading later or start download from beginning')
    message_delete_table = input('Message 5: Do you want start downloading from the beginning and delete the temporary_url_table_tree_registration_unregfar with all its URLs? Write yes or no: ')
    if (message_delete_table == 'yes'):
        cur.execute('''TRUNCATE TABLE temporary_url_table_tree_registration_unregfar''') # deletes all rows in the table but preserves the table
        conn.commit()
        print('All rows in table temporary_url_table_tree_registration_unregfar have been deleted')
    else:
        print('TABLE temporary_url_table_tree_registration_unregfar has been preserved')
else:

    count_identifiers = 0
    count_pages = 0

    for first_download_url in fetch_latest_url: # Parse through the nextPageUrl pages. Get the following nextPageUrl and store it in the table
        cur.execute('''SELECT download_url FROM temporary_url_table_tree_registration_unregfar order by id DESC LIMIT 1;''')
        first_download_url = cur.fetchone()[0]
        load_page = requests.get(first_download_url, headers=headers).content
        page_decode = load_page.decode() # decode from byte to string
        json_dict = json.loads(page_decode) # convert from string to json dictionary
        count_pages += 1
        print('Message 6: start harvesting from next url page: ', count_pages)

        for site_registration_distributed_trees in json_dict['formInstances']:

            #print(registration_light)
            try:
                identifier_akvo = site_registration_distributed_trees['identifier']
            except (IndexError,KeyError):
                identifier_akvo = None

            try:
                displayname = site_registration_distributed_trees['displayName']
            except (IndexError,KeyError):
                displayname = None

            try:
                device_id = site_registration_distributed_trees['deviceIdentifier']
            except (IndexError,KeyError):
                device_id = None

            try:
                instance = site_registration_distributed_trees['id']
            except (IndexError,KeyError):
                instance = None

            try:
                submitter = site_registration_distributed_trees['submitter']
            except (IndexError,KeyError):
                submitter = None

            try:
                submission_date = left(site_registration_distributed_trees['submissionDate'],10)
            except (IndexError,KeyError):
                submission_date = None

            try:
                form_version = site_registration_distributed_trees['formVersion']
            except (IndexError,KeyError):
                form_version = None
                form_version = str(form_version)

            try:
                test = site_registration_distributed_trees['responses']['205421525'][0]['262680218'][0].get('text')
            except (IndexError,KeyError):
                test = None

            try:
                name_region_village_planting_site = site_registration_distributed_trees['responses']['205421525'][0].get('205610424')
            except (IndexError,KeyError):
                name_region_village_planting_site = None


            try:
                name_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0].get('251020087')
            except (IndexError,KeyError):
                name_owner_planting_site = None

            try:
                gender_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0]['234700078'][0].get('text')
            except (IndexError,KeyError):
                gender_owner_planting_site = None

            try:
                if site_registration_distributed_trees['responses']['205421525'][0]['251000760'] != None:
                    photo_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0]['251000760'].get('filename')
                else:
                    photo_owner_planting_site = None
            except (IndexError,KeyError):
                photo_owner_planting_site = None

            try:
                nr_trees_received = site_registration_distributed_trees['responses']['205421525'][0].get('258590461')
            except (IndexError,KeyError):
                nr_trees_received = None

            try:
                confirm_plant_location_own_land = site_registration_distributed_trees['responses']['205421525'][0]['245660427'][0].get('text')
            except (IndexError,KeyError):
                confirm_plant_location_own_land = None

            try:
                one_multiple_planting_sites = site_registration_distributed_trees['responses']['205421525'][0]['251052748'][0].get('text')
            except (IndexError,KeyError):
                one_multiple_planting_sites = None

            try:
                nr_trees_given_away = site_registration_distributed_trees['responses']['205421525'][0].get('262731726')
            except (IndexError,KeyError):
                nr_trees_given_away = None

            try:
                more_less_200_trees = site_registration_distributed_trees['responses']['205421525'][0]['215732096'][0].get('text')
            except (IndexError,KeyError):
                more_less_200_trees = None

            try:
                date_tree_planting = left(site_registration_distributed_trees['responses']['205421525'][0]['247472690'],10)
            except (IndexError,KeyError):
                date_tree_planting = None

            try:
                centroid_coord_lat = site_registration_distributed_trees['responses']['231532129'][0]['205652116'].get('lat')
                centroid_coord_lon = site_registration_distributed_trees['responses']['231532129'][0]['205652116'].get('long')
            except (IndexError,KeyError):
                centroid_coord_lat = None
                centroid_coord_lon = None

            if centroid_coord_lat is None or centroid_coord_lon is None:
                centroid_coord = None
            else:
                lat_centr_conv = str(centroid_coord_lat)
                lon_centr_conv = str(centroid_coord_lon)
                centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'


            try:
                geom_get = site_registration_distributed_trees['responses']['231532129'] # Up to this level it can go wrong (due to empty entry)
                if geom_get != None:
                    geom_get = site_registration_distributed_trees['responses']['231532129'][0]['264382254']['features'][0].get('geometry','')
                    area_ha = area(geom_get)
                    area_ha = round((area_ha/10000),3)
                    geometry = convert.geojson_to_wkt(geom_get)
                    get_geom_type = geometry.split(' ',1)
                    if get_geom_type[0] == 'POLYGON':
                        polygon = convert.geojson_to_wkt(geom_get)
                        coord = re.findall('\s', polygon)
                        number_coord_pol = int((len(coord)/2)-1)
                        if number_coord_pol < 3:
                            polygon = None
                    else:
                        geom_get = None
                        area_ha = None
                        polygon = None
                else:
                    geom_get = None
                    area_ha = None
                    polygon = None

            except (IndexError, KeyError):
                polygon = None
                number_coord_pol = None
                area_ha = None

            try:
                avg_tree_distance = site_registration_distributed_trees['responses']['251064121'][0].get('247454719')
                print(avg_tree_distance)
            except (IndexError,KeyError):
                avg_tree_distance = None

            try:
                estimated_area = site_registration_distributed_trees['responses']['251064121'][0].get('256794593')
            except (IndexError,KeyError):
                estimated_area = None

            try:
                unit_estimated_area = site_registration_distributed_trees['responses']['251064121'][0]['258644833'][0].get('text')
            except (IndexError,KeyError):
                unit_estimated_area = None

            try:
                estimated_tree_number_planted = site_registration_distributed_trees['responses']['251064121'][0].get('256784230')
            except (IndexError,KeyError):
                estimated_tree_number_planted = None

            try:
                confirm_additional_photos = site_registration_distributed_trees['responses']['245650331'][0]['260300823'][0].get('text')
            except (IndexError,KeyError):
                confirm_additional_photos = None

            try:
                status_of_trees = site_registration_distributed_trees['responses']['262740058'][0]['247390855'][0].get('text')
            except (IndexError,KeyError):
                status_of_trees = None

            try:
                comment_enumerator = site_registration_distributed_trees['responses']['262740058'][0].get('2231510689')
            except (IndexError,KeyError):
                comment_enumerator = None


            cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees
            (identifier_akvo, displayname, device_id, instance, submitter, submission_date, form_version, test,
            name_region_village_planting_site, name_owner_planting_site, gender_owner_planting_site,
            photo_owner_planting_site, nr_trees_received, confirm_plant_location_own_land,
            one_multiple_planting_sites, nr_trees_given_away, more_less_200_trees, date_tree_planting, centroid_coord,
            polygon, number_coord_pol, area_ha, avg_tree_distance, estimated_area, unit_estimated_area,
            estimated_tree_number_planted, confirm_additional_photos, comment_enumerator)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, submitter, submission_date, form_version, test,
            name_region_village_planting_site, name_owner_planting_site, gender_owner_planting_site,
            photo_owner_planting_site, nr_trees_received, confirm_plant_location_own_land,
            one_multiple_planting_sites, nr_trees_given_away, more_less_200_trees, date_tree_planting, centroid_coord,
            polygon, number_coord_pol, area_ha, avg_tree_distance, estimated_area, unit_estimated_area,
            estimated_tree_number_planted, confirm_additional_photos, comment_enumerator))

            try:
                site_registration_distributed_trees['responses']['245650331']
            except KeyError:
                continue
            else:
                for x in site_registration_distributed_trees['responses']['245650331']:
                    print(x)
                    for k,v in x.items():
                        print(v)
                        try:
                            if isinstance(v, dict):
                                photo_url_4 = v['filename']
                            else:
                                continue
                        except KeyError:
                            continue
                        try:
                            if isinstance(v, dict) and v['location'] is not None:
                                photo_location = v['location']
                            else:
                                photo_location = None
                        except KeyError:
                            photo_location = None
                        else:
                            if isinstance(v, dict):
                                photo_location = v['location']
                                if photo_location is not None:
                                    photo_geotag_lat = v['location']['latitude']
                                    photo_lat = str(photo_geotag_lat)
                                    photo_geotag_lon = v['location']['longitude']
                                    photo_lon = str(photo_geotag_lon)
                                    photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                        cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees_photos
                        (identifier_akvo, displayname, device_id, instance, photo_url_4, photo_location)
                        VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, photo_url_4, photo_location))


            try:
                site_registration_distributed_trees['responses']['252970460']
            except (IndexError,KeyError):
                species_lat = None
                species_local = None
            else:
                for x in site_registration_distributed_trees['responses']['252970460']: # Gives list with X number of dictionaries
                    print('PPPPPPPP :', x)
                    for k,v in x.items(): # Gives a list for v containing 2 dictionaries
                        try:
                            species_lat = v[1]['code']
                            species_local = v[1]['name']
                        except (KeyError, IndexError):
                            species_lat = None
                            species_local = None
                        else: # Get the list (v), select the second item from this list (=dict) and then the values from this dict
                            species_lat = v[1]['code']
                            species_local = v[1]['name']

                        cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees_species
                        (identifier_akvo, displayname, device_id, instance, species_lat, species_local)
                        VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, species_lat, species_local))


        # WHEN ALL DATA FROM A PAGE IS PROCESSED SUCCESFULLY, THE nextPageUrl IS ADDED TO THE DATABASE.
        # IF INTERRUPTED (by an error), THE nextPageUrl IS NOT ADDED. THEN IN A NEXT RUN, THE PREVIOUS nextPageUrl WILL BE USED AS A START.
        get_nextPageUrl = json_dict.get('nextPageUrl', None) # get the nextPageUrl from the latest page
        if get_nextPageUrl == None:
            print('Message 7: No nextPageUrl was found. Either all data was harvested or a page was empty. The script ended')
            cur.execute('''SELECT COUNT(DISTINCT(identifier)) FROM test_number_identifiers_tree_registration_unregfar;''')
            end_message_unique = cur.fetchone()[0]
            cur.execute('''SELECT COUNT(*) FROM test_number_identifiers_tree_registration_unregfar;''')
            end_message_total = cur.fetchone()[0]
            print('Message 8: A total of ', end_message_unique, ' UNIQUE registration instances was harvested so far within a total of ', end_message_total, ' instances. Check if this is all data')
            message_delete_table = input('Message 9: Do you want to delete the temporary_url_table_tree_registration_unregfar? Write yes or no: ')
            if message_delete_table == 'yes':
                cur.execute('''DROP TABLE temporary_url_table_tree_registration_unregfar''')
                conn.commit()
                cur.execute('''DROP TABLE test_number_identifiers_tree_registration_unregfar''')
                conn.commit()
                sys.exit()
            elif message_delete_table == 'no':
                sys.exit()
            else:
                sys.exit()


        else:
            cur.execute('''INSERT INTO temporary_url_table_tree_registration_unregfar(download_url) VALUES (%s)''', (get_nextPageUrl,))
            conn.commit()
