import requests
import json
import psycopg2
import re
import geojson
import geodaisy.converters as convert
from area import area
import os

#config = Config()

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

form_response_tree_distribution = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=215246811&form_id=252327848&page_size=250'
form_response_tree_registration = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=215246811&form_id=258280042&page_size=250'

# Create list with first url from registration form
url_list = list()
url_list.append(form_site_audits) # this one is needed to add the first url to the url list

# Add other next-URL's to the list of registration forms
for all_pages in url_list:
    print("URL retrieved for listing: ", all_pages)
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


#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_distribution_unregistered_farmers;
DROP TABLE IF EXISTS AKVO_Tree_distribution_unregistered_tree_species;
DROP TABLE IF EXISTS AKVO_site_registration_distributed_trees;
DROP TABLE IF EXISTS AKVO_site_registration_distributed_trees_photos;
DROP TABLE IF EXISTS AKVO_site_registration_distributed_trees_species;

CREATE TABLE AKVO_tree_distribution_unregistered_farmers (identifier_akvo TEXT, display_name TEXT, device_id TEXT, instance INTEGER,
submission DATE, submitter TEXT, AKVO_form_version NUMERIC(10,2), country TEXT, test TEXT, organisation TEXT, contract_number NUMERIC(20,2),
name_tree_receiver TEXT, gender_tree_receiver TEXT, check_ownership_trees TEXT, check_ownership_land TEXT, url_photo_receiver_trees TEXT,
url_photo_id_card_tree_receiver TEXT, location_house_tree_receiver TEXT, name_site_id_tree_planting TEXT, confirm_planting_location TEXT,
total_number_trees_received INTEGER, url_signature_tree_receiver TEXT);

CREATE TABLE AKVO_Tree_distribution_unregistered_tree_species (identifier_akvo TEXT, display_name TEXT,
device_id TEXT, instance INTEGER, species_lat TEXT, species_local TEXT, number_species INTEGER);

CREATE TABLE AKVO_site_registration_distributed_trees (identifier_akvo TEXT, displayname TEXT, device_id TEXT,
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


def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]

count_pages_registration_data = 0

for all_data in url_list:
    load_page = requests.get(all_data, headers=headers).content
    page_decode = load_page.decode()
    json_dict = json.loads(page_decode)
    count_page = count_pages_registration_data + 1
    print("Nr. processed pages registration data: ", count_page)

    def left(var, amount):
        return var[:amount]

    def mid(var,begin,end):
        return var[begin:end]

    def photo(key, url):
        return


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

        conn.commit()

        try:
            name_species_local = registration_distributed_trees['responses']['242845264']
        except (IndexError,KeyError):
            name_species_local = None
            name_species_latin = None
        else:
            for x in registration_distributed_trees['responses']['242845264']:
                print(x)
                try:
                    number_species = x.get('258032656')
                except (IndexError,KeyError):
                    number_species = None
                try:
                    name_species_local = x.get('211873502')[1].get('name')
                except (IndexError,KeyError):
                    name_species_local = None
                try:
                    name_species_latin = x.get('211873502')[1].get('code')
                except (IndexError,KeyError):
                    name_species_latin = None

            cur.execute('''INSERT INTO AKVO_Tree_distribution_unregistered_tree_species (identifier_akvo, display_name,
            device_id, instance, species_lat, species_local, number_species)
            VALUES (%s,%s,%s,%s,%s,%s,%s)''',
            (identifier_akvo, displayname, device_id, instance, name_species_latin, name_species_local, number_species))

            conn.commit()

# Create list with first url from registration form
url_list_m = list()
url_list_m.append(form_response_tree_registration) # this one is needed to add the first url to the url list



# Add other next-URL's to the list of registration forms
for all_pages in url_list_m:
    print("URL retrieved for listing: ", all_pages)
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

count_pages_monitoring_data = 0


for all_data_m in url_list_m:
    load_page = requests.get(all_data_m, headers=headers).content
    page_decode = load_page.decode()
    json_dict_m = json.loads(page_decode)
    count_page_m = count_pages_monitoring_data + 1
    print("Nr. processed pages registration data: ", count_page)

    for site_registration_distributed_trees in json_dict_m['formInstances']:

        #print(registration_light)
        try:
            identifier_akvo = site_registration_distributed_trees['identifier']
        except (IndexError,KeyError):
            identifier_akvo = None
            print(identifier_akvo)

        try:
            displayname = site_registration_distributed_trees['displayName']
        except (IndexError,KeyError):
            displayname = None
            print(displayname)

        try:
            device_id = site_registration_distributed_trees['deviceIdentifier']
        except (IndexError,KeyError):
            device_id = None
            print(device_id)

        try:
            instance = site_registration_distributed_trees['id']
        except (IndexError,KeyError):
            instance = None
            print(instance)

        try:
            submitter = site_registration_distributed_trees['submitter']
        except (IndexError,KeyError):
            submitter = None
            print(submitter)

        try:
            submission_date = left(site_registration_distributed_trees['submissionDate'],10)
        except (IndexError,KeyError):
            submission_date = None
            print(submission_date)

        try:
            form_version = site_registration_distributed_trees['formVersion']
        except (IndexError,KeyError):
            form_version = None
            form_version = str(form_version)

        try:
            test = site_registration_distributed_trees['responses']['205421525'][0]['262680218'][0].get('text')
            print(test)
        except (IndexError,KeyError):
            test = None

        try:
            name_region_village_planting_site = site_registration_distributed_trees['responses']['205421525'][0].get('205610424')
            print(name_region_village_planting_site)
        except (IndexError,KeyError):
            name_region_village_planting_site = None


        try:
            name_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0].get('251020087')
            print(name_owner_planting_site)
        except (IndexError,KeyError):
            name_owner_planting_site = None

        try:
            gender_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0]['234700078'][0].get('text')
            print(gender_owner_planting_site)
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
            print(nr_trees_received)
        except (IndexError,KeyError):
            nr_trees_received = None

        try:
            confirm_plant_location_own_land = site_registration_distributed_trees['responses']['205421525'][0]['245660427'][0].get('text')
            print(confirm_plant_location_own_land)
        except (IndexError,KeyError):
            confirm_plant_location_own_land = None

        try:
            one_multiple_planting_sites = site_registration_distributed_trees['responses']['205421525'][0]['251052748'][0].get('text')
            print(one_multiple_planting_sites)
        except (IndexError,KeyError):
            one_multiple_planting_sites = None

        try:
            nr_trees_given_away = site_registration_distributed_trees['responses']['205421525'][0].get('262731726')
            print(nr_trees_given_away)
        except (IndexError,KeyError):
            nr_trees_given_away = None

        try:
            more_less_200_trees = site_registration_distributed_trees['responses']['205421525'][0]['215732096'][0].get('text')
            print(more_less_200_trees)
        except (IndexError,KeyError):
            more_less_200_trees = None

        try:
            date_tree_planting = left(site_registration_distributed_trees['responses']['205421525'][0]['247472690'],10)
            print(date_tree_planting)
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
            print(estimated_area)
        except (IndexError,KeyError):
            estimated_area = None

        try:
            unit_estimated_area = site_registration_distributed_trees['responses']['251064121'][0]['258644833'][0].get('text')
            print(unit_estimated_area)
        except (IndexError,KeyError):
            unit_estimated_area = None

        try:
            estimated_tree_number_planted = site_registration_distributed_trees['responses']['251064121'][0].get('256784230')
            print(estimated_tree_number_planted)
        except (IndexError,KeyError):
            estimated_tree_number_planted = None

        try:
            confirm_additional_photos = site_registration_distributed_trees['responses']['245650331'][0]['260300823'][0].get('text')
            print('22222 :', confirm_additional_photos)
        except (IndexError,KeyError):
            confirm_additional_photos = None

        try:
            status_of_trees = site_registration_distributed_trees['responses']['262740058'][0]['247390855'][0].get('text')
            print('33333 :',status_of_trees)
        except (IndexError,KeyError):
            status_of_trees = None

        try:
            comment_enumerator = site_registration_distributed_trees['responses']['262740058'][0].get('2231510689')
            print('44444 :',comment_enumerator)
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

        conn.commit()

        try:
            site_registration_distributed_trees['responses']['245650331']
        except (IndexError,KeyError):
            photo_url_4 = None
            photo_location = None
        else:
            for x in site_registration_distributed_trees['responses']['245650331']:
                remove_list = x.pop('260300823')
                for k,v in x.items():
                   photo_url_4 = v.get('filename')
                   try:
                       v['location']
                   except (IndexError,KeyError):
                       photo_location = None
                   else:
                       if v['location'] is not None:
                           photo_geotag_lat = v.get('location')['latitude']
                           photo_lat = str(photo_geotag_lat)
                           photo_geotag_lon = v.get('location')['longitude']
                           photo_lon = str(photo_geotag_lon)
                           photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                       else:
                           photo_location = None

                       cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees_photos
                       (identifier_akvo, displayname, device_id, instance, photo_url_4, photo_location)
                       VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, photo_url_4, photo_location))

                       conn.commit()



        try:
            site_registration_distributed_trees['responses']['252970460']
        except (IndexError,KeyError):
            species_lat = None
            species_local = None
        else:
            for x in site_registration_distributed_trees['responses'].get('252970460'):
                for y in x.values():
                    if not y: # check if list is empty
                        species_lat = None
                        species_local = None
                    else:
                        species_lat = y[1].get('code')
                        species_local = y[1].get('name')
                        #print(species_lat, species_local)

            cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees_species
            (identifier_akvo, displayname, device_id, instance, species_lat, species_local)
            VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, species_lat, species_local))

            conn.commit()
