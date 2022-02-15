import requests
import json
import sqlite3
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
import os

form_response_tree = 'https://api-auth0.akvo.org/flow/orgs/ecosia/form_instances?survey_id=31840001&form_id=48090001'
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

# The code block below must only be executed during a full download of the database. The NextSyncUrl that restuls from the Initial Request is used
# to update (sync) the database. As can be seen, thsi URL is written to a file (sync_urls.txt) that is used to store the url for later sync requests.
get_initial_syncurl = requests.get(initial_sync_request, headers = headers)
converturltotxt = get_initial_syncurl.text
converttojson = json.loads(converturltotxt)
initial_syncurl = converttojson.get('nextSyncUrl',"Ecosia: No sync url was found in this instance")
with open('sync_urls_PG.txt', 'w') as f:
    f.write(initial_syncurl)

with open('sync_initial_url_after download_PG.txt', 'w') as f:
    f.write(initial_syncurl)

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
        
print('all urls loaded')

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS AKVO_Tree_registration_species;
DROP TABLE IF EXISTS AKVO_Tree_registration_photos;
DROP TABLE IF EXISTS AKVO_Tree_registration_areas;

CREATE TABLE AKVO_Tree_registration_areas (identifier_akvo TEXT PRIMARY KEY, display_name TEXT, device_id TEXT, instance INTEGER, submission TEXT, submission_year SMALLINT, submissiontime TEXT, submitter TEXT, modifiedAt TEXT, AKVO_form_version TEXT, country TEXT, test TEXT, organisation TEXT, contract_number NUMERIC(20,2), id_planting_site TEXT, land_title TEXT, name_village TEXT, name_region TEXT, name_owner TEXT, gender_owner TEXT, objective_site TEXT, site_preparation TEXT, planting_technique TEXT, planting_system TEXT, remark TEXT, nr_trees_option TEXT, planting_date TEXT, tree_number INTEGER, estimated_area NUMERIC(20,3), calc_area NUMERIC(20,3), lat_y REAL, lon_x REAL, number_coord_polygon INTEGER, centroid_coord geography(POINT, 4326), polygon geography(POLYGON, 4326), multipoint geography(MULTIPOINT, 4326));

CREATE TABLE AKVO_Tree_registration_photos (identifier_akvo TEXT, instance INTEGER, photo_url TEXT, photo_location geography(POINT, 4326), FOREIGN KEY (identifier_akvo) REFERENCES AKVO_Tree_registration_areas (identifier_akvo) ON DELETE CASCADE);

CREATE TABLE AKVO_Tree_registration_species (identifier_akvo TEXT, instance INTEGER, lat_name_species TEXT, local_name_species TEXT, number_species INTEGER, FOREIGN KEY (identifier_akvo) REFERENCES AKVO_Tree_registration_areas (identifier_akvo) ON DELETE CASCADE);

''')

def left(var, amount):
    return var[:amount]

def mid(var,begin,end):
    return var[begin:end]


for all_data in url_list:
    load_page = requests.get(all_data, headers=headers).content
    page_decode = load_page.decode()
    json_dict = json.loads(page_decode)
    print('jsons loaded')


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
            nr_trees_planted = level1['responses']['56230114'][0].get('3990003',None)
        except (KeyError, IndexError):
            nr_trees_planted = None

        try:
            if level1['responses']['56230114'][0].get('54050005') != None:
                planting_date = level1['responses']['56230114'][0].get('54050005')
                planting_date_trunc = left(planting_date,10)
            else:
                planting_date = None
                planting_date_trunc = None
        except (KeyError, IndexError):
            planting_date = None
            planting_date_trunc = None


        try:
            estimated_area = level1['responses']['56230114'][0].get('39860006',None)
        except (KeyError, IndexError):
            estimated_area = None

        try:
            lat_centr = level1['responses']['1960007'][0]['25860015']['lat']
            lat_centr_conv = str(lat_centr)
        except (KeyError, IndexError):
            lat_centr = None

        try:
            lon_centr = level1['responses']['1960007'][0]['25860015']['long']
            lon_centr_conv = str(lon_centr)
        except (KeyError, IndexError):
            lon_centr = None

        try:
            centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'
        except (KeyError, IndexError):
            centroid_coord = None

        try:
            geom_get = level1['responses']['1960007'][0]['50110008'] # Up to this level it can go wrong (due to empty entry)
            if geom_get != None:
                geom_get = level1['responses']['1960007'][0]['50110008']['features'][0].get('geometry','')
                area_ha = area(geom_get)
                area_ha = round((area_ha/10000),3)
                geometry = convert.geojson_to_wkt(geom_get)
                get_geom_type = geometry.split(' ',1)
                if get_geom_type[0] == 'POLYGON':
                    polygon = convert.geojson_to_wkt(geom_get)
                    coord = re.findall('\s', polygon)
                    number_coord_pol = int((len(coord)/2)-1)
                    multipoint = None
                    if number_coord_pol < 3: # Need to skipp erroneous polygons since Postgres does not accepts them (gives error at loading)
                        polygon = None
                        multipoint = None
                        remark += remark + 'Automated message: Polygon was generated by partner but had less than 3 points'

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

        print('the following area registration identifier was parsed: ', identifier)

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
                photo_items4 = list(photo.values())


                for url4 in photo_items4:
                    photo_r4_url = url4['filename']

                    try: #print(photo_url) # print multiple rows well up to here with only urls
                        if url4['location'] is not None:
                            photo_lat = url4['location']['latitude']
                            photo_lon = url4['location']['longitude']
                            photo_lat = str(photo_lat)
                            photo_lon = str(photo_lon)
                            photo_r4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                    except:
                        photo_lat = None
                        photo_lon = None
                        photo_r4_location = None
                    

                    print('photo identifier:',identifier_akvo)
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
                        species_latin = ''
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
                        species_local = ''
                    else:
                        species_local = x['50340047'][0]['name']
                else:
                    species_local = x['50340047'][1]['name']

                number_species = x.get('50530001', 0)
                
                print('Species identifier: ', identifier_akvo)

                cur.execute('''INSERT INTO AKVO_Tree_registration_species (identifier_akvo, instance, lat_name_species, local_name_species, number_species)
                VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, species_latin, species_local, number_species))

                conn.commit()
