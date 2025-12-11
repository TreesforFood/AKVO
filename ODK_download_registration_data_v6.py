from pyodk.client import Client
import pandas as pd
import requests
import re
import json
import psycopg2
# from shapely.ops import transform
# from shapely.geometry import Polygon
# from shapely.geometry import Point
# from shapely.geometry import LineString
import os
import sys


# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS ODK_Tree_registration_photos;
DROP TABLE IF EXISTS ODK_Tree_registration_areas;
DROP TABLE IF EXISTS ODK_Tree_registration_main;
DROP TABLE IF EXISTS ODK_Tree_registration_tree_species;''')

conn.commit()


cur.execute('''CREATE TABLE IF NOT EXISTS ODK_Tree_registration_main (FID SERIAL PRIMARY KEY, submissionid_odk TEXT, ecosia_site_id TEXT, device_id TEXT, updated_at TIMESTAMPTZ,
field_date DATE, submission_date DATE, submission_date_time_start TIMESTAMPTZ, start TIMESTAMPTZ, ends TIMESTAMPTZ, submitter TEXT, odk_form_version TEXT, test TEXT,
reporting_type TEXT, reporting_activity_new_site TEXT, country TEXT, organisation TEXT, contract_number NUMERIC(20,2), id_planting_site TEXT, land_title TEXT, name_owner TEXT,
landscape_element_type TEXT, photo_owner TEXT, gender_owner TEXT, planting_technique TEXT, remark TEXT, planting_date TEXT, tree_number INTEGER, tree_species TEXT, calc_area NUMERIC(20,3), lat_y REAL, lon_x REAL,
centroid_coord geography(POINT, 4326), polygon geography(POLYGON, 4326), line geography(LINESTRING, 4326));

CREATE TABLE IF NOT EXISTS tree_registration_main_manual_submissions (FID SERIAL PRIMARY KEY, submissionid_odk TEXT, ecosia_site_id TEXT, device_id TEXT, submission_date DATE,
field_date DATE, submission_date_time_start TIMESTAMPTZ, submitter TEXT, odk_form_version TEXT, test TEXT,
reporting_type TEXT, reporting_activity_new_site TEXT, country TEXT, organisation TEXT, contract_number NUMERIC(20,2), id_planting_site TEXT, land_title TEXT, name_owner TEXT,
landscape_element_type TEXT, photo_owner TEXT, gender_owner TEXT, planting_technique TEXT, remark TEXT, planting_date TEXT, tree_number INTEGER, tree_species TEXT, calc_area NUMERIC(20,3), lat_y REAL, lon_x REAL,
centroid_coord geometry(POINT, 4326), polygon geometry(POLYGON, 4326), line geometry(LINESTRING, 4326));

CREATE TABLE IF NOT EXISTS ODK_Tree_registration_photos (submissionid_odk TEXT, repeatid_odk TEXT, photo_name_1 TEXT, photo_name_2 TEXT, photo_name_3 TEXT, photo_name_4 TEXT, photo_gps_location geography(POINT, 4326));

CREATE TABLE IF NOT EXISTS ODK_Tree_registration_tree_species (submissionid_odk TEXT, species_name_latin TEXT, iucn_code TEXT, native_exotic TEXT, nr_trees_per_species INTEGER);''')

conn.commit()

# Retrieve environment variables
base_url = "https://ecosia.getodk.cloud"
username = os.environ["ODK_CENTRAL_USERNAME"]
password = os.environ["ODK_CENTRAL_PASSWORD"]
default_project_id = 1

# Define the file content
file_content = f"""[central]
base_url = "{base_url}"
username = "{username}"
password = "{password}"
default_project_id = {default_project_id}
"""

# Define a writable path (/app/tmp is a writable directory on Heroku)
file_path = "/app/tmp/pyodk_config.ini"

# Create the directory if it doesn't exist
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Write the configuration to the file
with open(file_path, "w") as file:
    file.write(file_content)

# Connect to ODK central server and use the merge command
client = Client(config_path="/app/tmp/pyodk_config.ini", cache_path="/app/tmp/pyodk_cache.ini")
client.open()

json_registration = client.submissions.get_table(form_id='planting_site_reporting')['value']
json_photos_planting_site = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_new_site.group_tree_photos.repeat_photos_polygon')['value']
json_nr_per_tree_species = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_new_site.group_tree_registration.repeat_registration_nr_species')['value']



# # check for audit requests: https://docs.getodk.org/central-api-system-endpoints/#server-audit-logs
# audit_report_soft_deleted_submissions = client.get('audits?action=submission.delete', headers={'X-Extended-Metadata': 'true'}).json()
# #print(audit_report_soft_deleted_submissions)
# audit_report_modified_submissions = client.get('audits?action=submission.update.version', headers={'X-Extended-Metadata': 'true'}).json()
# #print(audit_report_modified_submissions)
# audit_report_created_submissions = client.get('audits?action=submission.create', headers={'X-Extended-Metadata': 'true'}).json()

"""truncate from right to keep last characters"""
def truncate_from_right(s, begin):
    return s[begin:]

"""truncate middle characters and rename label"""
def truncate_middle(s, begin, end):
    string = s[begin:end]
    if string == 'N':
        exotic_native = string.replace('N', 'native')
    elif string == 'E':
        exotic_native = string.replace('E', 'exotic')
    else:
        exotic_native = 'not defined'
    return exotic_native


"""Converts list of coordinates into WKT and reverse latlon to lonlat)"""
def convert(list):
    def flip(x, y):
        """Flips the x and y coordinate values"""
        return y, x
    dict = {}
    lat_lon_coords = []

    # Create a dictionary and appending the polygons to this dictionary
    for lon_lat in list[0]:
        if lon_lat is None:
            lon_lat = None
        else:
            lat_lon_coords.append(Polygon(lon_lat))
            #lat_lon_coords.append(transform(flip, lon_lat_coords).wkt)

    return lat_lon_coords


"""Removes the z-values from a polygon tuple withs coordinates ((5.897, 52.00, 0), (5.895, 52.001, 0)) >> ((5.897, 52.00), (5.895, 52.001))"""
def flatten_polygon(nested):
    xy_coordinates = []
    xy_polygon = ()

    for item in nested:
        if isinstance(item, tuple):
            if len(item) >= 3:  # Check if the tuple has at least 3 elements
                xy_coordinates.append(item[0:2])  # Extract the z-coordinate
                xy_coordinates.extend(flatten_polygon(item))  # Recur for nested tuples >> [(x,y), (x,y)]
                xy_polygon = tuple(xy_coordinates) # convert main list into tuple >> ((x,y), (x,y))
    return xy_polygon


"""Converts the polygon coordinate strings (inside a list) into a WKT format."""
def convert_polygon_wkt(coordinate_list):
    def to_tuple(coordinate_list):
        """Convert a nested json dictionary into a nested tuple """
        return tuple(to_tuple(i) if isinstance(i, list) else i for i in coordinate_list)

    if len(coordinate_list) < 3:
        polygon = None
    else:
        ll = coordinate_list
        ll = to_tuple(ll)
        ll = flatten_polygon(ll)
        polygon = Polygon(ll)
        return polygon.wkt


"""Converts the linestring coordinate strings (inside a list) into a WKT format."""
def convert_line_wkt(coordinate_list):
    def to_tuple(coordinate_list):
        """Convert a nested json dictionary into a nested tuple """
        return tuple(to_tuple(i) if isinstance(i, list) else i for i in coordinate_list)

    if len(coordinate_list) < 3:
        line = None
    else:
        ll = coordinate_list
        ll = to_tuple(ll)
        ll = flatten_polygon(ll)
        line = LineString(ll)
        return line.wkt


"""Converts the centroid point coordinates (inside a list) into a WKT format"""
def convert_point_wkt(coordinate_list):

    if len(coordinate_list) < 2:
        centroid_coord = None
    else:
        lat_long = coordinate_list
        lat_long_remove_z = lat_long.pop(2)
        lat_long_tuple = tuple(lat_long)
        centroid_coord = Point(lat_long_tuple)
        lon = lat_long[0]
        lat = lat_long[1]
        return [centroid_coord.wkt, lon, lat]


"""Extract nested values from a JSON tree."""
def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values

# count = 0
#
# for json_in in audit_report_created_submissions:
#     #print(json_in)
#     count = count + 1
#     instanceId = json_extract(json_in, 'instanceId')[0]
#     createdAt = json_extract(json_in, 'createdAt')
#     loggedAt = json_extract(json_in, 'loggedAt')
#     #print('CREATED SUBMISSIONS: ', count, instanceId, createdAt, loggedAt)
#
#
# for json_in in audit_report_modified_submissions:
#     acteeId = json_extract(json_in, 'acteeId')[0]
#     created_at = json_extract(json_in, 'updatedAt')[0]
#     meta_instanceID = json_extract(json_in, 'instanceId')[0] # be carefull: the variable in this json is instanceId NOT instanceID!
#     #key = json_extract(json_in, '__id')[0] # bestaat niet in deze json...?
#     updated_at = json_extract(json_in, 'loggedAt')[0] # As is seems, the 'loggedAt' key represent the modification date
#     #print('UPDATED: ',updatedAt, loggedAt, meta_instanceID)
#
#
# for json_in in audit_report_soft_deleted_submissions:
#     acteeId = json_extract(json_in, 'acteeId')[0]
#     updatedAt = json_extract(json_in, 'updatedAt')[0]
#     instanceId = json_extract(json_in, 'instanceId')[0] # be carefull: the variable in this json is instanceId NOT instanceID!
#     #print('DELETED: ',updatedAt, instanceId)

count = 0
for json_in in json_registration:
    #print(json_in)
    if json_extract(json_in, 'reporting_type')[0] == 'new_site':
        count = count+1

        submissionid_odk = json_extract(json_in, 'instanceID')[0]
        ecosia_site_id = json_extract(json_in, 'instanceid')[0]

        start = json_extract(json_in, 'start')[0]
        end = json_extract(json_in, 'end')[0]
        updated_at = json_extract(json_in, 'updatedAt')[0]
        submission_date = json_extract(json_in, 'submissionDate')[0]
        today = json_extract(json_in, 'today')[0]
        print('id:', ecosia_site_id, 'updatedAt:', updated_at, 'submission_date:', submission_date, 'Start:', start, 'End:', end)

        device_id = json_extract(json_in, 'device_id')[0]
        submitter = json_extract(json_in, 'username')[0]

        odk_form_version = json_extract(json_in, 'formVersion')[0]

        country = json_extract(json_in, 'country_registration_save')[0]
        test_data_yes_no = json_extract(json_in, 'test_data_yes_no')[0]
        land_title = json_extract(json_in, 'landuse_title')[0]
        name_owner_individual = json_extract(json_in, 'name_owner_individual')[0]
        gender_owner = json_extract(json_in, 'gender_owner')[0]
        name_owner_communal = json_extract(json_in, 'name_owner_communal')[0]
        planting_technique = json_extract(json_in, 'planting_technique')[0]
        test = json_extract(json_in, 'test_data_yes_no')[0]
        reporting_type = json_extract(json_in, 'reporting_type')[0]
        reporting_activity_new_site = json_extract(json_in, 'reporting_activity_new_site')[0]
        area_calculation_round_decimal = json_extract(json_in, 'area_calculation_round_decimal')[0]
        baseline_mature_trees = json_extract(json_in, 'baseline_mature_trees')[0]
        id_planting_site = json_extract(json_in, 'id_planting_site')[0]
        landscape_element_new_site = json_extract(json_in, 'landscape_element_type')[0]

        if json_in['group_new_site']['group_map_new_site']['gps_center_planting_site'] != None:
            return_list = convert_point_wkt(json_in['group_new_site']['group_map_new_site']['gps_center_planting_site']['coordinates'])
            geometry_planting_point = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            geometry_planting_point = None
            lon_x = None
            lat_y = None


        #if landscape_element_new_site == 'line_planting':
        if json_in['group_new_site']['group_map_new_site']['line_planting_site'] != None:
            geometry_planting_line = convert_line_wkt(json_in['group_new_site']['group_map_new_site']['line_planting_site']['coordinates'])
            #print('LINESTRING:', geometry_planting_line)
        else:
            geometry_planting_line = None

        #if landscape_element_new_site == 'area_planting':
        if json_in['group_new_site']['group_map_new_site']['polygon_planting_site'] != None:
            geometry_planting_polygon = convert_polygon_wkt(json_in['group_new_site']['group_map_new_site']['polygon_planting_site']['coordinates'][0])
            #print('POLYGON: ',geometry_planting_polygon)
        else:
            geometry_planting_polygon = None

        contract_number = json_extract(json_in, 'contract_number')[0]
        tree_species = json_extract(json_in, 'tree_species_registered')[0]
        organisation = json_extract(json_in, 'organisation')[0]
        tree_number = json_extract(json_in, 'tree_number')[0]
        audit = json_extract(json_in, 'audit')[0]
        meta_instanceID = json_extract(json_in, 'instanceID')[0] # be carefull: the variable in this json is instanceID NOT instanceId!
        name_region = 'still to be intergrated'
        photo_owner = json_extract(json_in, 'photo_owner')[0]
        remark = json_extract(json_in, 'remark')[0]
        #nr_trees_option = json_extract(json_in, 'landscape_element_type')[0]
        planting_date = json_extract(json_in, 'planting_date')[0]

        # Create a temp CTE table to download all main registration data from ODK
        cur.execute('''INSERT INTO ODK_Tree_registration_main (submissionid_odk, ecosia_site_id, device_id, updated_at, field_date, submission_date, submission_date_time_start, start, ends, submitter, odk_form_version, test, reporting_type, reporting_activity_new_site, country, organisation, contract_number, id_planting_site, land_title, name_owner, landscape_element_type, photo_owner, gender_owner, planting_technique, remark, planting_date, tree_number, tree_species, calc_area, lat_y, lon_x, centroid_coord, polygon, line)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, ecosia_site_id, device_id, updated_at, today, submission_date, submission_date, start, end, submitter, odk_form_version, test, reporting_type, reporting_activity_new_site, country, organisation, contract_number, id_planting_site, land_title, name_owner_individual, landscape_element_new_site, photo_owner, gender_owner, planting_technique, remark, planting_date, tree_number, tree_species, area_calculation_round_decimal, lat_y, lon_x, geometry_planting_point, geometry_planting_polygon, geometry_planting_line))

        conn.commit()


for json_in_tree_species in json_nr_per_tree_species:
    #print(json_in_tree_species)
    submissionid_odk = json_extract(json_in_tree_species, '__Submissions-id')[0]
    species_name_latin = json_extract(json_in_tree_species, 'calculate_species_position')[0]
    nr_trees_per_species = json_extract(json_in_tree_species, 'nr_trees_per_species_registered')[0]
    iucn_code = truncate_from_right(str(species_name_latin),-3)
    native_exotic = truncate_middle(str(species_name_latin),-3, -2)
    #print(submissionid_odk, species_name_latin, nr_trees_per_species)


    # Create a temp CTE table to download all main registration data from ODK
    cur.execute('''INSERT INTO ODK_Tree_registration_tree_species (submissionid_odk, species_name_latin, iucn_code, native_exotic, nr_trees_per_species)
    VALUES (%s,%s,%s,%s,%s)''', (submissionid_odk, species_name_latin, iucn_code, native_exotic, nr_trees_per_species))

    conn.commit()


# After the insert of new manual submissions into the main table, the table content with manual submissions can be deleted
cur.execute('''TRUNCATE tree_registration_main_manual_submissions''')


# In order to update/modify data in QGIS we need an FID serial column. Since this was already made, we first have to drop this old column and then create a new FID column with serial numbers. This is the most secure approach compared to updating the FID.
cur.execute('''
ALTER TABLE ODK_Tree_registration_main
DROP COLUMN IF EXISTS FID;''')
conn.commit()

# Add the FID column in order to be able to edit the data in QGIS (without the FID column editing is not possible in QGIS)
cur.execute('''ALTER TABLE ODK_Tree_registration_main ADD column FID SERIAL PRIMARY KEY''')
conn.commit()


# We first create the pgcrypto extension to enable the generation of new uuid's. THis is for new submissions made in QGIS (manual uploads)
cur.execute('''CREATE EXTENSION IF NOT EXISTS pgcrypto;''')
conn.commit()

# Update the uuid's for new records that were added to the database (e.g. by manual upload) in QGIS. Add the 'uuid:' so that this matches the typo comming from ODK submissions.
cur.execute('''UPDATE ODK_Tree_registration_main
SET submissionid_odk = gen_random_uuid()
WHERE submissionid_odk IS NULL;''')
conn.commit()


for json_in in json_photos_planting_site:
    print(json_in)
    submissionid_odk = json_extract(json_in, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in, '__id')[0]
    if json_in['group_photos']['gps_photo_polygon'] != None:
        return_list = convert_point_wkt(json_in['group_photos']['gps_photo_polygon']['coordinates'])
        gps_photo_polygon = return_list[0]

    instanceID = json_extract(json_in, '__Submissions-id')[0]

    if json_extract(json_in, 'photo_tree_polygon_1')[0] is not None:
        #photo_1 = "https://ecosia.getodk.cloud"+"/projects/"+str(1)+"/forms/"+str('planting_site_reporting')+"/submissions/"+str(instanceID)+"/attachments/"+json_extract(json_in, 'photo_tree_polygon_1')[0]
        photo_1 = 'https://ecosia.getodk.cloud/v1/projects/1/forms/planting_site_reporting/submissions/'+instanceID+'/attachments/'+json_extract(json_in, 'photo_tree_polygon_1')[0];
    else:
        photo_1 = ''

    if json_extract(json_in, 'photo_tree_polygon_2')[0] is not None:
        #photo_1 = "https://ecosia.getodk.cloud"+"/projects/"+str(1)+"/forms/"+str('planting_site_reporting')+"/submissions/"+str(instanceID)+"/attachments/"+json_extract(json_in, 'photo_tree_polygon_1')[0]
        photo_2 = 'https://ecosia.getodk.cloud/v1/projects/1/forms/planting_site_reporting/submissions/'+instanceID+'/attachments/'+json_extract(json_in, 'photo_tree_polygon_2')[0];
    else:
        photo_2 = ''

    if json_extract(json_in, 'photo_tree_polygon_3')[0] is not None:
        #photo_1 = "https://ecosia.getodk.cloud"+"/projects/"+str(1)+"/forms/"+str('planting_site_reporting')+"/submissions/"+str(instanceID)+"/attachments/"+json_extract(json_in, 'photo_tree_polygon_1')[0]
        photo_3 = 'https://ecosia.getodk.cloud/v1/projects/1/forms/planting_site_reporting/submissions/'+instanceID+'/attachments/'+json_extract(json_in, 'photo_tree_polygon_3')[0];
    else:
        photo_3 = ''

    if json_extract(json_in, 'photo_tree_polygon_4')[0] is not None:
        #photo_1 = "https://ecosia.getodk.cloud"+"/projects/"+str(1)+"/forms/"+str('planting_site_reporting')+"/submissions/"+str(instanceID)+"/attachments/"+json_extract(json_in, 'photo_tree_polygon_1')[0]
        photo_4 = 'https://ecosia.getodk.cloud/v1/projects/1/forms/planting_site_reporting/submissions/'+instanceID+'/attachments/'+json_extract(json_in, 'photo_tree_polygon_4')[0];
    else:
        photo_4 = ''

    # Populate the photo registration table
    cur.execute('''INSERT INTO ODK_Tree_registration_photos (submissionid_odk, repeatid_odk, photo_name_1, photo_name_2, photo_name_3, photo_name_4, photo_gps_location)
    VALUES (%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, repeatid_odk, photo_1, photo_2, photo_3, photo_4, gps_photo_polygon))

    conn.commit()

client.close()
