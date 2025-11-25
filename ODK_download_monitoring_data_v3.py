from pyodk.client import Client
import pandas as pd
import requests
import re
import json
import psycopg2
from shapely.ops import transform
from shapely.geometry import Polygon
from shapely.geometry import Point
from shapely.geometry import LineString
import os
import sys


# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''

DROP TABLE IF EXISTS ODK_Tree_monitoring_main;
DROP TABLE IF EXISTS ODK_Tree_monitoring_add_photos;
DROP TABLE IF EXISTS ODK_Tree_monitoring_count_trees;
DROP TABLE IF EXISTS ODK_Tree_monitoring_count_photos;
DROP TABLE IF EXISTS ODK_Tree_monitoring_pcq;
DROP TABLE IF EXISTS ODK_Tree_monitoring_own_method;''')

conn.commit()


cur.execute('''CREATE TABLE ODK_Tree_monitoring_main (submissionid_odk TEXT, ecosia_site_id TEXT, submission_date DATE, form_version TEXT, field_date DATE, updated_at TIMESTAMPTZ, username TEXT,
country TEXT, organisation TEXT, id_planting_site TEXT, test TEXT, reporting_type TEXT,
monitoring_type TEXT, monitoring_method TEXT, contract_number_monitoring NUMERIC(20,2), landscape_element_type TEXT, remaped_polygon_planting_site geography(POLYGON, 4326),
remap_line_planting_site geography(LINESTRING, 4326), remap_point_planting_site geography(POINT, 4326), nr_added_trees INTEGER, contract_number_added_trees NUMERIC(20,2), tree_species_added TEXT,
tree_species_added_other TEXT, method_monitoring TEXT, overall_quality_site TEXT, alert_for_site TEXT, overall_observation_site TEXT);

CREATE TABLE ODK_Tree_monitoring_add_photos (submissionid_odk TEXT, repeatid_odk TEXT, add_photo_1 TEXT,
add_photo_2 TEXT, add_photo_3 TEXT, add_photo_4 TEXT, add_photo_remark TEXT, add_photo_gps_location geography(POINT, 4326));

CREATE TABLE ODK_Tree_monitoring_count_trees (submissionid_odk TEXT, repeatid_odk TEXT, tree_species TEXT, count_species INTEGER, avg_tree_height_species NUMERIC(10,2));

CREATE TABLE ODK_Tree_monitoring_count_photos (submissionid_odk TEXT, repeatid_odk TEXT, tree_photo TEXT, tree_photo_gps geography(POINT, 4326));

CREATE TABLE ODK_Tree_monitoring_pcq (submissionid_odk TEXT, repeatid_odk TEXT, gps_pcq_sample geography(POINT, 4326), lat_pcq_sample REAL, lon_pcq_sample REAL, tree_distance_q1 NUMERIC(20,2), tree_height_q1 NUMERIC(20,2), tree_species_pcq_q1 TEXT, tree_type_pcq1 TEXT,tree_dbh_pcq_q1 NUMERIC(20,2),tree_photo_q1 TEXT,
tree_distance_q2 NUMERIC(20,2), tree_height_q2 NUMERIC(20,2),tree_species_pcq_q2 TEXT,tree_type_pcq2 TEXT,tree_dbh_pcq_q2 NUMERIC(20,2),tree_photo_q2 TEXT, tree_distance_q3 NUMERIC(20,2),
tree_height_q3 NUMERIC(20,2),tree_species_pcq_q3 TEXT,tree_type_pcq3 TEXT,tree_dbh_pcq_q3 NUMERIC(20,2),tree_photo_q3 TEXT,tree_distance_q4 NUMERIC(20,2),tree_height_q4 NUMERIC(20,2),
tree_species_pcq_q4 TEXT,tree_type_pcq4 TEXT,tree_dbh_pcq_q4 NUMERIC(20,2),tree_photo_q4 TEXT);


CREATE TABLE ODK_Tree_monitoring_own_method (submissionid_odk TEXT, repeatid_odk TEXT, tree_species_own_method TEXT,
tree_number_own_method INTEGER, tree_height_own_method NUMERIC(20,2));''')

#own_method_number_tree_species,

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

json_monitoring_main = client.submissions.get_table(form_id='planting_site_reporting')['value']
json_additional_photos = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_existing_site.group_add_photos.repeat_additional_photos')['value']
json_pcq_method = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_existing_site.group_tree_survival.group_pcq_tree_number.pcq_method_repeat')['value']
json_count_tree_photos = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_existing_site.group_tree_survival.repeat_tree_count_photos')['value']
json_count_trees = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_existing_site.group_tree_survival.group_count_tree_number.repeat_tree_count_per_species')['value']
json_own_method = client.submissions.get_table(form_id='planting_site_reporting', table_name='Submissions.group_existing_site.group_tree_survival.group_own_method.own_method_survived_trees_repeat')['value']



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


for json_in_main in json_monitoring_main:
    # Populate the tree monitoring main table
    print(json_in_main)
    if json_extract(json_in_main, 'reporting_type')[0] == 'existing_site':
        submissionid_odk = json_extract(json_in_main, 'instanceID')[0]
        ecosia_site_id = json_extract(json_in_main, 'instanceid_to_monitor')[0]
        id_planting_site = json_extract(json_in_main, 'id_planting_site_monitor')[0]
        submission_date = json_extract(json_in_main, 'submissionDate')[0]
        form_version = json_extract(json_in_main, 'formVersion')[0]
        field_date = json_extract(json_in_main, 'today')[0]
        updated_at = json_extract(json_in_main, 'updatedAt')[0]
        username = json_extract(json_in_main, 'username')[0]
        country_registration_save = json_extract(json_in_main, 'country_to_monitor')[0]
        organisation = json_extract(json_in_main, 'organisation')[0]
        test = json_extract(json_in_main, 'test_data_yes_no')[0]
        reporting_type = json_extract(json_in_main, 'reporting_type')[0]
        monitoring_type = json_extract(json_in_main, 'reporting_activity_existing_site')[0]
        monitoring_method = json_extract(json_in_main, 'method_monitoring')[0]
        #reporting_activity_new_site = json_extract(json_in_main, 'reporting_activity_new_site')[0]
        contract_number_monitoring = json_extract(json_in_main, 'contract_number_to_monitor')[0]
        landscape_element_type = json_extract(json_in_main, 'landscape_element_type')[0]

        if json_in_main['group_existing_site']['group_remap_site']['gps_center_planting_site_remapped'] != None:
            return_list = convert_point_wkt(json_in_main['group_existing_site']['group_remap_site']['gps_center_planting_site_remapped']['coordinates'])
            remaped_point_planting_site = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            remaped_point_planting_site = None
            lon_x = None
            lat_y = None


        #if landscape_element_new_site == 'line_planting':
        if json_in_main['group_existing_site']['group_remap_site']['line_planting_site_remapped'] != None:
            remaped_line_planting_site = convert_line_wkt(json_in_main['group_existing_site']['group_remap_site']['line_planting_site_remapped']['coordinates'])
            #print('LINESTRING:', geometry_planting_line)
        else:
            remaped_line_planting_site = None

        #if landscape_element_new_site == 'area_planting':
        if json_in_main['group_existing_site']['group_remap_site']['polygon_planting_site_remapped'] != None:
            remaped_polygon_planting_site = convert_polygon_wkt(json_in_main['group_existing_site']['group_remap_site']['polygon_planting_site_remapped']['coordinates'][0])
            #print('POLYGON: ',geometry_planting_polygon)
        else:
            remaped_polygon_planting_site = None

        nr_added_trees = json_extract(json_in_main, 'tree_number_added')[0]
        contract_number_added_trees = json_extract(json_in_main, 'contract_number_added_trees')[0]
        tree_species_added = json_extract(json_in_main, 'tree_species_added')[0]
        tree_species_added_other = json_extract(json_in_main, 'other_tree_species_added')[0]
        method_monitoring = json_extract(json_in_main, 'method_monitoring')[0]
        overall_quality_site = json_extract(json_in_main, 'overall_quality_site')[0]
        alert_for_site = json_extract(json_in_main, 'alert_site')[0]
        overall_observation_site = json_extract(json_in_main, 'free_text')[0]


        cur.execute('''INSERT INTO ODK_Tree_monitoring_main (
        submissionid_odk, ecosia_site_id, submission_date, form_version, field_date, updated_at, username, country, organisation, id_planting_site, test, reporting_type,
        monitoring_type, monitoring_method, contract_number_monitoring, landscape_element_type, remaped_polygon_planting_site,
        remap_line_planting_site, remap_point_planting_site, nr_added_trees, contract_number_added_trees, tree_species_added,
        tree_species_added_other, method_monitoring, overall_quality_site, alert_for_site, overall_observation_site) VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (submissionid_odk, ecosia_site_id, submission_date, form_version, field_date, updated_at, username, country_registration_save, organisation, id_planting_site, test, reporting_type,
        monitoring_type, monitoring_method, contract_number_monitoring, landscape_element_type,
        remaped_polygon_planting_site,remaped_line_planting_site, remaped_point_planting_site, nr_added_trees,
        contract_number_added_trees, tree_species_added,tree_species_added_other, method_monitoring, overall_quality_site,
        alert_for_site, overall_observation_site))

        conn.commit()


# Find every instance of `name` in a Python dictionary.
for json_in in json_additional_photos:
    #print(json_in)
    submissionid_odk = json_extract(json_in, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in, '__id')[0]

    try:
        photo_1 = json_extract(json_in, 'additional_photo_1')[0]
    except:
        photo_1 = ''

    photo_2 = json_extract(json_in, 'additional_photo_2')[0]
    photo_3 = json_extract(json_in, 'additional_photo_3')[0]
    photo_4 = json_extract(json_in, 'additional_photo_4')[0]

    if json_in['group_photo_taking']['additional_photo_gps'] != None:
        return_list = convert_point_wkt(json_in['group_photo_taking']['additional_photo_gps']['coordinates'])
        add_photo_gps_location = return_list[0]

    add_photo_remark = json_extract(json_in, 'additional_photo_remark')[0]

    # Populate the tree monitoring table
    cur.execute('''INSERT INTO ODK_Tree_monitoring_add_photos (submissionid_odk, repeatid_odk, add_photo_1,
    add_photo_2, add_photo_3, add_photo_4, add_photo_remark, add_photo_gps_location)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, repeatid_odk, photo_1, photo_2, photo_3, photo_4, add_photo_remark, add_photo_gps_location))

    conn.commit()


for json_in_pcq in json_pcq_method:
    #print(json_in_pcq)
    submissionid_odk = json_extract(json_in_pcq, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in_pcq, '__id')[0]

    if json_in_pcq['gps_pcq_sample'] != None:
        return_list = convert_point_wkt(json_in_pcq['gps_pcq_sample']['coordinates'])
        gps_pcq_sample = return_list[0]
        lon_pcq_sample = return_list[1]
        lat_pcq_sample = return_list[2]
    else:
        gps_pcq_sample = None
        lon_pcq_sample = None
        lat_pcq_sample = None


    tree_distance_q1 = json_extract(json_in_pcq, 'tree_distance_q1')[0]
    tree_height_q1 = json_extract(json_in_pcq, 'tree_height_q1')[0]
    tree_species_pcq_q1 = json_extract(json_in_pcq, 'tree_species_pcq_q1')[0]
    tree_type_pcq1 = json_extract(json_in_pcq, 'tree_type_pcq1')[0]
    tree_dbh_pcq_q1 = json_extract(json_in_pcq, 'tree_dbh_pcq_q1')[0]
    tree_photo_q1 = json_extract(json_in_pcq, 'tree_distance_q2')[0]
    tree_distance_q2 = json_extract(json_in_pcq, 'tree_distance_q2')[0]
    tree_height_q2 = json_extract(json_in_pcq, 'tree_height_q2')[0]
    tree_species_pcq_q2 = json_extract(json_in_pcq, 'tree_species_pcq_q2')[0]
    tree_type_pcq2 = json_extract(json_in_pcq, 'tree_type_pcq2')[0]
    tree_dbh_pcq_q2 = json_extract(json_in_pcq, 'tree_dbh_pcq_q2')[0]
    tree_photo_q2 = json_extract(json_in_pcq, 'tree_photo_q2')[0]
    tree_distance_q3 = json_extract(json_in_pcq, 'tree_distance_q3')[0]
    tree_height_q3 = json_extract(json_in_pcq, 'tree_height_q3')[0]
    tree_species_pcq_q3 = json_extract(json_in_pcq, 'tree_species_pcq_q3')[0]
    tree_type_pcq3 = json_extract(json_in_pcq, 'tree_type_pcq3')[0]
    tree_dbh_pcq_q3 = json_extract(json_in_pcq, 'tree_dbh_pcq_q3')[0]
    tree_photo_q3 = json_extract(json_in_pcq, 'tree_photo_q3')[0]
    tree_distance_q4 = json_extract(json_in_pcq, 'tree_distance_q4')[0]
    tree_height_q4 = json_extract(json_in_pcq, 'tree_height_q4')[0]
    tree_species_pcq_q4 = json_extract(json_in_pcq, 'tree_species_pcq_q4')[0]
    tree_type_pcq4 = json_extract(json_in_pcq, 'tree_type_pcq4')[0]
    tree_dbh_pcq_q4 = json_extract(json_in_pcq, 'tree_dbh_pcq_q4')[0]
    tree_photo_q4 = json_extract(json_in_pcq, 'tree_photo_q4')[0]


    cur.execute('''INSERT INTO ODK_Tree_monitoring_pcq (submissionid_odk, repeatid_odk, gps_pcq_sample, lat_pcq_sample, lon_pcq_sample, tree_distance_q1, tree_height_q1,
    tree_species_pcq_q1, tree_type_pcq1, tree_dbh_pcq_q1, tree_photo_q1, tree_distance_q2, tree_height_q2, tree_species_pcq_q2, tree_type_pcq2,
    tree_dbh_pcq_q2, tree_photo_q2, tree_distance_q3, tree_height_q3, tree_species_pcq_q3, tree_type_pcq3, tree_dbh_pcq_q3, tree_photo_q3,
    tree_distance_q4, tree_height_q4, tree_species_pcq_q4, tree_type_pcq4, tree_dbh_pcq_q4, tree_photo_q4) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, repeatid_odk, gps_pcq_sample, lat_pcq_sample, lon_pcq_sample, tree_distance_q1, tree_height_q1,
    tree_species_pcq_q1, tree_type_pcq1,tree_dbh_pcq_q1,tree_photo_q1,tree_distance_q2, tree_height_q2,tree_species_pcq_q2,tree_type_pcq2,
    tree_dbh_pcq_q2,tree_photo_q2, tree_distance_q3,tree_height_q3,tree_species_pcq_q3,tree_type_pcq3,tree_dbh_pcq_q3,tree_photo_q3,
    tree_distance_q4,tree_height_q4,tree_species_pcq_q4,tree_type_pcq4,tree_dbh_pcq_q4,tree_photo_q4))

    conn.commit()


#for json_in_count_photos in json_count_tree_photos:


for json_in_count_trees in json_count_trees:
    #print(json_in_count_trees)
    submissionid_odk = json_extract(json_in_count_trees, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in_count_trees, '__id')[0]
    tree_species = json_extract(json_in_count_trees, 'selected_tree_species')[0]
    count_species = json_extract(json_in_count_trees, 'tree_number_count_save')[0]
    avg_tree_height_species = json_extract(json_in_count_trees, 'tree_height_count')[0]

    cur.execute('''INSERT INTO ODK_Tree_monitoring_count_trees (submissionid_odk, repeatid_odk, tree_species, count_species, avg_tree_height_species) VALUES (%s,%s,%s,%s,%s)''', (submissionid_odk, repeatid_odk, tree_species, count_species, avg_tree_height_species))

    conn.commit()


for json_in_count_photos in json_count_tree_photos:
    #print(json_in_count_photos)
    submissionid_odk = json_extract(json_in_count_photos, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in_count_photos, '__id')[0]
    tree_photo = json_extract(json_in_count_photos, 'tree_photo_count')[0]

    if json_in_count_photos['group_tree_count_photos']['tree_photo_gps_count'] != None:
        return_list = convert_point_wkt(json_in_count_photos['group_tree_count_photos']['tree_photo_gps_count']['coordinates'])
        tree_photo_gps = return_list[0]
    else:
        tree_photo_gps = None

    cur.execute('''INSERT INTO ODK_Tree_monitoring_count_photos (submissionid_odk, repeatid_odk, tree_photo, tree_photo_gps) VALUES (%s,%s,%s,%s)''', (submissionid_odk, repeatid_odk, tree_photo, tree_photo_gps))

    conn.commit()


for json_in_own_method in json_own_method:

    submissionid_odk = json_extract(json_in_own_method, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in_own_method, '__id')[0]
    tree_species_own_method = json_extract(json_in_own_method, 'tree_species_own_method')[0]
    tree_number_own_method = json_extract(json_in_own_method, 'tree_number_own_method')[0]
    tree_height_own_method = json_extract(json_in_own_method, 'tree_height_own_method')[0]

    cur.execute('''INSERT INTO ODK_Tree_monitoring_own_method (submissionid_odk, repeatid_odk, tree_species_own_method,
    tree_number_own_method, tree_height_own_method) VALUES (%s,%s,%s,%s,%s)''', (submissionid_odk, repeatid_odk, tree_species_own_method, tree_number_own_method, tree_height_own_method))

    conn.commit()



client.close()
