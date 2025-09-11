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
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_handout_main;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_handout_species;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_handout_other_recipients;

DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_registration_main;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_registration_species;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_registration_photos;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_registration_additional_areas;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_registration_additional_areas_species;

DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_monitoring_main;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_monitoring_photos;
DROP TABLE IF EXISTS ODK_unregistered_farmers_tree_monitoring_species;

;''')

conn.commit()

cur.execute('''
CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_handout_main (submissionid_odk TEXT, ecosia_site_id_dist TEXT, device_id TEXT, submission_date DATE,
field_date DATE, submission_date_time_start TIMESTAMPTZ, submitter TEXT, odk_form_version TEXT, test TEXT, organisation TEXT,
reporting_type TEXT, contract_number NUMERIC(20,2), location_type_handout_select TEXT, name_nursery_handout TEXT, other_location_handout_name TEXT,
other_location_handout_gps geography(POINT, 4326), total_tree_nr_handed_out INTEGER, choice_tree_ownership TEXT, name_location_tree_planting TEXT,
planting_site_id TEXT, recipient_full_name TEXT, recipient_gender TEXT, id_recipient TEXT, recipient_photo TEXT);

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_handout_species (submissionid_odk TEXT, tree_species_handed_out TEXT, tree_species_number INTEGER);

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_handout_other_recipients (submissionid_odk TEXT,
name_other_recipient TEXT, distance_other_recipient TEXT, tree_number_other_recipient INTEGER);

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_registration_main (submissionid_odk TEXT, ecosia_site_id_dist TEXT, submitter TEXT, device_id TEXT, odk_form_version TEXT, submission_date DATE, field_date DATE, planting_date TEXT, test TEXT, planting_system_used TEXT, comment_planting_site TEXT, registration_multiple_locations TEXT, polygon geography(POLYGON, 4326), line geography(LINESTRING, 4326), point geography(POINT, 4326));

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_registration_species (submissionid_odk TEXT, tree_species_registered TEXT, tree_species_number_registered INTEGER);

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_registration_photos (submissionid_odk TEXT, url_registration_photo_1 TEXT, url_registration_photo_2 TEXT, url_registration_photo_3 TEXT, url_registration_photo_4 TEXT, gps_location_photo geography(POINT, 4326));

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_registration_additional_areas (submissionid_odk TEXT, repeatid_odk TEXT, landscape_element_type_other TEXT, comment_planting_site_other TEXT, additional_polygon geography(POLYGON, 4326), additional_line geography(LINESTRING, 4326), additional_gps_point geography(POINT, 4326));

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_registration_additional_areas_species (submissionid_odk TEXT, repeatid_odk TEXT, tree_species_registered_additional TEXT, tree_species_number_registered_additional INTEGER);

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_monitoring_species (submissionid_odk TEXT, ecosia_site_id TEXT, repeatid_odk TEXT, tree_species_monitored TEXT, tree_species_number_monitored TEXT, tree_species_height NUMERIC(20,2));

CREATE TABLE IF NOT EXISTS ODK_unregistered_farmers_tree_monitoring_photos(submissionid_odk TEXT, ecosia_site_id TEXT, repeatid_odk TEXT, url_registration_photo_1 TEXT, url_registration_photo_2 TEXT, url_registration_photo_3 TEXT, url_registration_photo_4 TEXT);


''')

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

json_unregistered_farmers_main = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers')['value']
json_unregistered_farmers_tree_species = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_distribution.repeat_tree_details')['value']
json_unregistered_farmers_other_recipients = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_distribution.repeat_other_recipients')['value']
json_unregistered_farmers_tree_registration_species = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_registration.repeat_tree_number_and_species')['value']
json_unregistered_farmers_tree_registration_photos = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_registration.group_tree_registration_photos.repeat_registration_photos_polygon')['value']
json_unregistered_farmers_tree_registration_additional_area = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_registration.repeat_area_registration_other')['value']
json_unregistered_farmers_tree_registration_additional_area_species = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_registration.repeat_tree_registration_number_and_species_other')['value']
json_unregistered_farmers_tree_monitoring_main = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_monitoring.repeat_monitoring_photos_polygon')['value']
json_unregistered_farmers_tree_monitoring_species = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_monitoring.repeat_monitoring_tree_number_and_species_other')['value']
json_unregistered_farmers_tree_monitoring_photos = client.submissions.get_table(form_id='tree_distribution_unregistered_farmers', table_name='Submissions.group_tree_monitoring.repeat_monitoring_photos_polygon')['value']


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

count = 0

for json_in in json_unregistered_farmers_main:
    #print(json_in)
    if json_extract(json_in, 'reporting_type')[0] == 'tree_hand_out':
        count = count+1
        #print(json_in)
        submissionid_odk = json_extract(json_in, 'instanceID')[0]
        ecosia_site_id_dist = json_extract(json_in, 'instanceid')[0]
        device_id = json_extract(json_in, 'device_id')[0]
        submission_date = json_extract(json_in, 'start')[0]
        field_date = json_extract(json_in, 'today')[0]
        submission_date_time_start = json_extract(json_in, 'start')[0]
        submitter = json_extract(json_in, 'username')[0]
        odk_form_version = json_extract(json_in, 'form_version')[0]
        test = json_extract(json_in, 'test_data_yes_no')[0]
        organisation = json_extract(json_in, 'organisation')[0]
        reporting_type = json_extract(json_in, 'reporting_type')[0]
        contract_number = json_extract(json_in, 'contract_number_save')[0]
        location_type_handout_select = json_extract(json_in, 'distribution_nursery_other')[0]
        name_nursery_handout = json_extract(json_in, 'distribution_nursery_selection')[0]
        other_location_handout_name = json_extract(json_in, 'distribution_location_name')[0]

        if json_in['group_tree_distribution']['group_location_hand_out']['distribution_location_gps'] != None:
            return_list = convert_point_wkt(json_in['group_tree_distribution']['group_location_hand_out']['distribution_location_gps']['coordinates'])
            other_location_handout_gps = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            other_location_handout_gps = None
            lon_x = None
            lat_y = None

        total_tree_nr_handed_out = json_extract(json_in, 'recipient_total_nr_trees')[0]
        choice_tree_ownership = json_extract(json_in, 'recipient_tree_ownership')[0]
        name_location_tree_planting = json_extract(json_in, 'recipient_site_location')[0]
        planting_site_id = json_extract(json_in, 'recipient_id_planting_site')[0]
        recipient_full_name = json_extract(json_in, 'recipient_full_name')[0]
        recipient_gender = json_extract(json_in, 'recipient_gender')[0]
        id_recipient = json_extract(json_in, 'id_recipient')[0]
        recipient_photo = json_extract(json_in, 'recipient_photo')[0]


        # Populate the table with recipients of trees at the central distribution
        cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_handout_main (submissionid_odk, ecosia_site_id_dist, device_id, submission_date,
        field_date, submission_date_time_start, submitter, odk_form_version, test, organisation,
        reporting_type, contract_number, location_type_handout_select, name_nursery_handout, other_location_handout_name,
        other_location_handout_gps, total_tree_nr_handed_out, choice_tree_ownership, name_location_tree_planting,
        planting_site_id, recipient_full_name, recipient_gender, id_recipient, recipient_photo)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, ecosia_site_id_dist, device_id, submission_date,
        field_date, submission_date_time_start, submitter, odk_form_version, test, organisation,
        reporting_type, contract_number, location_type_handout_select, name_nursery_handout, other_location_handout_name,
        other_location_handout_gps, total_tree_nr_handed_out, choice_tree_ownership, name_location_tree_planting,
        planting_site_id, recipient_full_name, recipient_gender, id_recipient, recipient_photo))

        conn.commit()

for json_in_species in json_unregistered_farmers_tree_species:
    #print(json_in_species)
    submissionid_odk = json_extract(json_in_species, '__Submissions-id')[0]
    tree_species_handed_out = json_extract(json_in_species, 'recipient_tree_species')[0]
    tree_species_number = json_extract(json_in_species, 'recipient_nr_per_tree_species')[0]

    # Populate the table that indicates how much trees per tree species were handed out to recipient.
    cur.execute('''INSERT INTO  ODK_unregistered_farmers_tree_handout_species (submissionid_odk, tree_species_handed_out, tree_species_number)
    VALUES (%s,%s,%s)''', (submissionid_odk, tree_species_handed_out, tree_species_number))

    conn.commit()

for json_in_other_recipients in json_unregistered_farmers_other_recipients:
    #print(json_in_other_recipients)
    submissionid_odk = json_extract(json_in_other_recipients, '__Submissions-id')[0]
    other_recipient_name = json_extract(json_in_other_recipients, 'recipient_other_people_names')[0]
    other_recipient_distance = json_extract(json_in_other_recipients, 'recipient_other_people_distance')[0]
    other_recipient_tree_number = json_extract(json_in_other_recipients, 'recipient_other_people_nr_trees')[0]

    # Populate the table that indicates how much trees per tree species were handed out to recipient.
    cur.execute('''INSERT INTO  ODK_unregistered_farmers_tree_handout_other_recipients (submissionid_odk,
    name_other_recipient, distance_other_recipient, tree_number_other_recipient) VALUES (%s,%s,%s,%s)''',
    (submissionid_odk, other_recipient_name, other_recipient_distance, other_recipient_tree_number))

    conn.commit()


for json_in_tree_registration in json_unregistered_farmers_main:
    print(json_in_tree_registration)
    if json_extract(json_in_tree_registration, 'reporting_type')[0] == 'tree_registration':
        ecosia_site_id_dist = json_extract(json_in_tree_registration, 'tree_registration_name_recipient')[0]
        submitter = json_extract(json_in_tree_registration, 'username')[0]
        submitter = json_extract(json_in_tree_registration, 'device_id')[0]
        odk_form_version = json_extract(json_in_tree_registration, 'form_version')[0]
        submission_date = json_extract(json_in_tree_registration, 'start')[0]
        field_date = json_extract(json_in_tree_registration, 'today')[0]
        planting_date = json_extract(json_in_tree_registration, 'planting_date')[0]
        test = json_extract(json_in_tree_registration, 'test_data_yes_no')[0]
        submissionid_odk = json_extract(json_in_tree_registration, 'instanceID')[0]
        landscape_element_type = json_extract(json_in_tree_registration, 'landscape_element_type')[0]
        #comment_planting_site = json_extract(json_in_tree_registration, 'registration_comment')[0]
        comment_planting_site = ''
        registration_multiple_locations = json_extract(json_in_tree_registration, 'registration_multiple_locations')[0]


        #if landscape_element == 'point planting':
        if json_in_tree_registration['group_tree_registration']['gps_center_planting_site']!= None:
            return_list = convert_point_wkt(json_in_tree_registration['group_tree_registration']['gps_center_planting_site']['coordinates'])
            gps_center_planting_site = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            gps_center_planting_site = None
            lon_x = None
            lat_y = None

        #if landscape_element == 'line_planting':
        if json_in_tree_registration['group_tree_registration']['line_planting_site']!= None:
            line_planting_site = convert_line_wkt(json_in_tree_registration['group_tree_registration']['line_planting_site']['coordinates'])
        else:
            line_planting_site = None

        #if landscape_element == 'area_planting':
        if json_in_tree_registration['group_tree_registration']['polygon_planting_site']!= None:
            polygon_planting_site = convert_polygon_wkt(json_in_tree_registration['group_tree_registration']['polygon_planting_site']['coordinates'][0])
        else:
            polygon_planting_site = None

        # Populate the table that indicates the registered locations of the tree plantings
        cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_registration_main (submissionid_odk, ecosia_site_id_dist, submitter, device_id, odk_form_version, submission_date, field_date, planting_date, test, planting_system_used, comment_planting_site, registration_multiple_locations, polygon, line, point) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (submissionid_odk, ecosia_site_id_dist, submitter, device_id, odk_form_version, submission_date, field_date, planting_date, test, landscape_element_type, comment_planting_site, registration_multiple_locations, polygon_planting_site, line_planting_site, gps_center_planting_site))

        conn.commit()


for json_in_tree_registration_species in json_unregistered_farmers_tree_registration_species:
    submissionid_odk = json_extract(json_in_tree_registration_species, '__Submissions-id')[0]
    tree_species_registered = json_extract(json_in_tree_registration_species, 'registration_species')[0]
    tree_species_number_registered = json_extract(json_in_tree_registration_species, 'registration_tree_number_count_save')[0]

    # Populate the table that indicates the registered tree species at the planting locations
    cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_registration_species (submissionid_odk, tree_species_registered, tree_species_number_registered) VALUES (%s,%s,%s)''',
    (submissionid_odk, tree_species_registered, tree_species_number_registered))

    conn.commit()

for json_in_tree_registration_photos in json_unregistered_farmers_tree_registration_photos:
    #print(json_in_tree_registration_photos)
    submissionid_odk = json_extract(json_in_tree_registration_photos, '__Submissions-id')[0]
    #repeatid_odk = json_extract(json_in_tree_registration_photos, '__id')[0]
    if json_in_tree_registration_photos['group_registration_photos']['registration_gps_photo_polygon'] != None:
        return_list = convert_point_wkt(json_in_tree_registration_photos['group_registration_photos']['registration_gps_photo_polygon']['coordinates'])
        gps_photo_polygon = return_list[0]
        photo_1 = json_extract(json_in_tree_registration_photos, 'registration_photo_tree_polygon_1')[0]
        photo_2 = json_extract(json_in_tree_registration_photos, 'registration_photo_tree_polygon_2')[0]
        photo_3 = json_extract(json_in_tree_registration_photos, 'registration_photo_tree_polygon_3')[0]
        photo_4 = json_extract(json_in_tree_registration_photos, 'registration_photo_tree_polygon_4')[0]

        # Populate the table that indicates the registered tree species at the planting locations
        cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_registration_photos  (submissionid_odk, url_registration_photo_1, url_registration_photo_2, url_registration_photo_3, url_registration_photo_4, gps_location_photo) VALUES (%s,%s,%s,%s,%s,%s)''',
        (submissionid_odk, photo_1, photo_1, photo_1, photo_1, gps_photo_polygon))

        conn.commit()


for json_in_tree_registration_additional_area in json_unregistered_farmers_tree_registration_additional_area:
    submissionid_odk = json_extract(json_in_tree_registration_additional_area, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in_tree_registration_additional_area, '__id')[0]
    landscape_element_type_other = json_extract(json_in_tree_registration_additional_area, 'landscape_element_type_other')[0]
    #comment_planting_site_other = json_extract(json_in_tree_registration_additional_area, 'registration_comment_other')[0]
    comment_planting_site_other = ''


    #if landscape_element == 'point planting':
    if json_in_tree_registration_additional_area['group_tree_registration_other']['gps_center_planting_site_other']!= None:
        return_list = convert_point_wkt(json_in_tree_registration_additional_area['group_tree_registration_other']['gps_center_planting_site_other']['coordinates'])
        gps_center_planting_site_other = return_list[0]
        lon_x = return_list[1]
        lat_y = return_list[2]
    else:
        gps_center_planting_site_other = None
        lon_x = None
        lat_y = None

    #if landscape_element == 'line_planting':
    if json_in_tree_registration_additional_area['group_tree_registration_other']['line_planting_site_other']!= None:
        line_planting_site_other = convert_line_wkt(json_in_tree_registration_additional_area['group_tree_registration_other']['line_planting_site_other']['coordinates'])
    else:
        line_planting_site_other = None

    #if landscape_element == 'area_planting':
    if json_in_tree_registration_additional_area['group_tree_registration_other']['polygon_planting_site_other']!= None:
        polygon_planting_site_other = convert_polygon_wkt(json_in_tree_registration_additional_area['group_tree_registration_other']['polygon_planting_site_other']['coordinates'][0])
    else:
        polygon_planting_site_other = None

    # Populate the table with registered areas that are additional
    cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_registration_additional_areas(submissionid_odk, repeatid_odk, landscape_element_type_other, comment_planting_site_other, additional_polygon, additional_line, additional_gps_point) VALUES (%s,%s,%s,%s,%s,%s,%s)''',
    (submissionid_odk, repeatid_odk, landscape_element_type_other, comment_planting_site_other, polygon_planting_site_other, line_planting_site_other, gps_center_planting_site_other))

    conn.commit()


for json_in_tree_registration_additional_area_species in json_unregistered_farmers_tree_registration_additional_area_species:
    submissionid_odk = json_extract(json_in_tree_registration_additional_area_species, '__Submissions-id')[0]
    repeatid_odk = json_extract(json_in_tree_registration_additional_area_species, '__id')[0]
    species_additional_area = json_extract(json_in_tree_registration_additional_area_species, 'registration_species_other')[0]
    number_species_additional_area = json_extract(json_in_tree_registration_additional_area_species, 'registration_tree_number_count_save_other')[0]

    # Populate the table with tree species planted in additional areas
    cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_registration_additional_areas_species(submissionid_odk, repeatid_odk, tree_species_registered_additional, tree_species_number_registered_additional) VALUES (%s,%s,%s,%s)''',
    (submissionid_odk, repeatid_odk, species_additional_area, number_species_additional_area))

    conn.commit()



# for json_in_tree_monitoring_species in json_unregistered_farmers_tree_monitoring_species:
#     print('SSSS:', json_in_tree_monitoring_species)
    # submissionid_odk = json_extract(json_in_tree_monitoring_species, '__Submissions-id')[0]
    # ecosia_site_id = json_extract(json_in_tree_monitoring_species, 'tree_monitoring_instanceid')[0]
    # repeatid_odk = json_extract(json_in_tree_monitoring_species, '__id')[0]
    # tree_species_monitored = json_extract(json_in_tree_monitoring_species, 'tree_monitoring_species')[0]
    # tree_species_number_monitored = json_extract(json_in_tree_monitoring_species, 'tree_monitoring_tree_number_count_save')[0]
    # tree_species_height = json_extract(json_in_tree_monitoring_species, 'tree_monitoring_tree_height')[0]
    #
    #
    # # Populate the table with tree species planted in additional areas
    # cur.execute('''INSERT INTO ODK_unregistered_farmers_tree_monitoring_species(submissionid_odk, ecosia_site_id, repeatid_odk, tree_species_monitored, tree_species_number_monitored, tree_species_height) VALUES (%s,%s,%s,%s,%s)''',
    # (submissionid_odk, ecosia_site_id, repeatid_odk, tree_species_monitored, tree_species_number_monitored, tree_species_height))
    #
    # conn.commit()

# if json_in_tree_monitoring_main['group_monitoring_photos']!= None:
#     monitoring_photo_tree_polygon_1 = json_extract(json_in_tree_monitoring_main, 'monitoring_photo_tree_polygon_1')[0]
#     monitoring_photo_tree_polygon_2 = json_extract(json_in_tree_monitoring_main, 'monitoring_photo_tree_polygon_2')[0]
#     monitoring_photo_tree_polygon_3 = json_extract(json_in_tree_monitoring_main, 'monitoring_photo_tree_polygon_3')[0]
#     monitoring_photo_tree_polygon_4 = json_extract(json_in_tree_monitoring_main, 'monitoring_photo_tree_polygon_4')[0]


client.close()
