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
DROP TABLE IF EXISTS odk_nursery_registration_main;
DROP TABLE IF EXISTS odk_nursery_monitoring_main;''')

conn.commit()

cur.execute('''CREATE TABLE odk_nursery_registration_main (submissionid_odk TEXT, ecosia_nursery_id TEXT, user_name TEXT, organisation TEXT, field_date DATE, submission_date DATE, form_version TEXT, test_data_yes_no TEXT, reporting_type TEXT,
select_tree_production TEXT, contract_number_tree_production NUMERIC(20,2), nursery_registration_gps geography(POINT, 4326), lat_y REAL, lon_x REAL,
nursery_registration_type TEXT, nursery_registration_name TEXT,nursery_registration_establishment TEXT, nursery_registration_full_production_capacity INTEGER, photo_nursery_bed_1 TEXT,
photo_nursery_bed_2 TEXT, photo_nursery_bed_3 TEXT, photo_nursery_bed_4 TEXT, nursery_registration_photo_north TEXT, nursery_registration_photo_east TEXT, nursery_registration_photo_south TEXT,
nursery_registration_photo_west TEXT);

CREATE TABLE odk_nursery_monitoring_main (submissionid_odk TEXT, user_name TEXT, organisation TEXT, field_date DATE, submission_date DATE, name_nursery_monitoring TEXT, form_version TEXT, ecosia_nursery_id TEXT, test_data_yes_no TEXT, reporting_type TEXT,
contract_number_to_monitor NUMERIC(20,2), nursery_monitoring_manager TEXT, nursery_monitoring_gender_manager TEXT, nursery_monitoring_nr_people INTEGER, nursery_monitoring_challenges TEXT,
nursery_monitoring_tree_capacity INTEGER, nursery_monitoring_tree_species TEXT, nursery_monitoring_tree_species_other TEXT, month_first_distribution TEXT, nursery_monitoring_planting_months TEXT,
nursery_bed_gps geography(POINT, 4326), nursery_monitoring_bed_1 TEXT, nursery_monitoring_bed_2 TEXT, nursery_monitoring_bed_3 TEXT, nursery_monitoring_bed_4 TEXT);
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

json_main_nursery_registration = client.submissions.get_table(form_id='nursery_reporting')['value']
#print(json_main_nursery)
json_main_nursery_monitoring = client.submissions.get_table(form_id='nursery_reporting')['value']


# check for audit requests: https://docs.getodk.org/central-api-system-endpoints/#server-audit-logs
audit_report_soft_deleted_submissions = client.get('audits?action=submission.delete', headers={'X-Extended-Metadata': 'true'}).json()
#print(audit_report_soft_deleted_submissions)
audit_report_modified_submissions = client.get('audits?action=submission.update.version', headers={'X-Extended-Metadata': 'true'}).json()
#print(audit_report_modified_submissions)
audit_report_created_submissions = client.get('audits?action=submission.create', headers={'X-Extended-Metadata': 'true'}).json()



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

for json_in in audit_report_created_submissions:
    #print(json_in)
    count = count + 1
    instanceId = json_extract(json_in, 'instanceId')[0]
    createdAt = json_extract(json_in, 'createdAt')
    loggedAt = json_extract(json_in, 'loggedAt')
    #print('CREATED SUBMISSIONS: ', count, instanceId, createdAt, loggedAt)


for json_in in audit_report_modified_submissions:
    acteeId = json_extract(json_in, 'acteeId')[0]
    created_at = json_extract(json_in, 'updatedAt')[0]
    meta_instanceID = json_extract(json_in, 'instanceId')[0] # be carefull: the variable in this json is instanceId NOT instanceID!
    #key = json_extract(json_in, '__id')[0] # bestaat niet in deze json...?
    updated_at = json_extract(json_in, 'loggedAt')[0] # As is seems, the 'loggedAt' key represent the modification date
    #print('UPDATED: ',updatedAt, loggedAt, meta_instanceID)


for json_in in audit_report_soft_deleted_submissions:
    acteeId = json_extract(json_in, 'acteeId')[0]
    updatedAt = json_extract(json_in, 'updatedAt')[0]
    instanceId = json_extract(json_in, 'instanceId')[0] # be carefull: the variable in this json is instanceId NOT instanceID!
    #print('DELETED: ',updatedAt, instanceId)



count = 0
for json_in in json_main_nursery_registration:
    print(json_in)
    if json_extract(json_in, 'reporting_type')[0] == 'new_nursery':
        count = count+1

        user_name = json_extract(json_in, 'username')[0]
        organisation = json_extract(json_in, 'organisation')[0]
        start = json_extract(json_in, 'start')[0]
        end = json_extract(json_in, 'end')[0]
        submission_date = json_extract(json_in, 'submissionDate')[0]
        today = json_extract(json_in, 'today')[0]
        device_id = json_extract(json_in, 'device_id')[0]
        submission_date = json_extract(json_in, 'today')[0]
        form_version = json_extract(json_in, 'formVersion')[0]
        test_data_yes_no = json_extract(json_in, 'test_data_yes_no')[0]
        reporting_type = json_extract(json_in, 'reporting_type')[0]
        submissionid_odk = json_extract(json_in, 'instanceID')[0]
        ecosia_nursery_id = json_extract(json_in, 'instanceid')[0]
        nursery_id_to_monitor = json_extract(json_in, 'nursery_id_to_monitor')[0]
        select_tree_production = json_extract(json_in, 'select_tree_production')[0]
        contract_number_save = json_extract(json_in, 'contract_number_save')[0]
        nursery_registration_type = json_extract(json_in, 'nursery_registration_type')[0]
        nursery_registration_name = json_extract(json_in, 'nursery_registration_name')[0]
        nursery_registration_establishment = json_extract(json_in, 'nursery_registration_establishment')[0]
        nursery_registration_full_production_capacity = json_extract(json_in, 'nursery_registration_full_production_capacity')[0]
        photo_nursery_bed_1 = json_extract(json_in, 'photo_nursery_bed_1')[0]
        photo_nursery_bed_2 = json_extract(json_in, 'photo_nursery_bed_2')[0]
        photo_nursery_bed_3 = json_extract(json_in, 'photo_nursery_bed_3')[0]
        photo_nursery_bed_4 = json_extract(json_in, 'photo_nursery_bed_4')[0]
        nursery_registration_photo_north = json_extract(json_in, 'nursery_registration_photo_north')[0]
        nursery_registration_photo_east = json_extract(json_in, 'nursery_registration_photo_east')[0]
        nursery_registration_photo_south = json_extract(json_in, 'nursery_registration_photo_south')[0]
        nursery_registration_photo_west = json_extract(json_in, 'nursery_registration_photo_west')[0]


        if json_in['group_nursery_registration']['group_nursery_registration_details']['nursery_registration_gps'] != None:
            return_list = convert_point_wkt(json_in['group_nursery_registration']['group_nursery_registration_details']['nursery_registration_gps']['coordinates'])
            nursery_registration_gps = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            nursery_registration_gps = None
            lon_x = None
            lat_y = None

        # Populate the nursery registration table
        cur.execute('''INSERT INTO odk_nursery_registration_main (submissionid_odk, ecosia_nursery_id, user_name, organisation, field_date, submission_date, form_version, test_data_yes_no, reporting_type,
        select_tree_production, contract_number_tree_production, nursery_registration_gps, lat_y, lon_x, nursery_registration_type, nursery_registration_name, nursery_registration_establishment, nursery_registration_full_production_capacity, photo_nursery_bed_1,
        photo_nursery_bed_2, photo_nursery_bed_3, photo_nursery_bed_4, nursery_registration_photo_north, nursery_registration_photo_east, nursery_registration_photo_south,
        nursery_registration_photo_west)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, ecosia_nursery_id, user_name, organisation, today,
        submission_date, form_version, test_data_yes_no, reporting_type, select_tree_production, contract_number_save, nursery_registration_gps, lat_y, lon_x,
        nursery_registration_type, nursery_registration_name, nursery_registration_establishment, nursery_registration_full_production_capacity, photo_nursery_bed_1,
        photo_nursery_bed_2, photo_nursery_bed_3, photo_nursery_bed_4, nursery_registration_photo_north, nursery_registration_photo_east, nursery_registration_photo_south,
        nursery_registration_photo_west))

        conn.commit()




count = 0
for json_in in json_main_nursery_monitoring:
    print(json_in)
    if json_extract(json_in, 'reporting_type')[0] == 'existing_nursery':
        count = count+1
        start = json_extract(json_in, 'start')[0]
        end = json_extract(json_in, 'end')[0]
        submission_date = json_extract(json_in, 'submissionDate')[0]
        today = json_extract(json_in, 'today')[0]
        device_id = json_extract(json_in, 'device_id')[0]
        submission_date = json_extract(json_in, 'today')[0]
        form_version = json_extract(json_in, 'formVersion')[0]
        test_data_yes_no = json_extract(json_in, 'test_data_yes_no')[0]
        user_name = json_extract(json_in, 'username')[0]
        organisation = json_extract(json_in, 'organisation')[0]
        contract_number_to_monitor_save = json_extract(json_in, 'contract_number_to_monitor_save')[0]
        name_nursery_monitoring = json_extract(json_in, 'name_nursery_monitoring_save')[0]
        reporting_type = json_extract(json_in, 'reporting_type')[0]
        submissionid_odk = json_extract(json_in, 'instanceID')[0]
        ecosia_nursery_id = json_extract(json_in, 'instanceid')[0]
        contract_number_to_monitor = json_extract(json_in, 'contract_number_to_monitor_save')[0]
        nursery_id_to_monitor = json_extract(json_in, 'nursery_id_to_monitor_save')[0]
        nursery_monitoring_manager = json_extract(json_in, 'nursery_monitoring_manager')[0]
        nursery_monitoring_gender_manager = json_extract(json_in, 'nursery_monitoring_gender_manager')[0]
        nursery_monitoring_nr_people = json_extract(json_in, 'nursery_monitoring_nr_people')[0]
        nursery_monitoring_challenges = json_extract(json_in, 'nursery_monitoring_challenges')[0]
        nursery_monitoring_tree_capacity = json_extract(json_in, 'nursery_monitoring_tree_capacity')[0]
        nursery_monitoring_tree_species = json_extract(json_in, 'nursery_monitoring_tree_species')[0]
        nursery_monitoring_tree_species_other = json_extract(json_in, 'nursery_monitoring_tree_species_other')[0]
        month_first_distribution = json_extract(json_in, 'month_first_distribution')[0]
        nursery_monitoring_planting_months = json_extract(json_in, 'nursery_monitoring_planting_months')[0]
        nursery_monitoring_bed_1 = json_extract(json_in, 'nursery_monitoring_bed_1')[0]
        nursery_monitoring_bed_2 = json_extract(json_in, 'nursery_monitoring_bed_2')[0]
        nursery_monitoring_bed_3 = json_extract(json_in, 'nursery_monitoring_bed_3')[0]
        nursery_monitoring_bed_4 = json_extract(json_in, 'nursery_monitoring_bed_4')[0]

        if json_in['group_nursery_monitoring']['group_nursery_photos']['nursery_bed_gps'] != None:
            return_list = convert_point_wkt(json_in['group_nursery_monitoring']['group_nursery_photos']['nursery_bed_gps']['coordinates'])
            nursery_bed_gps = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            nursery_bed_gps = None
            lon_x = None
            lat_y = None


        # Populate the nursery monitoring table
        cur.execute('''INSERT INTO odk_nursery_monitoring_main (submissionid_odk, user_name, organisation, field_date, submission_date, ecosia_nursery_id, name_nursery_monitoring, form_version, test_data_yes_no, reporting_type,
        contract_number_to_monitor, nursery_monitoring_manager, nursery_monitoring_gender_manager, nursery_monitoring_nr_people, nursery_monitoring_challenges,
        nursery_monitoring_tree_capacity, nursery_monitoring_tree_species, nursery_monitoring_tree_species_other, month_first_distribution, nursery_monitoring_planting_months,
        nursery_bed_gps, nursery_monitoring_bed_1, nursery_monitoring_bed_2, nursery_monitoring_bed_3, nursery_monitoring_bed_4)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (submissionid_odk, user_name, organisation, today,
        submission_date, nursery_id_to_monitor, name_nursery_monitoring, form_version, test_data_yes_no, reporting_type, contract_number_to_monitor_save, nursery_monitoring_manager, nursery_monitoring_gender_manager, nursery_monitoring_nr_people, nursery_monitoring_challenges,
        nursery_monitoring_tree_capacity, nursery_monitoring_tree_species, nursery_monitoring_tree_species_other, month_first_distribution,
        nursery_monitoring_planting_months, nursery_bed_gps, nursery_monitoring_bed_1, nursery_monitoring_bed_2, nursery_monitoring_bed_3, nursery_monitoring_bed_4))

        conn.commit()

client.close()
