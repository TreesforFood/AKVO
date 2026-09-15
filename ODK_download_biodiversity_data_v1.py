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
from requests.auth import HTTPBasicAuth
from urllib.parse import quote


# Retrieve environment variables from Heroku
base_url = "https://ecosia.getodk.cloud"
username = os.environ["ODK_CENTRAL_USERNAME"]
password = os.environ["ODK_CENTRAL_PASSWORD"]
form_id = "biodiversity_reporting"
default_project_id = 1
page_size = 5000
auth = HTTPBasicAuth(username, password)
odk_photo_token = os.environ["ODK_photo_token"]


# Connect to the Postgresql database on Heroku
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()


cur.execute('''
DROP TABLE IF EXISTS ODK_biodiversity_main;
DROP TABLE IF EXISTS ODK_biodiversity_species;''')
conn.commit()


#SubmissionDate	start	end	today	device_id	warmup_gps-Latitude	warmup_gps-Longitude	warmup_gps-Altitude	warmup_gps-Accuracy	gps_photo-Latitude	gps_photo-Longitude	gps_photo-Altitude	gps_photo-Accuracy	country	topography	surrounding	meta-audit	meta-instanceID	KEY	SubmitterID	SubmitterName	AttachmentsPresent	AttachmentsExpected	Status	ReviewState	DeviceID	Edits	FormVersion

#photo_species	class_species	exotic_native	PARENT_KEY	KEY

cur.execute('''CREATE TABLE IF NOT EXISTS ODK_biodiversity_main (FID SERIAL PRIMARY KEY, identifier_odk TEXT, submission_date DATE, lat_y REAL, lon_x REAL, country TEXT, topography TEXT, surrounding TEXT, centroid_coord geometry(POINT, 4326));

CREATE TABLE IF NOT EXISTS ODK_biodiversity_species (identifier_akvo TEXT, photo_species TEXT, class_species TEXT, exotic_native TEXT );''')

conn.commit()


"""Class to get ODK submissions in pages"""
class ODKCentralClient:
    def __init__(self, base_url, default_project_id, table_name, username, password, page_size):
        """
        Initialize the client with ODK Central credentials and settings.

        :param base_url: Base URL of the ODK Central server (e.g. https://your-odk-server)
        :param default_project_id: ID of the project to access
        :param username: Username for Basic Auth
        :param password: Password for Basic Auth
        :param page_size: Number of submissions per page (default 200)
        """
        self.base_url = base_url.rstrip('/')
        self.default_project_id = default_project_id
        self.auth = HTTPBasicAuth(username, password)
        self.page_size = page_size
        self.table_name = table_name

    def _build_endpoint(self, form_id):
        """
        Build the OData submissions endpoint URL with page size limit.

        :param form_id: The form ID (not form name)
        :return: Full URL string
        """
        return (f"{self.base_url}/v1/projects/{self.default_project_id}/forms/"
                f"{form_id}.svc/{self.table_name}?$top={self.page_size}")

    def get_all_submissions(self, form_id, process_page_callback):
        """
        Fetch all submissions for a form, handling pagination, and process each page immediately.

        :param form_id: The form ID to fetch submissions from
        :param process_page_callback: A callable that takes a list of submissions (one page)
                                      and processes them (e.g., saves to DB, writes to file)
        """
        endpoint = self._build_endpoint(form_id)
        page_number = 1

        while endpoint:
            response = requests.get(endpoint, auth=self.auth)
            if response.status_code == 200:
                data = response.json()
                current_page_submissions = data.get('value', [])
                print(f"Processing page {page_number} with {len(current_page_submissions)} submissions.")

                # Process the current page immediately
                process_page_callback(current_page_submissions)

                # Get next page link
                endpoint = data.get('@odata.nextLink')
                if endpoint and not endpoint.startswith('http'):
                    endpoint = f"{self.base_url}{endpoint}"

                page_number += 1 if endpoint else 0
            else:
                raise Exception(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")

        print("All pages processed.")



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

print('start phase 6: start script with harvesting loop 1')

def extract_keys_once(obj, keys):
    """Extract multiple keys in one pass from nested JSON."""
    results = {key: [] for key in keys}

    def extract(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in keys:
                    results[k].append(v)
                if isinstance(v, (dict, list)):
                    extract(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item)

    extract(obj)
    # For keys expected to have a single value, get first or None
    return {k: (v[0] if v else None) for k, v in results.items()}


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



# set the variable table to genernal submissions
table_name = "Submissions"

def process_page(json_registration):
    for json_in in json_registration:
        print(json_in)
        submissionid_odk = json_extract(json_in, 'instanceID')[0]
        #print(submissionid_odk)

        start = json_extract(json_in, 'start')[0]
        end = json_extract(json_in, 'end')[0]
        updated_at = json_extract(json_in, 'updatedAt')[0]
        submission_date = json_extract(json_in, 'submissionDate')[0]
        today = json_extract(json_in, 'today')[0]

        identifier_odk = json_extract(json_in, 'instanceID')[0]
        country = json_extract(json_in, 'country')[0]
        topography = json_extract(json_in, 'topography')[0]
        surrounding = json_extract(json_in, 'surrounding')[0]
        #form_version = json_extract(json_in, 'form_version')[0]


        if json_in['gps_photo'] != None:
            return_list = convert_point_wkt(json_in['gps_photo']['coordinates'])
            centroid_coord = return_list[0]
            lon_x = return_list[1]
            lat_y = return_list[2]
        else:
            centroid_coord = None
            lon_x = None
            lat_y = None

        cur.execute('''INSERT INTO ODK_biodiversity_main (identifier_odk, submission_date, lat_y, lon_x, country, topography, surrounding, centroid_coord)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_odk, submission_date, lat_y, lon_x, country, topography, surrounding, centroid_coord))

        conn.commit()


# call the submissions
print('processing the main data...')
client = ODKCentralClient(base_url, default_project_id, table_name, username, password, page_size)
json_registration = client.get_all_submissions(form_id, process_page_callback = process_page)


table_name = "Submissions.group_main_entrance.repeat_photos"

def process_page(json_species):
    count = 0  # You can make this globa

    for json_species_repeat in json_species:
        #print(json_in_tree_species)
        identifier_akvo = json_extract(json_species_repeat, '__Submissions-id')[0]
        species_name_latin = json_extract(json_species_repeat, 'calculate_species_position')[0]
        nr_trees_per_species = json_extract(json_species_repeat, 'nr_trees_per_species_registered')[0]
        photo_species = json_extract(json_species_repeat, 'photo_species')[0]
        class_species = json_extract(json_species_repeat, 'class_species')[0]
        exotic_native = json_extract(json_species_repeat, 'exotic_native')[0]


        # Create a temp CTE table to download all main registration data from ODK
        cur.execute('''INSERT INTO ODK_biodiversity_species (identifier_akvo, photo_species, class_species, exotic_native)
        VALUES (%s,%s,%s,%s)''', (identifier_akvo, photo_species, class_species, exotic_native))

        conn.commit()



conn.close()
