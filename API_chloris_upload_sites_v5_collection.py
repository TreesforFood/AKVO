from chloris_app_sdk import ChlorisAppClient
import requests
import psycopg2
import os
import json
import re

# Chloris refresh token.
refresh_token = os.environ["refresh_token_chloris"]
organisation_id = os.environ["organisation_id_chloris"]
contract_number = input('select the contract number: ') # 106.30


# create the client.
client = ChlorisAppClient(
    organization_id=organisation_id,
    refresh_token=refresh_token, # env: CHLORIS_REFRESH_TOKEN
)


# Connect to Heroku Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

# Execute a query to retrieve the polygon data
cur.execute('''DROP TABLE IF EXISTS CHLORIS_latest_uploads;

CREATE TABLE CHLORIS_latest_uploads AS
WITH CLORIS_uploads AS (SELECT
a.identifier_akvo,
b.identifier_akvo AS identifier_already_submitted_to_chloris,
'uploaded' AS tag_uploaded_chloris,
a.calc_area AS area_ha,
a.organisation,
a.contract_number,
a.id_planting_site,
a.planting_date,
ST_AsGeoJSON(a.polygon) AS str_geojson,
TO_CHAR(NULLIF(a.planting_date, '')::date, 'yyyy') AS planting_year_uploaded

FROM akvo_tree_registration_areas_updated a

LEFT JOIN superset_ecosia_CHLORIS_polygon_results b
ON a.identifier_akvo = b.identifier_akvo

WHERE a.polygon NOTNULL
AND a.total_nr_geometric_errors = 0
AND a.planting_date <> '')

SELECT * FROM CLORIS_uploads
WHERE identifier_already_submitted_to_chloris ISNULL
AND contract_number = %s''', (contract_number,))

conn.commit()


#### DO THE CHORIS ANALYSIS ON 30 METER RESOLUTION FOR SITES LARGER THAN 100HA
cur.execute('''SELECT
identifier_akvo,
organisation,
contract_number,
CONCAT(organisation,' :: ', contract_number,' :-: ', id_planting_site) AS description,
str_geojson
FROM CHLORIS_latest_uploads
WHERE area_ha >= 100
ORDER BY planting_date DESC''')

uploads_more_100ha = cur.fetchall() # output is list of tuples named rows
list_identifiers_30m = []
list_reportingunitid_30m = []

# Generate file paths for each polygon
for submission in uploads_more_100ha:
    identifier_akvo = submission[0]
    organisation = submission[1]
    contract_number = submission[2]
    description_organisation_contract_number_id_planting_site = submission[3]
    polygon_geojson_string = submission[4]

    # Set the resolution variable to 30 meters for sites larger than 100ha
    resolution_30_more_100ha = 30

    # Create a checklist of identfiers to see whether there are some polygons to upload
    list_identifiers_30m.append(identifier_akvo)

    # Convert the string to a Python dictionary
    geojson_data = json.loads(polygon_geojson_string)

    # Specify the output file path
    output_file = "/tmp/polygon.geojson"

    # Write the GeoJSON data to a file
    with open(output_file, 'w') as f:
        json.dump(geojson_data, f, indent=2)


# # Define a writable path for GetODK (/app/tmp is a writable directory on Heroku)
# file_path = "/app/tmp/pyodk_config.ini"
#
# # Create the GetODK directory if it doesn't exist
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
#
# # Write the GetODK configuration to the file
# with open(file_path, "w") as file:
#     file.write(file_content)



    # submit the sites LAGER THAN 100ha to Chloris (30 meter analysis)
    if len(list_identifiers_30m) > 0:
        reporting_unit = client.submit_site(
            label=f"{identifier_akvo}",
            description=f"{description_organisation_contract_number_id_planting_site}",
            tags=["ARR"],
            boundary_path="/tmp/polygon.geojson",
            period_change_start_year=2000,
            period_change_end_year=2025,
            resolution = resolution_30_more_100ha,
            forest_baseline_year=2000,
            #dryrun=True ## ONLY FOR TEST SITES!! USE FALSE FOR REAL DATA
            # optional control site
            # control_boundary_path="path/to/control.geojson",
            # or control_boundary_path="https://example.com/path/to/control.geojson",
            # optionally disable email notifications for this site, recommended if using webhooks
            # notify=False,
            )

    # Create a list of reporting unit ID's that will be the input for the collections
    if len(list_identifiers_30m) > 0:
        reporting_unit_id = reporting_unit['reportingUnitId']
        reporting_unit = client.get_reporting_unit(reporting_unit_id)
        print(reporting_unit)
        list_reportingunitid_30m.append(reporting_unit_id)


try:
    name_organisation = organisation
except NameError:
    name_organisation = 'unknown'

print('30m pixel: ', list_reportingunitid_30m)

# create or update a collection
if len(list_identifiers_30m) > 0:
    collection = client.put_collection(
      collection_entry={
        "organizationId": f"{organisation_id}",
        #"reportingUnitId": "your-collection-reporting-unit-id", # omit to create a new collection, else updates an existing collection
        "label": f"{name_organisation}-30m-nr: {contract_number}",
        "description": f"{name_organisation}-30m-nr: {contract_number}",
        "reportingUnitIds": list_reportingunitid_30m
      }
    )


#### DO THE CHLORIS ANALYSIS ON 10 METER RESOLUTION FOR SITES SMALLER THAN 100HA
cur.execute('''SELECT
identifier_akvo,
organisation,
contract_number,
CONCAT(organisation,' :: ', contract_number,' :-: ', id_planting_site) AS description,
str_geojson
FROM CHLORIS_latest_uploads
WHERE area_ha < 100
ORDER BY planting_date DESC''')

uploads_less_100ha = cur.fetchall() # output is list of tuples named rows
list_identifiers_10m = []
list_reportingunitid_10m = []

# Generate file paths for each polygon
for submission in uploads_less_100ha:
    identifier_akvo = submission[0]
    organisation = submission[1]
    contract_number = submission[2]
    description_organisation_contract_number_id_planting_site = submission[3]
    polygon_geojson_string = submission[4]

    # Set the resolution variable to 10 meters for sites less than 100ha
    resolution_10_less_100ha = 10

    # Create a checklist of identfiers to see whether there are some polygons to upload
    list_identifiers_10m.append(identifier_akvo)

    # Convert the string to a Python dictionary
    geojson_data = json.loads(polygon_geojson_string)

    # Specify the output file path
    output_file = "/tmp/polygon.geojson"

    # Write the GeoJSON data to a file
    with open(output_file, 'w') as file:
        json.dump(geojson_data, file, indent=2)


    # submit the sites SMALLER THAN 100ha to Chloris (10 meter analysis)
    if len(list_identifiers_10m) > 0:
        reporting_unit = client.submit_site(
            label=f"{identifier_akvo}",
            description=f"{description_organisation_contract_number_id_planting_site}",
            tags=["ARR"],
            boundary_path="/tmp/polygon.geojson",
            period_change_start_year=2000,
            period_change_end_year=2025,
            resolution = resolution_10_less_100ha,
            forest_baseline_year=2000,
            #dryrun=True ## ONLY FOR TEST SITES!! USE FALSE FOR REAL DATA
            # optional control site
            # control_boundary_path="path/to/control.geojson",
            # or control_boundary_path="https://example.com/path/to/control.geojson",
            # optionally disable email notifications for this site, recommended if using webhooks
            # notify=False,
            )

    # Create a list of reporting unit ID's that will be the input for the collections
    if len(list_identifiers_10m) > 0:
        reporting_unit_id = reporting_unit['reportingUnitId']
        list_reportingunitid_10m.append(reporting_unit_id)

try:
    name_organisation = organisation
except NameError:
    name_organisation = 'unknown'


print('10m pixel: ', list_reportingunitid_10m)

#create or update a collection
if len(list_identifiers_10m) > 0:
    collection = client.put_collection(
      collection_entry={
        "organizationId": f"{organisation_id}",
        "label": f"{name_organisation}-10m-nr: {contract_number}",
        "description": f"{name_organisation}-10m-nr: {contract_number}",
        "reportingUnitIds": list_reportingunitid_10m
      }
    )
