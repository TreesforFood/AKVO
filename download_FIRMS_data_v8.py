import nasa_wildfires
import geopandas as gpd
from math import sqrt
from shapely import wkt
from shapely.geometry import Polygon
from shapely.geometry import Point
import matplotlib.pyplot as plt
import numpy as np
import math
import psycopg2
import os
from sqlalchemy import create_engine

# retrieve specific sensor and specific region data
fires_north_central_africa_suomi = nasa_wildfires.get_viirs_suomi(region="global", time_range="24h")
fires_north_central_africa_noaa = nasa_wildfires.get_viirs_noaa(region="global", time_range="24h")


list_all_aquisitions = []
polygons = []
dates = [] # Date of VIIRS acquisition.
confidences = [] # Confidence values are set to low, nominal, and high
brightness = [] # VIIRS I-4 channel brightness temperature of the fire pixel measured in Kelvin.
satellite = [] # N= Suomi National Polar-orbiting Partnership (Suomi NPP), 1= NOAA-20 (formally JPSS-1), 2=NOAA-21
fire_radiative_power = [] # FRP depicts the pixel-integrated fire radiative power in megawatts (MW).
xcent = [] # Center of nominal 375 m fire pixel.
ycent = [] # Center of nominal 375 m fire pixel.

# More info on the attributes can be found here: https://www.earthdata.nasa.gov/learn/find-data/near-real-time/firms/vnp14imgtdlnrt#ed-viirs-375m-attributes

# Create the tile where the fire has been detected.
# -------------------------------------------------
for json_noaa in fires_north_central_africa_noaa.get('features'):
    list_all_aquisitions.append(json_noaa)

for json_suomi in fires_north_central_africa_suomi.get('features'):
    list_all_aquisitions.append(json_suomi)

for list_all in list_all_aquisitions:
    #print(json_suomi)
    coordinates = list_all['geometry']['coordinates']
    date = list_all['properties']['acq_date']
    confidence = list_all['properties']['confidence']
    brightness_pix_temp_kelvin_channel4 = list_all['properties']['bright_ti4']
    satellite_names = list_all['properties']['satellite']
    if satellite_names == 'N':
        satellite_name = 'Suomi NPP'
    elif satellite_names == '1':
        satellite_name = 'NOAA-20'
    else:
        satellite_name = 'Not defined'
    fire_radiative_power_megawatt = list_all['properties']['frp']
    xcenter = coordinates[0]
    ycenter = coordinates[1]
    xcent.append(xcenter)
    ycent.append(ycenter)

    #-------------------------------
    # How to plot fire footprint area: https://firms.modaps.eosdis.nasa.gov/tutorials/fire-footprint/
    coef = 0.375 # VIIRS has a pixel size of 375 meter
    ycenter_radians = (ycenter/180)*math.pi # The cosinus function of python is using radians so we need to convert degrees to radians
    width = coef/(math.cos(ycenter_radians) * 111.32) # The Earth's circumference is 40,075 km / 360 degrees; so 1 degree ~ 111.32 km
    height = (coef / 111.32 )
    #-------------------------------

    rows = int(np.ceil(ycenter+(0.5*height)))
    cols = int(np.ceil(xcenter+(0.5*width)))
    XleftOrigin = xcenter-(0.5*width)
    XrightOrigin = xcenter+(0.5*width)
    YtopOrigin = ycenter+(0.5*height)
    YbottomOrigin = ycenter-(0.5*height)
    Ytop = YtopOrigin
    Ybottom =YbottomOrigin
    polygons.append(Polygon([(XleftOrigin, Ytop), (XrightOrigin, Ytop), (XrightOrigin, Ybottom), (XleftOrigin, Ybottom)]))
    dates.append(date)
    confidences.append(confidence)
    brightness.append(brightness_pix_temp_kelvin_channel4)
    satellite.append(satellite_name)
    fire_radiative_power.append(fire_radiative_power_megawatt)

columns = {'date': dates, 'confidence': confidences, 'brightness_pix_temp_kelvin_channel4': brightness,
'satellite_name': satellite, 'fire_radiative_power_megawatt': fire_radiative_power,
'ycenter': ycent, 'xcenter': xcent, 'geometry':polygons}
grid = gpd.GeoDataFrame(columns)
grid = grid.to_wkt()


#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS FIRES_FIRM_GLOBAL;''')
conn.commit()

cur.execute('''
CREATE TABLE FIRES_FIRM_GLOBAL (date DATE, confidence_level TEXT, brightness_pix_temp_kelvin_channel4 NUMERIC(10,2),
satellite_name TEXT, fire_radiative_power_megawatt NUMERIC(10,2), xcenter REAL, ycenter REAL, pix_polygon geography(POLYGON, 4326));
''')
conn.commit()

# Populate the fire table. Loop through the rows using iterrows()
for index, row in grid.iterrows():
    #print(row['date'], row['confidence'], row['geometry'])
    cur.execute('''INSERT INTO FIRES_FIRM_GLOBAL (date, confidence_level, brightness_pix_temp_kelvin_channel4,
    satellite_name, fire_radiative_power_megawatt, xcenter, ycenter, pix_polygon)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''', (row['date'], row['confidence'], row['brightness_pix_temp_kelvin_channel4'],
    row['satellite_name'], row['fire_radiative_power_megawatt'],
    row['xcenter'], row['ycenter'], row['geometry']))

conn.commit()

cur.execute('''CREATE INDEX IF NOT EXISTS indexfirepol ON FIRES_FIRM_GLOBAL USING gist(pix_polygon)''')

conn.commit()

cur.execute('''
DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated_new_24h_fires;''')

conn.commit()


cur.execute('''
-- Create a table that spatially joins the registration areas and fire pixels of the last 24h

CREATE TABLE AKVO_tree_registration_areas_updated_new_24h_fires
AS (SELECT
a.identifier_akvo,
a.display_name,
c.partnercode_main,
c.partnercode_sub,
c.organisation_name_main,
c.organisation_name_sub,
c.contract_number,
b.date,
b.confidence_level,
b.brightness_pix_temp_kelvin_channel4,
b.satellite_name,
b.fire_radiative_power_megawatt,
ROUND((ST_Area(ST_Intersection(a.polygon, b.pix_polygon))/ST_Area(b.pix_polygon)*100)::numeric,2) AS perc_firepixel_covered_by_overlap,
b.xcenter,
b.ycenter,
b.pix_polygon

FROM AKVO_tree_registration_areas_updated AS a
INNER JOIN FIRES_FIRM_GLOBAL AS b
ON ST_Intersects(a.polygon::geometry, b.pix_polygon::geometry)
INNER JOIN superset_ecosia_tree_registration AS c
ON c.identifier_akvo = a.identifier_akvo
WHERE a.polygon NOTNULL
AND
a.self_intersection ISNULL
AND
a.needle_shape ISNULL);

-- Add new 24h fires to historic fire table to build up an historic fire database for each planting site
INSERT INTO superset_ecosia_firms_historic_fires
(identifier_akvo, partnercode_main, partnercode_sub, contract_number, date, confidence_level, brightness_pix_temp_kelvin_channel4, satellite_name,
fire_radiative_power_megawatt, perc_firepixel_covered_by_overlap, xcenter, ycenter, fire_pixel)

SELECT identifier_akvo, partnercode_main, partnercode_sub, contract_number, date, confidence_level, brightness_pix_temp_kelvin_channel4, satellite_name,
fire_radiative_power_megawatt, perc_firepixel_covered_by_overlap, xcenter, ycenter, pix_polygon
FROM AKVO_tree_registration_areas_updated_new_24h_fires;


UPDATE superset_ecosia_firms_historic_fires
SET likeliness_of_fire =
CASE
WHEN perc_firepixel_covered_by_overlap < 25
THEN 'fire reported, but not likely inside planting site or with low confidence'
WHEN perc_firepixel_covered_by_overlap BETWEEN 25 AND 50 AND confidence_level <> 'low'
THEN 'fire possible inside planting site'
WHEN perc_firepixel_covered_by_overlap BETWEEN 50 AND 75 AND confidence_level <> 'low'
THEN 'fire very likely inside planting site'
WHEN perc_firepixel_covered_by_overlap > 75 AND confidence_level <> 'low'
THEN 'fire almost definitely inside planting site'
ELSE 'fire reported around or in the planting site, but with low confidence'
END;

UPDATE superset_ecosia_firms_historic_fires AS ta
SET
display_name = tb.display_name,
partnercode_main = tb.partnercode_main,
partnercode_sub = tb.partnercode_sub,
organisation_name_main = tb.organisation_name_main,
organisation_name_sub = tb.organisation_name_sub,

contract_number = tb.contract_number
FROM superset_ecosia_tree_registration AS tb
WHERE ta.identifier_akvo = tb.identifier_akvo;

WITH geojson_table AS (select identifier_akvo, geojson, date, jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'fire_location',
        'geometry',   ST_AsGeoJSON(fire_pixel)::json)))::text AS geojson_fires

FROM superset_ecosia_firms_historic_fires
group by geojson, date, identifier_akvo)

UPDATE superset_ecosia_firms_historic_fires
SET geojson = geojson_table.geojson_fires
from geojson_table
WHERE superset_ecosia_firms_historic_fires.identifier_akvo = geojson_table.identifier_akvo
AND superset_ecosia_firms_historic_fires.date = geojson_table.date
AND superset_ecosia_firms_historic_fires.geojson = geojson_table.geojson;''')

conn.commit()

cur.execute('''DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated_new_24h_fires;''')

conn.commit()

cur.execute('''DROP TABLE IF EXISTS FIRES_FIRM_GLOBAL;''')

conn.commit()

cur.execute('''

GRANT USAGE ON SCHEMA PUBLIC TO ecosia_superset;
GRANT USAGE ON SCHEMA HEROKU_EXT TO ecosia_superset;

GRANT SELECT ON TABLE superset_ecosia_firms_historic_fires TO ecosia_superset;

DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_firms_historic_fires;

ALTER TABLE superset_ecosia_firms_historic_fires enable ROW LEVEL SECURITY;

CREATE POLICY ecosia_superset_policy ON superset_ecosia_firms_historic_fires TO ecosia_superset USING (true);''')

conn.commit()

cur.close()
