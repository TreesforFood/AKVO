import psycopg2
import os

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

detect_needle_polygons = '''
WITH akvo_tree_registration_areas_updated_temp_table AS (SELECT identifier_akvo, id_planting_site, organisation, country, ST_MakeValid(polygon::geometry) AS pol
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL),

--FIND NEEDLE POLYGONS
akvo_tree_registration_areas_updated_needle_polygons AS (
SELECT t.identifier_akvo, t.country, t.id_planting_site,
CASE
WHEN ST_Length(ST_LongestLine(pol, pol), true) > 5000
THEN True
END AS lenll_m
FROM akvo_tree_registration_areas_updated_temp_table AS t
WHERE
ST_Length(ST_LongestLine(pol, pol), true) > 5000)

UPDATE akvo_tree_registration_areas_updated
SET needle_shape = akvo_tree_registration_areas_updated_needle_polygons.lenll_m
FROM akvo_tree_registration_areas_updated_needle_polygons
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_needle_polygons.identifier_akvo;'''

count_total_geometric_errors = '''WITH total_errors AS (SELECT identifier_akvo,
CASE
WHEN self_intersection = True
THEN 1
ELSE 0
END +

CASE
WHEN overlap = True
THEN 1
ELSE 0
END +

CASE
WHEN outside_country = True
THEN 1
ELSE 0
END +

CASE
WHEN needle_shape = True
THEN 1
ELSE 0
END AS true_count

FROM akvo_tree_registration_areas_updated)

UPDATE akvo_tree_registration_areas_updated
SET total_nr_geometric_errors = total_errors.true_count
FROM total_errors
WHERE akvo_tree_registration_areas_updated.identifier_akvo = total_errors.identifier_akvo;'''

cur.execute(detect_needle_polygons)
conn.commit()
cur.execute(count_total_geometric_errors)
conn.commit()
cur.close()
