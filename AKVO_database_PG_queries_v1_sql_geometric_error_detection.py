import psycopg2
import os

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

drop_tables = '''DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated_self_intersections;'''

conn.commit()

cur.execute('''CREATE INDEX IF NOT EXISTS indexpol ON akvo_tree_registration_areas_updated USING gist(polygon)''')

conn.commit()

#Check reasons for being invalid polygon:
#ST_IsValidReason(geometry)

#Check self-intersections
detect_self_intersections = '''
WITH AKVO_tree_registration_areas_updated_self_intersections AS (SELECT identifier_akvo,

-- Inverse "False" output to "True" because the default setting in the boolean column is "False"
CASE
WHEN ST_IsValid(polygon::geometry) = False
THEN True
ELSE False
END AS self_intersect,

polygon::geometry AS pol
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL AND ST_IsValid(polygon::geometry) = False)

UPDATE akvo_tree_registration_areas_updated
SET self_intersection = AKVO_tree_registration_areas_updated_self_intersections.self_intersect
FROM AKVO_tree_registration_areas_updated_self_intersections
WHERE akvo_tree_registration_areas_updated.identifier_akvo = AKVO_tree_registration_areas_updated_self_intersections.identifier_akvo
;'''

conn.commit()

detect_overlap = '''
WITH akvo_tree_registration_areas_updated_temp_table AS (SELECT identifier_akvo, id_planting_site, organisation, country, ST_MakeValid(polygon::geometry) AS pol
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL),

--Check overlap polygons
akvo_tree_registration_areas_updated_overlap AS (SELECT
a.identifier_akvo, a.id_planting_site, a.organisation, ST_Overlaps(a.pol,c.pol) AS overlap,
ST_INTERSECTION(a.pol, c.pol),
ST_Area(ST_INTERSECTION(a.pol::geography, c.pol::geography))
FROM akvo_tree_registration_areas_updated_temp_table a
INNER JOIN akvo_tree_registration_areas_updated_temp_table c
ON (a.pol && c.pol
AND ST_Overlaps(a.pol,c.pol))
WHERE
a.identifier_akvo != c.identifier_akvo)

UPDATE akvo_tree_registration_areas_updated
SET overlap = akvo_tree_registration_areas_updated_overlap.overlap
FROM akvo_tree_registration_areas_updated_overlap
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_overlap.identifier_akvo;'''

conn.commit()

detect_outside_country = '''
WITH akvo_tree_registration_areas_updated_temp_table AS (SELECT identifier_akvo, id_planting_site, organisation, country, ST_MakeValid(polygon::geometry) AS pol
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL),

--CHECK IF POLYGON IS WITHIN A COUNTRY
akvo_tree_registration_areas_updated_outside_country AS (SELECT a.identifier_akvo,
CASE
WHEN count(*) > 1
THEN True
END AS outside_country
FROM akvo_tree_registration_areas_updated_temp_table a
INNER JOIN "World_Countries" c
ON ST_Overlaps(a.pol, c.geom)
GROUP BY a.identifier_akvo HAVING count(*) > 1)

UPDATE akvo_tree_registration_areas_updated
SET outside_country = akvo_tree_registration_areas_updated_outside_country.outside_country
FROM akvo_tree_registration_areas_updated_outside_country
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_outside_country.identifier_akvo
;'''

conn.commit()

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

conn.commit()

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

conn.commit()

cur.execute(drop_tables)
cur.execute(detect_self_intersections)
cur.execute(detect_overlap)
cur.execute(detect_outside_country)
cur.execute(detect_needle_polygons)
cur.execute(count_total_geometric_errors)

conn.commit()
cur.close()
