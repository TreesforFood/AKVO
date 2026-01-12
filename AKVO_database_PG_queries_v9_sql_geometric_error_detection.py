import psycopg2
import os

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

drop_tables = '''DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated_self_intersections;'''

conn.commit()

cur.execute('''CREATE INDEX IF NOT EXISTS indexpol ON akvo_tree_registration_areas_updated USING gist(polygon)''')

conn.commit()

## Find size of self intersections by an esxplode and then area calculation for all
## AKVO identifiers. Then check the ratio between the smallest and lagest area.
## Then classify then into SMALL and BIG self intersections
### EXPLODE: https://gis.stackexchange.com/questions/454272/geopandas-explode-multi-polygon-holes-filled-inexplicably

detect_self_intersections = '''
-- First clean the overlap column to set repaired overlap polygons to NULL
UPDATE akvo_tree_registration_areas_updated
SET self_intersection = NULL;

WITH AKVO_tree_registration_areas_updated_self_intersections AS (SELECT identifier_akvo,

-- Inverse "False" output to "True" because the default setting in the boolean column is "False"
CASE
WHEN ST_IsValid(polygon::geometry) = False
THEN True
ELSE False
END AS self_intersect,

polygon::geometry AS pol
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL AND ST_IsValid(polygon::geometry) = False
AND total_nr_geometric_errors ISNULL)

UPDATE akvo_tree_registration_areas_updated
SET self_intersection = AKVO_tree_registration_areas_updated_self_intersections.self_intersect
FROM AKVO_tree_registration_areas_updated_self_intersections
WHERE akvo_tree_registration_areas_updated.identifier_akvo = AKVO_tree_registration_areas_updated_self_intersections.identifier_akvo
;'''

conn.commit()

detect_overlap = '''
-- The column 'polygon' needs to be changed from type 'geographic' to type 'geometric' in order to carry out function ST_Equals.
-- From there, a spatial INDEX need to be created on the geometric column(!)
-- Since a spatial INDEX cannot be created on CTE tables, we created TEMPORARY tables (automatically deleted after a Postgres session)
-- Despite TEMPORARY tables are autom. deleted after a session, we forced them to be deleted with a DROP command

DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table1;
DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table2;

-- First clean the overlap column to set repaired overlap polygons to NULL
UPDATE akvo_tree_registration_areas_updated
SET overlap = NULL;

CREATE TEMPORARY TABLE akvo_tree_registration_areas_updated_temp_table1 AS (SELECT identifier_akvo, id_planting_site, organisation, country,
total_nr_geometric_errors, ST_MakeValid(polygon::geometry) AS pol, ST_Transform(polygon::geometry,4326) AS polgeo
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL);
--AND test != 'xxxxx'
--AND test != 'This is a test, this record can be deleted.'
--AND test != 'This is a test, this record can be deleted');

CREATE TEMPORARY TABLE akvo_tree_registration_areas_updated_temp_table2 AS (SELECT identifier_akvo, id_planting_site, organisation, country,
total_nr_geometric_errors, ST_Transform(polgeo::geometry,4326) AS polgeo
FROM akvo_tree_registration_areas_updated_temp_table1
WHERE polgeo NOTNULL AND ST_IsValid(polgeo::geometry));

CREATE INDEX IF NOT EXISTS indexpol ON akvo_tree_registration_areas_updated_temp_table2 USING gist(polgeo);

--Check overlap polygons
WITH akvo_tree_registration_areas_updated_overlap AS (SELECT
a.identifier_akvo AS identifier_akvo_a,
c.identifier_akvo AS identifier_akvo_c,
a.id_planting_site,
a.organisation,
a.total_nr_geometric_errors,
ST_Overlaps(a.polgeo,c.polgeo) AS overlap,
ST_INTERSECTION(a.polgeo, c.polgeo),
ST_Area(ST_INTERSECTION(a.polgeo, c.polgeo))
FROM akvo_tree_registration_areas_updated_temp_table2 a
INNER JOIN akvo_tree_registration_areas_updated_temp_table2 c
ON (a.polgeo && c.polgeo
AND ST_Overlaps(a.polgeo,c.polgeo))
WHERE
a.identifier_akvo != c.identifier_akvo),

--UPDATE akvo_tree_registration_areas_updated
--SET overlap = akvo_tree_registration_areas_updated_overlap.overlap
--FROM akvo_tree_registration_areas_updated_overlap
--WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_overlap.identifier_akvo

-- Transpose all identifiers_akvo from multiple rows into 1 row
transpose_overlap_identifiers AS
(SELECT z.identifier_akvo_a AS identifier_akvo,
STRING_AGG(z.identifier_akvo_c,' | ') overlapping_with_identifiers
FROM akvo_tree_registration_areas_updated_overlap z
GROUP BY z.identifier_akvo_a)

UPDATE akvo_tree_registration_areas_updated
SET overlap = transpose_overlap_identifiers.overlapping_with_identifiers
FROM transpose_overlap_identifiers
WHERE akvo_tree_registration_areas_updated.identifier_akvo = transpose_overlap_identifiers.identifier_akvo;

DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table1;
DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table2;'''

conn.commit()


detect_outside_country = '''
-- First clean the detect column to set repaired overlap polygons to NULL
UPDATE akvo_tree_registration_areas_updated
SET outside_country = NULL;

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
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_outside_country.identifier_akvo;'''

conn.commit()

detect_needle_polygons = '''
-- First clean the overlap column to set repaired overlap polygons to NULL
UPDATE akvo_tree_registration_areas_updated
SET needle_shape = NULL;

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

check_200_trees = '''
-- First clean the check column to set repaired overlap polygons to NULL
UPDATE akvo_tree_registration_areas_updated
SET check_200_trees = NULL;

WITH akvo_tree_registration_areas_updated_check_200_trees AS (
SELECT t.identifier_akvo, t.tree_number, t.polygon,

CASE
WHEN t.tree_number > 200 AND t.polygon ISNULL
THEN True
END AS check_200_trees

FROM akvo_tree_registration_areas_updated AS t)

UPDATE akvo_tree_registration_areas_updated
SET check_200_trees = akvo_tree_registration_areas_updated_check_200_trees.check_200_trees
FROM akvo_tree_registration_areas_updated_check_200_trees
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_check_200_trees.identifier_akvo;'''

conn.commit()


check_duplicate_polygons = '''
-- The column 'polygon' needs to be changed from type 'geographic' to type 'geometric' in order to carry out function ST_Equals.
-- From there, a spatial INDEX need to be created on the geometric column(!)
-- Since a spatial INDEX cannot be created on CTE tables, we created TEMPORARY tables (automatically deleted after a Postgres session)
-- Despite TEMPORARY tables are autom. deleted after a session, we forced them to be deleted with a DROP command

DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table3;
DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table4;

-- First clean the duplicate column to set repaired overlap polygons to NULL
UPDATE akvo_tree_registration_areas_updated
SET check_duplicate_polygons = NULL;

CREATE TEMPORARY TABLE akvo_tree_registration_areas_updated_temp_table3 AS (SELECT identifier_akvo, id_planting_site, organisation, country,
total_nr_geometric_errors, ST_MakeValid(polygon::geometry) AS pol, ST_Transform(polygon::geometry,4326) AS polgeo
FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL);

CREATE TEMPORARY TABLE akvo_tree_registration_areas_updated_temp_table4 AS (SELECT identifier_akvo, id_planting_site, organisation, country,
total_nr_geometric_errors, ST_Transform(polgeo::geometry,4326) AS polgeo
FROM akvo_tree_registration_areas_updated_temp_table3
WHERE polgeo NOTNULL AND ST_IsValid(polgeo::geometry));

CREATE INDEX IF NOT EXISTS indexpol ON akvo_tree_registration_areas_updated_temp_table4 USING gist(polgeo);

WITH duplicate_polygons AS (SELECT
a.identifier_akvo AS identifier_akvo_a,
b.identifier_akvo AS identifier_akvo_b
FROM akvo_tree_registration_areas_updated_temp_table4 a, akvo_tree_registration_areas_updated_temp_table4 b
WHERE ST_EQUALS(a.polgeo, b.polgeo)
AND a.identifier_akvo < b.identifier_akvo),
--AND a.identifier_akvo != b.identifier_akvo),

-- Transpose all identifiers_akvo from multiple rows into 1 row
transpose_duplicate_identifiers AS
(SELECT duplicate_polygons.identifier_akvo_a AS identifier_akvo,
STRING_AGG(duplicate_polygons.identifier_akvo_b,' | ') duplicates
FROM duplicate_polygons
GROUP BY duplicate_polygons.identifier_akvo_a)

UPDATE akvo_tree_registration_areas_updated
SET check_duplicate_polygons = transpose_duplicate_identifiers.duplicates
FROM transpose_duplicate_identifiers
WHERE akvo_tree_registration_areas_updated.identifier_akvo = transpose_duplicate_identifiers.identifier_akvo;

DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table3;
DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_temp_table4;'''

conn.commit()

count_total_geometric_errors_akvo = '''WITH total_errors AS (SELECT identifier_akvo,
CASE
WHEN self_intersection = True
THEN 1
ELSE 0
END

+

CASE
WHEN overlap NOTNULL
THEN 1
ELSE 0
END

+

CASE
WHEN check_200_trees = True
THEN 1
ELSE 0
END

+

CASE
WHEN check_duplicate_polygons NOTNULL
THEN 1
ELSE 0
END

+

CASE
WHEN outside_country = True
THEN 1
ELSE 0
END

+

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

# UPDATE THE SUPERSET TABLE SEPERATLY SINCE WE FIRST CREATE THIS TABLE BY COPYING THE TABLE akvo_tree_registration_areas_updated.
count_total_geometric_errors_superset = '''
UPDATE superset_ecosia_tree_registration
SET
polygon_has_selfintersection = akvo_tree_registration_areas_updated.self_intersection,
polygon_has_overlap_with_other_polygon = akvo_tree_registration_areas_updated.overlap,
polygon_overlaps_country_boundary = akvo_tree_registration_areas_updated.outside_country,
more_200_trees_no_polygon = akvo_tree_registration_areas_updated.check_200_trees,
check_duplicate_polygons = akvo_tree_registration_areas_updated.check_duplicate_polygons,
polygon_is_spatially_distorted = akvo_tree_registration_areas_updated.needle_shape,
total_nr_polygon_errors_found = akvo_tree_registration_areas_updated.total_nr_geometric_errors

FROM akvo_tree_registration_areas_updated
WHERE superset_ecosia_tree_registration.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo;'''

conn.commit()

cur.execute(drop_tables)
cur.execute(detect_self_intersections)
cur.execute(detect_overlap)
cur.execute(detect_outside_country)
cur.execute(detect_needle_polygons)
cur.execute(check_200_trees)
cur.execute(check_duplicate_polygons)
cur.execute(count_total_geometric_errors_akvo)
cur.execute(count_total_geometric_errors_superset)

conn.commit()
cur.close()
