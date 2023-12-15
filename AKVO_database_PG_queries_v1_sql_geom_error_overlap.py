import psycopg2
import os

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

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

cur.execute(detect_overlap)
conn.commit()
cur.close()
