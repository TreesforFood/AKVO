import psycopg2
import os

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

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
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_updated_outside_country.identifier_akvo;

UPDATE superset_ecosia_tree_registration
SET polygon_overlaps_country_boundary = akvo_tree_registration_areas_updated.outside_country
FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_updated.identifier_akvo = superset_ecosia_tree_registration.identifier_akvo;
'''

cur.execute(detect_outside_country)
conn.commit()
cur.close()
