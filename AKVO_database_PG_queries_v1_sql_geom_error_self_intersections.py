import psycopg2
import os

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

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
WHERE akvo_tree_registration_areas_updated.identifier_akvo = AKVO_tree_registration_areas_updated_self_intersections.identifier_akvo;

UPDATE superset_ecosia_tree_registration
SET polygon_has_selfintersection = akvo_tree_registration_areas_updated.self_intersection
FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_updated.identifier_akvo = superset_ecosia_tree_registration.identifier_akvo

;'''

conn.commit()
cur.execute(detect_self_intersections)
conn.commit()
cur.close()
