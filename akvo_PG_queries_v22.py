import requests
import json
import re
import geojson
import geodaisy.converters as convert
from geopy.distance import geodesic
from area import area
import psycopg2
import os
import cloudpickle

#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')
cur = conn.cursor()

drop_tables = '''DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated;
DROP TABLE IF EXISTS CALC_TAB_monitoring_calculations_per_site_by_partner;

DROP TABLE IF EXISTS CALC_TAB_Error_check_on_site_registration;
DROP TABLE IF EXISTS CALC_TAB_Error_partner_report_on_site_registration;
DROP TABLE IF EXISTS CALC_GEOM_pcq_calculations_per_site_by_external_audit;
DROP TABLE IF EXISTS CALC_GEOM_Trees_counted_per_site_by_external_audit;
DROP TABLE IF EXISTS CALC_GEOM_AKVO_tree_registration_submissions_today;
DROP TABLE IF EXISTS CALC_GEOM_AKVO_nursery_registration_submissions_today;
DROP TABLE IF EXISTS CALC_GEOM_AKVO_tree_registration_submissions_yesterday;
DROP TABLE IF EXISTS CALC_GEOM_AKVO_nursery_registration_submissions_yesterday;
DROP TABLE IF EXISTS CALC_GEOM_AKVO_check_photo_registrations;
DROP TABLE IF EXISTS CALC_GEOM_AKVO_check_species_registrations;
DROP TABLE IF EXISTS CALC_GEOM_locations_registration_versus_externalaudits;
DROP TABLE IF EXISTS CALC_GEOM_Trees_counted_per_site_by_partner;
DROP TABLE IF EXISTS CALC_TAB_overall_statistics;
DROP TABLE IF EXISTS CALC_TAB_tree_submissions_per_contract;
DROP TABLE IF EXISTS CALC_TAB_Error_check_on_nursery_registration;
DROP TABLE IF EXISTS CALC_TAB_Error_partner_report_on_nursery_registration;
DROP TABLE IF EXISTS akvo_ecosia_contract_overview;
DROP TABLE IF EXISTS error_partner_report_on_nursery_registration;
DROP TABLE IF EXISTS error_partner_report_on_site_registration;
DROP TABLE IF EXISTS akvo_ecosia_nursery_monitoring;
DROP TABLE IF EXISTS akvo_ecosia_nursery_registration;
DROP TABLE IF EXISTS akvo_ecosia_tree_area_monitoring;
DROP TABLE IF EXISTS akvo_ecosia_tree_area_registration;
DROP TABLE IF EXISTS akvo_ecosia_tree_photo_registration;
DROP TABLE IF EXISTS s4g_ecosia_site_health;
DROP TABLE IF EXISTS s4g_ecosia_fires;
DROP TABLE IF EXISTS s4g_ecosia_deforestation;
DROP TABLE IF EXISTS s4g_ecosia_landuse_cover;
DROP TABLE IF EXISTS superset_ecosia_nursery_registration;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_polygon;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring;
DROP TABLE IF EXISTS s4g_ecosia_data_quality;
DROP TABLE IF EXISTS superset_ecosia_s4g_site_health;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_point;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_species;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_polygon;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring;
DROP TABLE IF EXISTS superset_ecosia_nursery_registration_pictures;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_pictures;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_pictures;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_species;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_species;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_pictures;
DROP TABLE IF EXISTS superset_ecosia_s4g_fires;
DROP TABLE IF EXISTS superset_ecosia_s4g_deforestation;
DROP TABLE IF EXISTS superset_ecosia_tree_registration;'''

conn.commit()

create_a1 = '''CREATE TABLE akvo_tree_registration_areas_updated
AS TABLE akvo_tree_registration_areas;

ALTER TABLE akvo_tree_registration_areas_updated
ADD species_latin text;

WITH t as
(SELECT akvo_tree_registration_species.identifier_akvo,
	STRING_AGG(akvo_tree_registration_species.lat_name_species,' | ')
 species_list
	  FROM akvo_tree_registration_species
 JOIN akvo_tree_registration_areas_updated
 ON akvo_tree_registration_species.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
 GROUP BY akvo_tree_registration_species.identifier_akvo)

UPDATE akvo_tree_registration_areas_updated
SET species_latin = t.species_list
FROM t
WHERE t.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo;

UPDATE akvo_tree_registration_areas_updated
SET
polygon = akvo_tree_monitoring_remapped_areas.polygon_remapped
FROM akvo_tree_monitoring_remapped_areas
WHERE akvo_tree_registration_areas_updated.identifier_akvo
= akvo_tree_monitoring_remapped_areas.identifier_akvo
AND akvo_tree_monitoring_remapped_areas.polygon_remapped NOTNULL;'''

conn.commit()


# Works well
create_a2 = '''
--THIS IS THE MAIN QUERY TABLE WITH A SELF JOIN IN ORDER TO GROUP
CREATE TABLE CALC_TAB_monitoring_calculations_per_site_by_partner AS

-- Create CTE with aggregation on PCQ instances. This is important to get 1 single label strata per instance
WITH pcq_instances_grouped AS (SELECT
akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_monitoring_areas.submission
FROM AKVO_Tree_monitoring_areas
LEFT JOIN akvo_tree_monitoring_pcq ON AKVO_Tree_monitoring_areas.instance = akvo_tree_monitoring_pcq.instance
JOIN AKVO_Tree_registration_areas ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
WHERE (AKVO_Tree_monitoring_areas.test = '' OR AKVO_Tree_monitoring_areas.test = 'This is real, valid data')),

-- Create CTE with COUNT instances. These should be the count repeat instances in GROUP 2, which are the counts per species (table 'akvo_tree_monitoring_counts').
-- IF these values don't exists, the query should use the tree number in the main table (akvo_tree_monitoring_areas)
count_instances_grouped AS (SELECT
akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_monitoring_areas.submission
FROM AKVO_Tree_monitoring_areas
LEFT JOIN akvo_tree_monitoring_counts ON AKVO_Tree_monitoring_areas.instance = akvo_tree_monitoring_counts.instance
JOIN AKVO_Tree_registration_areas ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
WHERE (AKVO_Tree_monitoring_areas.test = '' OR AKVO_Tree_monitoring_areas.test = 'This is real, valid data')),


-- Create CTE with REGISTRATION instances and add them to the table so that the registration submission date is also included
registration_instances_grouped AS (SELECT
akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.instance,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.submission
FROM akvo_tree_registration_areas
JOIN AKVO_Tree_monitoring_areas ON AKVO_Tree_monitoring_areas.identifier_akvo = akvo_tree_registration_areas.identifier_akvo
WHERE (akvo_tree_registration_areas.test = '' OR AKVO_Tree_monitoring_areas.test = 'This is real, valid data')),


--UNION OF COUNTS and PCQ's and REGISTRATION into 1 table. Instances are grouped. This table enters the calculate to add strata (= days distance between instances) to instances
monitoring_instances_grouped AS
(SELECT * FROM pcq_instances_grouped
UNION
SELECT * FROM count_instances_grouped
UNION
SELECT * FROM registration_instances_grouped),


-- Create CTE to calculate relative distances between submission dates/instances and give each (grouped instance) a strata label
-- At this point, different instances uploaded only a few days or months after another, get the same strate label, meaning that
-- these instances still belong to the same monitoring period (if uploaded within 180 days).
table_label_strata AS (SELECT monitoring_instances_grouped.identifier_akvo, monitoring_instances_grouped.instance,
monitoring_instances_grouped.submission,
CASE
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  <180 THEN 180
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  ISNULL THEN 180
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 181 AND 360 THEN 360
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 361 AND 540 THEN 540
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 541 AND 720 THEN 720
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 721 AND 900 THEN 900
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 901 AND 1080 THEN 1080
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 1081 AND 1260 THEN 1260
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 1261 AND 1440 THEN 1440
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 1441 AND 1620 THEN 1620
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 1621 AND 1800 THEN 1800
WHEN (monitoring_instances_grouped.submission-LAG(monitoring_instances_grouped.submission)
OVER (PARTITION BY monitoring_instances_grouped.identifier_akvo ORDER BY monitoring_instances_grouped.submission ASC))
	  Between 1801 AND 1980 THEN 1980
ELSE 1111111111111
END AS monitoring_strata
FROM monitoring_instances_grouped),

-- Sub CTE table to calculate PCQ results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_pcq AS (SELECT
AKVO_Tree_monitoring_pcq.identifier_akvo,
AKVO_Tree_registration_areas.calc_area,
akvo_tree_registration_areas.tree_number,
ROUND(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2) as avg_tree_distance,
ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0))*10000) as avg_tree_density,
ROUND((((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0))*10000)*AKVO_Tree_registration_areas.calc_area),0) as nr_trees_pcq_monitored,
count(akvo_tree_monitoring_pcq.instance) AS nr_samples_pcq,
table_label_strata.submission AS latest_submission,
table_label_strata.monitoring_strata,

CASE
--WHEN AKVO_Tree_monitoring_areas.identifier_akvo = AKVO_Tree_monitoring_pcq.identifier_akvo
WHEN AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
THEN ROUND((ROUND((((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0))*10000)*AKVO_Tree_registration_areas.calc_area)/NULLIF(akvo_tree_registration_areas.tree_number,0),2)*100),0)
ELSE ROUND(((SUM(AKVO_Tree_monitoring_areas.number_living_trees)/NULLIF(akvo_tree_registration_areas.tree_number,0))*100),2)
END AS perc_trees_survived,

CASE
--WHEN AKVO_Tree_monitoring_areas.identifier_akvo = AKVO_Tree_monitoring_pcq.identifier_akvo
WHEN AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
THEN ROUND(((AVG(Q1_hgt) + AVG(Q2_hgt) + AVG(Q3_hgt) + AVG(Q4_hgt))/4),2)
ELSE akvo_tree_monitoring_areas.avg_tree_height
END AS avg_tree_height

FROM AKVO_Tree_monitoring_areas
LEFT JOIN AKVO_Tree_monitoring_pcq
ON AKVO_Tree_monitoring_areas.instance = AKVO_Tree_monitoring_pcq.instance
LEFT JOIN AKVO_Tree_registration_areas
ON AKVO_Tree_monitoring_areas.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance

GROUP BY
table_label_strata.monitoring_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_pcq.identifier_akvo,
AKVO_Tree_registration_areas.calc_area,
akvo_tree_registration_areas.tree_number,
akvo_tree_monitoring_areas.avg_tree_height,
table_label_strata.submission,
AKVO_Tree_monitoring_areas.method_selection),


-- Sub CTE table to calculate COUNTS results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_count AS (SELECT
--AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_counts.identifier_akvo,
AKVO_Tree_registration_areas.calc_area,
akvo_tree_registration_areas.tree_number as registered_tree_number,
table_label_strata.monitoring_strata,
table_label_strata.submission AS latest_submission,

CASE
--WHEN AKVO_Tree_monitoring_areas.identifier_akvo = AKVO_Tree_monitoring_counts.identifier_akvo
WHEN AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
THEN SUM(AKVO_Tree_monitoring_counts.number_species)
ELSE SUM(AKVO_Tree_monitoring_areas.number_living_trees)
END AS monitored_tree_number,

CASE
--WHEN AKVO_Tree_monitoring_areas.identifier_akvo = AKVO_Tree_monitoring_counts.identifier_akvo
WHEN AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
THEN CAST(SUM(AKVO_Tree_monitoring_counts.number_species*1.0)/NULLIF(akvo_tree_registration_areas.tree_number*1.0,0)*100 AS NUMERIC)
ELSE SUM(AKVO_Tree_monitoring_areas.number_living_trees)/NULLIF(akvo_tree_registration_areas.tree_number,0)*100
END AS perc_trees_survived,

akvo_tree_monitoring_areas.avg_tree_height AS avg_tree_height

FROM AKVO_Tree_monitoring_areas
LEFT JOIN AKVO_Tree_monitoring_counts
ON AKVO_Tree_monitoring_areas.instance = AKVO_Tree_monitoring_counts.instance
LEFT JOIN AKVO_Tree_registration_areas
ON AKVO_Tree_monitoring_areas.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance

GROUP BY
table_label_strata.monitoring_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_counts.identifier_akvo,
AKVO_Tree_registration_areas.calc_area,
akvo_tree_registration_areas.tree_number,
akvo_tree_monitoring_areas.avg_tree_height,
table_label_strata.submission,
AKVO_Tree_monitoring_areas.method_selection),

-- Calculate the monitoring results from the PCQ's per label strata (and instance)
monitoring_tree_numbers_pcq AS
(SELECT calc_interm_results_tree_numbers_pcq.identifier_akvo,
calc_interm_results_tree_numbers_pcq.monitoring_strata,
'PCQ' as monitoring_method,
SUM(calc_interm_results_tree_numbers_pcq.nr_samples_pcq) as nr_samples_pcq,
SUM(calc_interm_results_tree_numbers_pcq.nr_trees_pcq_monitored) AS monitored_tree_number,
ROUND(AVG(calc_interm_results_tree_numbers_pcq.avg_tree_distance),2) as avg_tree_distance,
ROUND(AVG(calc_interm_results_tree_numbers_pcq.avg_tree_density),2) as avg_tree_density,
ROUND(CAST(AVG(calc_interm_results_tree_numbers_pcq.avg_tree_height) AS NUMERIC),2) as average_tree_height,

ROUND(AVG(calc_interm_results_tree_numbers_pcq.perc_trees_survived),0) as avg_perc_survived_trees,

MAX(calc_interm_results_tree_numbers_pcq.latest_submission) AS submission

FROM calc_interm_results_tree_numbers_pcq

GROUP BY calc_interm_results_tree_numbers_pcq.identifier_akvo,
calc_interm_results_tree_numbers_pcq.monitoring_strata,
calc_interm_results_tree_numbers_pcq.tree_number),

-- Calculate the monitoring results from the COUNTS per label strata
monitoring_tree_numbers_counts AS (SELECT
calc_interm_results_tree_numbers_count.identifier_akvo,
calc_interm_results_tree_numbers_count.monitoring_strata,
'tree count' as monitoring_method,
0 AS nr_samples_pcq,
ROUND(AVG(calc_interm_results_tree_numbers_count.monitored_tree_number),0) as monitored_tree_number,
--calc_interm_results_tree_numbers_count.monitored_tree_number as monitored_tree_number,
0 AS avg_tree_distance,
0 AS avg_tree_density,
SUM(calc_interm_results_tree_numbers_count.avg_tree_height) AS average_tree_height,
ROUND(AVG(calc_interm_results_tree_numbers_count.perc_trees_survived),0) as avg_perc_survived_trees,
MAX(calc_interm_results_tree_numbers_count.latest_submission) AS submission

FROM calc_interm_results_tree_numbers_count

GROUP BY calc_interm_results_tree_numbers_count.identifier_akvo,
calc_interm_results_tree_numbers_count.monitoring_strata,
calc_interm_results_tree_numbers_count.registered_tree_number),

--UNION of the two monitoring tables (PCQ and COUNTS)
monitoring_tree_numbers AS
(SELECT * FROM monitoring_tree_numbers_pcq
UNION
SELECT * FROM monitoring_tree_numbers_counts)


-- Here is the creation of the table and that combines all data
-- First query unifies all results from the monitorings (both PCQ and COUNTS)
SELECT
monitoring_tree_numbers.identifier_akvo,
monitoring_tree_numbers.monitoring_strata,
monitoring_tree_numbers.monitoring_method as method_data_collection,
monitoring_tree_numbers.submission,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.organisation,
monitoring_tree_numbers.nr_samples_pcq AS number_of_PCQ_samples,
akvo_tree_registration_areas.calc_area AS "calcul/estim Area (ha)",
monitoring_tree_numbers.avg_tree_distance AS "Average tree distance (m)",
monitoring_tree_numbers.average_tree_height AS "Average tree height (m)",
monitoring_tree_numbers.avg_tree_density AS "Tree density (trees/ha)",
monitoring_tree_numbers.monitored_tree_number AS "Total nr trees on site (registered/monitored)",
monitoring_tree_numbers.avg_perc_survived_trees AS "Percentage of survived trees"

FROM monitoring_tree_numbers
JOIN akvo_tree_registration_areas
ON monitoring_tree_numbers.identifier_akvo = akvo_tree_registration_areas.identifier_akvo

UNION

-- Add the POLYGON results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial tree number). Only for polygons
SELECT
akvo_tree_registration_areas.identifier_akvo,
0 AS monitoring_strata,
'tree registration' as method_data_collection,
akvo_tree_registration_areas.submission,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.organisation,
0 as number_of_PCQ_samples,
akvo_tree_registration_areas.calc_area as "calcul/estim Area (ha)",
ROUND(100/NULLIF(SQRT(akvo_tree_registration_areas.tree_number/NULLIF(akvo_tree_registration_areas.calc_area,0)),0),2)
AS "Average tree distance (m)",
0 AS "Average tree height (m)",
ROUND((akvo_tree_registration_areas.tree_number/NULLIF(akvo_tree_registration_areas.calc_area,0)),0)
AS "Tree density (trees/ha)",
akvo_tree_registration_areas.tree_number AS "Total nr trees on site (registered/monitored)",
100 AS "Percentage of trees survived"

FROM akvo_tree_registration_areas
WHERE polygon NOTNULL

UNION

-- Add the NON-polygon results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial tree number). Only for NON-polygons
SELECT
akvo_tree_registration_areas.identifier_akvo,
0 AS monitoring_strata,
'tree registration' as method_data_collection,
akvo_tree_registration_areas.submission,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.organisation,
0 as number_of_PCQ_samples,
akvo_tree_registration_areas.estimated_area as "calcul/estim Area (ha)",
0 AS "Average tree distance (m)",
0 AS "Average tree height (m)",
0 AS "Tree density (trees/ha)",
akvo_tree_registration_areas.tree_number AS "Total nr trees on site (registered/monitored)",
100 AS "Percentage of trees survived"
FROM akvo_tree_registration_areas
WHERE polygon ISNULL
ORDER BY contract_number, id_planting_site, monitoring_strata;'''

conn.commit()

create_a3 = '''UPDATE akvo_tree_registration_photos
SET photo_url = RIGHT(photo_url, strpos(reverse(photo_url),'/'));

UPDATE akvo_tree_registration_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images',photo_url);'''

conn.commit()

create_a4 = '''
CREATE TABLE CALC_TAB_Error_check_on_site_registration
AS

WITH COUNT_Total_number_of_photos_taken AS (SELECT AKVO_Tree_registration_photos.identifier_akvo, COUNT(*)
FROM AKVO_Tree_registration_photos GROUP BY AKVO_Tree_registration_photos.identifier_akvo),

COUNT_Number_of_photos_with_geotag AS (SELECT identifier_akvo, COUNT(*) FROM AKVO_Tree_registration_photos
									   WHERE AKVO_Tree_registration_photos.photo_geotag_location NOTNULL
GROUP BY AKVO_Tree_registration_photos.identifier_akvo),

COUNT_Number_of_tree_species_registered AS (SELECT identifier_akvo, COUNT(*) FROM AKVO_Tree_registration_species
GROUP BY AKVO_Tree_registration_species.identifier_akvo),

COUNT_Number_of_tree_species_with_0_number AS (SELECT identifier_akvo, COUNT(*) FROM AKVO_Tree_registration_species
WHERE AKVO_Tree_registration_species.number_species = 0
GROUP BY AKVO_Tree_registration_species.identifier_akvo),

LABEL_type_geometry AS (SELECT AKVO_Tree_registration_areas.identifier_akvo, CASE
WHEN AKVO_Tree_registration_areas.polygon NOTNULL
THEN 'polygon'
ELSE 'centroid point'
END AS Geometric_feature
FROM AKVO_Tree_registration_areas)

SELECT
AKVO_Tree_registration_areas.identifier_akvo,
AKVO_Tree_registration_areas.instance,
AKVO_Tree_registration_areas.submitter AS "Submitter of registration",
AKVO_Tree_registration_areas.akvo_form_version AS "Form version used for registration",
AKVO_Tree_registration_areas.submission AS "Submission Date",
AKVO_Tree_registration_areas.organisation As name_organisation,
AKVO_Tree_registration_areas.country AS "Country",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.tree_number AS "Registered tree number",
AKVO_Tree_registration_areas.estimated_area AS "Estimated area (ha)",
calc_area AS "GIS calculated area (ha)",

COUNT_Total_number_of_photos_taken.count AS Total_number_of_photos_taken,

COUNT_Number_of_photos_with_geotag.count AS Number_of_photos_with_geotag,

COUNT_Number_of_tree_species_registered.count AS Number_of_tree_species_registered,

COUNT_Number_of_tree_species_with_0_number.count AS Number_of_tree_species_with_no_tree_number_indication,

LABEL_type_geometry.Geometric_feature,

ROUND(COUNT_Total_number_of_photos_taken.count/NULLIF(AKVO_Tree_registration_areas.estimated_area,0),2) AS "Photo density (photos/ha)",

number_coord_polygon AS "Number of points in registered polygon",

ROUND(AKVO_Tree_registration_areas.tree_number/NULLIF(AKVO_Tree_registration_areas.calc_area,0),0) AS "Registered tree density (trees/ha)",

AKVO_Tree_registration_areas.centroid_coord

FROM AKVO_Tree_registration_areas
LEFT JOIN LABEL_type_geometry
ON AKVO_Tree_registration_areas.identifier_akvo = LABEL_type_geometry.identifier_akvo
LEFT JOIN COUNT_Total_number_of_photos_taken
ON AKVO_Tree_registration_areas.identifier_akvo = COUNT_Total_number_of_photos_taken.identifier_akvo
LEFT JOIN COUNT_Number_of_photos_with_geotag
ON COUNT_Number_of_photos_with_geotag.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo
LEFT JOIN COUNT_Number_of_tree_species_registered
ON COUNT_Number_of_tree_species_registered.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo
LEFT JOIN COUNT_Number_of_tree_species_with_0_number
ON COUNT_Number_of_tree_species_with_0_number.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo

WHERE (AKVO_Tree_registration_areas.test = '' OR AKVO_Tree_registration_areas.test = 'This is real, valid data')

GROUP BY AKVO_Tree_registration_areas.identifier_akvo, akvo_tree_registration_areas.instance,
akvo_tree_registration_areas.submitter, akvo_tree_registration_areas.submission, akvo_tree_registration_areas.organisation, akvo_tree_registration_areas.country,
akvo_tree_registration_areas.id_planting_site, akvo_tree_registration_areas.contract_number, akvo_tree_registration_areas.estimated_area, akvo_tree_registration_areas.calc_area,
akvo_tree_registration_areas.number_coord_polygon, akvo_tree_registration_areas.tree_number,
Geometric_feature, COUNT_Total_number_of_photos_taken.count,COUNT_Number_of_photos_with_geotag.count,
COUNT_Number_of_tree_species_registered.count, COUNT_Number_of_tree_species_with_0_number.count,
AKVO_Tree_registration_areas.akvo_form_version,
AKVO_Tree_registration_areas.centroid_coord
ORDER BY AKVO_Tree_registration_areas.submission desc;'''

conn.commit()

create_a5 = '''CREATE TABLE CALC_TAB_Error_partner_report_on_site_registration AS
SELECT *,

CASE
WHEN Total_number_of_photos_taken < 16 and "Form version used for registration" >= 127
THEN 'Form version >= 127 used. Too few photos taken. At least 16 photos per site are needed'
WHEN "Photo density (photos/ha)" < 16 and "Form version used for registration" >= 127
THEN 'Form version >= 127 used. Too few photos taken. Should be at least 16 for each hectare'
WHEN "Photo density (photos/ha)" >= 16 and "Form version used for registration" >= 127
THEN 'Form version >= 127 used. Enough photos have been taken for this site'
WHEN Total_number_of_photos_taken < 4 and "Form version used for registration" < 127
THEN 'Form version < 127 used. Too few photos taken. Should be at least 4'
WHEN "Photo density (photos/ha)" < 1 AND "Estimated area (ha)" < 35 and "Form version used for registration" < 127
THEN 'Form version < 127 used. Too few photos taken. Should be at least 1 per hectare'
WHEN "Photo density (photos/ha)" < 1 AND "Estimated area (ha)" > 35 AND Total_number_of_photos_taken < 35 and "Form version used for registration" < 127
THEN 'Form version < 127 used. Too few photos taken. At least 35 photos should have been taken'
WHEN "Photo density (photos/ha)" ISNULL
THEN 'Unclear if enough photos are taken since area is not indicated. However, should at least be 16'
ELSE 'Sufficient nr of photos have been taken for this site'
END AS "Check nr of photos",

CASE
WHEN Total_number_of_photos_taken > Number_of_photos_with_geotag
THEN 'Not all photos have a geotag'
WHEN Number_of_photos_with_geotag ISNULL
THEN 'None of the photos have a geotag'
ELSE 'All photos have a geotag'
END AS "Check geotag of photos",

CASE
WHEN "Registered tree number" > 200 and Geometric_feature = 'polygon'
THEN 'Site has more than 200 trees and was mapped with a polygon. This is correct'
WHEN "Registered tree number" <= 200 and Geometric_feature = 'centroid point'
THEN 'Site has less than 200 trees and was mapped with a centroid point. This is correct'
WHEN "Registered tree number" > 200 and Geometric_feature = 'centroid point'
THEN 'Site has more than 200 trees and was mapped with a centroid point. This is not correct. Site should have been mapped with a polygon'
END AS "Check mapping geometric feature",

CASE
WHEN Geometric_feature = 'polygon'
AND "Number of points in registered polygon" < 4 AND "Number of points in registered polygon" > 0
THEN 'Polygons does not have enough points. Should be 4 at minimum but, preferably at least 8'
WHEN Geometric_feature = 'centroid point'
THEN 'Not applicable: The location of the site was mapped with a point, not a polygon'
ELSE 'Sufficient nr of points (>=4) have been taken to map the area'
END AS "Check nr of points in polygon",

CASE
WHEN "Registered tree density (trees/ha)" > 2000
THEN 'Registered tree density seems high (>2000 trees/ha). Confirm if this is correct'
WHEN "Registered tree density (trees/ha)" < 10
THEN 'Registered tree density seems very low (< 10 trees/ha). Confirm if this is correct'
ELSE 'Tree density seems within reasonable limits (>10 and <2000 trees/ha)'
END AS "Check on tree density"

FROM CALC_TAB_Error_check_on_site_registration
ORDER BY "Submission Date" desc;'''

conn.commit()


create_a6 = '''CREATE TABLE CALC_GEOM_PCQ_calculations_per_site_by_external_audit
AS
WITH species_audited AS (SELECT list.identifier_akvo,  COUNT(*) AS "Number of species audited", STRING_AGG(list.species,' | ') "Species audited"
FROM (SELECT DISTINCT AKVO_Tree_external_audits_pcq.identifier_akvo, AKVO_Tree_external_audits_pcq.q1_spec
 AS species
FROM AKVO_Tree_external_audits_pcq
	  WHERE AKVO_Tree_external_audits_pcq.q1_spec <>''
UNION
SELECT DISTINCT AKVO_Tree_external_audits_pcq.identifier_akvo, AKVO_Tree_external_audits_pcq.q2_spec
FROM AKVO_Tree_external_audits_pcq
	  WHERE AKVO_Tree_external_audits_pcq.q2_spec <>''
UNION
SELECT DISTINCT AKVO_Tree_external_audits_pcq.identifier_akvo, AKVO_Tree_external_audits_pcq.q3_spec
FROM AKVO_Tree_external_audits_pcq
	  WHERE AKVO_Tree_external_audits_pcq.q3_spec <>''
UNION
SELECT DISTINCT AKVO_Tree_external_audits_pcq.identifier_akvo, AKVO_Tree_external_audits_pcq.q4_spec
FROM AKVO_Tree_external_audits_pcq
	 WHERE AKVO_Tree_external_audits_pcq.q4_spec <> '') AS list
					 GROUP BY identifier_akvo),

latest_monitoring_results AS (SELECT
CALC_TAB_monitoring_calculations_per_site_by_partner.identifier_akvo,

MAX(CASE
WHEN CALC_TAB_monitoring_calculations_per_site_by_partner.monitoring_strata >= 180
THEN CALC_TAB_monitoring_calculations_per_site_by_partner."Tree density (trees/ha)"
END) as "Latest monitored tree density (trees/ha)",

MAX(CASE
WHEN CALC_TAB_monitoring_calculations_per_site_by_partner.monitoring_strata >= 180
THEN CALC_TAB_monitoring_calculations_per_site_by_partner."Percentage of survived trees"
END) AS "Latest monitored survival percentage"

FROM CALC_TAB_monitoring_calculations_per_site_by_partner

GROUP BY
CALC_TAB_monitoring_calculations_per_site_by_partner.identifier_akvo),

merge_pcq AS (select
identifier_akvo,
Q1_dist AS pcq_results_merged
FROM akvo_tree_external_audits_pcq
UNION ALL
Select identifier_akvo,	Q2_dist AS pcq_results_merged
FROM akvo_tree_external_audits_pcq
UNION ALL
Select identifier_akvo, Q3_dist AS pcq_results_merged
FROM akvo_tree_external_audits_pcq
UNION ALL
Select identifier_akvo, Q4_dist AS pcq_results_merged
FROM akvo_tree_external_audits_pcq),

skipped_pcq_outliers AS (Select
* FROM merge_pcq
where merge_pcq.pcq_results_merged > 0 AND merge_pcq.pcq_results_merged < 1000),

average_audited_tree_distance AS (SELECT skipped_pcq_outliers.identifier_akvo,
AVG(skipped_pcq_outliers.pcq_results_merged) AS avg_audited_tree_distance
FROM skipped_pcq_outliers
GROUP BY skipped_pcq_outliers.identifier_akvo)

SELECT
-- Implement results from registrations
AKVO_Tree_registration_areas.centroid_coord,
AKVO_Tree_registration_areas.identifier_akvo AS "Identifier AKVO",
AKVO_Tree_external_audits_pcq.instance AS "Audit instance",
AKVO_Tree_registration_areas.organisation AS "Name organisation",
AKVO_Tree_registration_areas.submitter AS "Submitter of registration data",
AKVO_Tree_external_audits_areas.submitter AS "Name auditor",
AKVO_Tree_external_audits_areas.submissiondate AS "Submission date audit",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.planting_date AS "Planting date of site",
AKVO_Tree_registration_areas.estimated_area AS "Estimated area registered polygon",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.calc_area AS "Calculated area registered polygon",
AKVO_Tree_external_audits_areas.calc_area AS "Calculated area of audit polygon",
AKVO_Tree_registration_areas.tree_number AS "Registered nr trees by partner",

CASE
WHEN AKVO_Tree_registration_areas.calc_area NOTNULL AND AKVO_Tree_registration_areas.tree_number NOTNULL
THEN CAST(SQRT(AKVO_Tree_registration_areas.calc_area*10000)/NULLIF(SQRT(AKVO_Tree_registration_areas.tree_number),0) AS NUMERIC(8,2))
END AS "Registered avg tree distance (m)",

average_audited_tree_distance.avg_audited_tree_distance AS "Average audited tree distance",

ROUND((1/NULLIF(POWER(average_audited_tree_distance.avg_audited_tree_distance,2),0)*10000),0) AS "Audited tree density (trees/ha)",

CASE
WHEN AKVO_Tree_external_audits_areas.calc_area NOTNULL
THEN ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_external_audits_areas.calc_area,0)),0)
ELSE ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_registration_areas.calc_area,0)),0)
END AS "Total audited nr trees for this site",

latest_monitoring_results."Latest monitored tree density (trees/ha)",
latest_monitoring_results."Latest monitored survival percentage",

CASE
WHEN AKVO_Tree_external_audits_areas.calc_area NOTNULL
THEN ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_external_audits_areas.calc_area,0)/NULLIF(AKVO_Tree_registration_areas.tree_number,0)*100),0)
ELSE ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_registration_areas.calc_area,0)/NULLIF(AKVO_Tree_registration_areas.tree_number,0)*100),0)
END AS "% survived trees",

species_audited."Number of species audited",
species_audited."Species audited"

FROM AKVO_Tree_external_audits_areas
JOIN AKVO_Tree_external_audits_pcq
ON AKVO_Tree_external_audits_areas.identifier_akvo = AKVO_Tree_external_audits_pcq.identifier_akvo
JOIN AKVO_Tree_registration_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
LEFT JOIN species_audited
ON AKVO_Tree_external_audits_areas.identifier_akvo = species_audited.identifier_akvo
LEFT JOIN CALC_TAB_monitoring_calculations_per_site_by_partner
ON CALC_TAB_monitoring_calculations_per_site_by_partner.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo
LEFT JOIN latest_monitoring_results
ON AKVO_Tree_external_audits_areas.identifier_akvo = latest_monitoring_results.identifier_akvo
LEFT JOIN average_audited_tree_distance
ON AKVO_Tree_external_audits_areas.identifier_akvo = average_audited_tree_distance.identifier_akvo


GROUP BY
AKVO_Tree_external_audits_pcq.instance,
akvo_tree_registration_areas.centroid_coord,
akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.organisation,
AKVO_Tree_registration_areas.submitter,
AKVO_Tree_external_audits_areas.submitter,
AKVO_Tree_registration_areas.id_planting_site,
AKVO_Tree_registration_areas.estimated_area,
AKVO_Tree_registration_areas.contract_number,
AKVO_Tree_registration_areas.calc_area,
AKVO_Tree_external_audits_areas.calc_area,
AKVO_Tree_registration_areas.tree_number,
species_audited."Species audited",
species_audited."Number of species audited",
AKVO_Tree_external_audits_areas.submissiondate,
AKVO_Tree_registration_areas.planting_date,
average_audited_tree_distance.avg_audited_tree_distance,
--CALC_TAB_monitoring_calculations_per_site_by_partner."Tree density (trees/ha)",
latest_monitoring_results."Latest monitored tree density (trees/ha)",
latest_monitoring_results."Latest monitored survival percentage"

ORDER BY AKVO_Tree_registration_areas.organisation, AKVO_Tree_registration_areas.id_planting_site;'''

conn.commit()

create_a7 = '''CREATE TABLE CALC_GEOM_Trees_counted_per_site_by_external_audit
AS SELECT AKVO_Tree_external_audits_areas.location_external_audit,
AKVO_Tree_registration_areas.identifier_akvo,
AKVO_Tree_registration_areas.organisation AS "Name organisation",
AKVO_Tree_registration_areas.submitter AS "Name submitter registration data",
AKVO_Tree_external_audits_areas.submitter AS "Name auditor",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.tree_number AS "Nr. trees registered",
AKVO_Tree_external_audits_areas.manual_tree_count AS "Nr. trees counted by auditor"
FROM AKVO_Tree_registration_areas
JOIN AKVO_Tree_external_audits_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
WHERE AKVO_Tree_external_audits_areas.manual_tree_count NOTNULL
ORDER BY AKVO_Tree_registration_areas.organisation;'''

conn.commit()

create_a8 = '''CREATE TABLE CALC_GEOM_AKVO_tree_registration_submissions_today
AS SELECT * FROM AKVO_Tree_registration_areas
WHERE AKVO_Tree_registration_areas.submission = current_date
ORDER BY submissiontime ASC;'''

conn.commit()

create_a9 = '''CREATE TABLE CALC_GEOM_AKVO_nursery_registration_submissions_today
AS SELECT *
FROM AKVO_Nursery_registration
WHERE AKVO_Nursery_registration.submission = current_date
ORDER BY submission ASC;'''

create_a10 = '''CREATE TABLE CALC_GEOM_AKVO_check_photo_registrations
AS SELECT AKVO_Tree_registration_areas.contract_number, AKVO_Tree_registration_photos.*  FROM AKVO_Tree_registration_photos
JOIN AKVO_Tree_registration_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_photos.identifier_akvo;'''

create_a11 = '''CREATE TABLE CALC_GEOM_AKVO_check_species_registrations
AS SELECT AKVO_Tree_registration_areas.centroid_coord, AKVO_Tree_registration_areas.contract_number, AKVO_Tree_registration_species.*
FROM AKVO_Tree_registration_species
JOIN AKVO_Tree_registration_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_species.identifier_akvo;'''

create_a12 = '''CREATE TABLE CALC_GEOM_locations_registration_versus_externalaudits
AS SELECT AKVO_Tree_external_audits_areas.location_external_audit, AKVO_Tree_registration_areas.contract_number, AKVO_Tree_registration_areas.instance, AKVO_Tree_registration_areas.id_planting_site,
AKVO_Tree_registration_areas.country, AKVO_Tree_external_audits_areas.submitter, AKVO_Tree_registration_areas.lat_y AS "lat-registration", AKVO_Tree_registration_areas.lon_x AS "lon-registration",
AKVO_Tree_external_audits_areas.lat_y AS "lat-audit", AKVO_Tree_external_audits_areas.lon_x AS "lon-audit"
FROM AKVO_Tree_registration_areas JOIN AKVO_Tree_external_audits_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
WHERE AKVO_Tree_external_audits_areas.lat_y NOTNULL and AKVO_Tree_external_audits_areas.lon_x NOTNULL
and AKVO_Tree_registration_areas.lat_y NOTNULL and AKVO_Tree_registration_areas.lon_x NOTNULL;'''


create_a13 = '''CREATE TABLE CALC_GEOM_Trees_counted_per_site_by_partner
AS Select AKVO_Tree_registration_areas.centroid_coord,
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.id_planting_site,
SUM(AKVO_Tree_monitoring_counts.number_species) AS "Number of trees counted by species",
COUNT(*) AS "Number of species counted",
AKVO_Tree_registration_areas.tree_number AS "Registered tree number", AKVO_Tree_monitoring_areas.*
FROM AKVO_Tree_monitoring_areas
JOIN AKVO_Tree_monitoring_counts
ON AKVO_Tree_monitoring_areas.instance = AKVO_Tree_monitoring_counts.instance
JOIN AKVO_Tree_registration_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
and AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
group by AKVO_Tree_monitoring_counts.instance, AKVO_Tree_registration_areas.contract_number, AKVO_Tree_registration_areas.id_planting_site,
AKVO_Tree_registration_areas.centroid_coord, AKVO_Tree_registration_areas.tree_number,
AKVO_Tree_registration_areas.identifier_akvo, akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_monitoring_areas.device_id, akvo_tree_monitoring_areas.instance,
akvo_tree_monitoring_areas.submission_year, akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.akvo_form_version,
akvo_tree_monitoring_areas.avg_tree_height,
akvo_tree_monitoring_areas.number_living_trees,
akvo_tree_monitoring_areas.site_impression,
akvo_tree_monitoring_areas.method_selection, akvo_tree_monitoring_areas.avg_circom_tree_count,
akvo_tree_monitoring_areas.test,
akvo_tree_monitoring_areas.avg_circom_tree_pcq, akvo_tree_monitoring_areas.submission, akvo_tree_monitoring_areas.display_name;'''

conn.commit()

create_a14 = '''CREATE TABLE CALC_TAB_overall_statistics
AS WITH RECURSIVE summary_stats_trees AS (

SELECT
COUNT(*) AS total_instances,
SUM(AKVO_Tree_registration_areas.tree_number) AS total_tree_number,
MAX(AKVO_Tree_registration_areas.estimated_area) AS largest_area_ha,
MIN(AKVO_Tree_registration_areas.estimated_area) AS smallest_area_ha,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY AKVO_Tree_registration_areas.calc_area) AS percentile_50,
PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY AKVO_Tree_registration_areas.calc_area) AS percentile_90,
PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY AKVO_Tree_registration_areas.calc_area) AS percentile_95,
PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY AKVO_Tree_registration_areas.calc_area) AS percentile_99
FROM AKVO_Tree_registration_areas

), summary_number_sites_monitored AS(
SELECT COUNT(DISTINCT identifier_akvo) AS nr_sites_monitored FROM akvo_tree_monitoring_areas
),

summary_stats_nurseries AS(
SELECT COUNT(*) AS total_nursery_instances
FROM AKVO_nursery_registration
),

summary_stats_photos AS(
SELECT COUNT(*) AS number_of_photos_registered
FROM AKVO_tree_registration_photos
),

summary_stats_species AS(
SELECT COUNT(DISTINCT lat_name_species) AS number_of_species
FROM AKVO_tree_registration_species
),

summary_stats_monitoring AS(
SELECT COUNT(*) AS total_sites_monitored
FROM AKVO_tree_monitoring_areas
),

row_summary_stats AS (

SELECT 1 AS sno, 'Total tree registration instances in database' AS statistic, total_instances AS value
FROM summary_stats_trees
UNION
SELECT 2, 'Total tree monitoring instances in database', total_sites_monitored
FROM summary_stats_monitoring
UNION
SELECT 3, 'Total number of trees in database', total_tree_number
FROM summary_stats_trees
UNION
SELECT 4, 'Total number of sites monitores (at least 1 time)', nr_sites_monitored
FROM summary_number_sites_monitored
UNION
SELECT 5, 'Largest estimated site in database (ha)', largest_area_ha
FROM summary_stats_trees
UNION
SELECT 6, 'Smalles estimated site in database (ha)', smallest_area_ha
FROM summary_stats_trees
UNION
SELECT 7, '50th percentile of area size', percentile_50
FROM summary_stats_trees
UNION
SELECT 8, '90th percentile of area size', percentile_90
FROM summary_stats_trees
UNION
SELECT 9, '95th percentile of area size', percentile_95
FROM summary_stats_trees
UNION
SELECT 10, '99th percentile of area size', percentile_99
FROM summary_stats_trees
UNION
SELECT 11, 'Number of nursery registrations', total_nursery_instances
FROM summary_stats_nurseries
UNION
SELECT 12, 'Number of tree species', number_of_species
FROM summary_stats_species
UNION
SELECT 13, 'Number of registered photos', number_of_photos_registered
FROM summary_stats_photos)
SELECT * FROM row_summary_stats
ORDER BY sno
;'''

conn.commit()

create_a15 = '''CREATE TABLE CALC_TAB_tree_submissions_per_contract
AS WITH CTE_total_tree_registrations AS (
select
lower(akvo_tree_registration_areas.country) as country,
lower(akvo_tree_registration_areas.organisation) as organisation,
akvo_tree_registration_areas.contract_number,
SUM(akvo_tree_registration_areas.tree_number) AS "Registered tree number",
MAX(akvo_tree_registration_areas.submission) AS "Latest submitted registration",
COUNT(*) AS "Nr of sites registered",
count(DISTINCT AKVO_Tree_registration_areas.identifier_akvo) AS "Number of site registrations"
FROM akvo_tree_registration_areas
WHERE (akvo_tree_registration_areas.test = '' OR akvo_tree_registration_areas.test = 'This is real, valid data')
AND NOT akvo_tree_registration_areas.id_planting_site = 'placeholder' AND NOT country = '' AND NOT organisation = ''
GROUP BY akvo_tree_registration_areas.contract_number,
country,
organisation),

CTE_tree_monitoring AS (select
akvo_tree_registration_areas.contract_number,
COUNT(DISTINCT AKVO_Tree_monitoring_areas.identifier_akvo) as nr_sites_monitored,
COUNT(DISTINCT AKVO_Tree_monitoring_areas.instance) as nr_of_monitorings,
MAX(AKVO_Tree_monitoring_areas.submission) AS "Latest submitted monitoring"
FROM akvo_tree_registration_areas
	LEFT JOIN AKVO_Tree_monitoring_areas
	ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
GROUP BY AKVO_Tree_registration_areas.contract_number
order by AKVO_Tree_registration_areas.contract_number),

CTE_tree_species AS (
select
	akvo_tree_registration_areas.contract_number,
	COUNT(DISTINCT akvo_tree_registration_species.lat_name_species) as "Number of tree species registered"
	FROM akvo_tree_registration_areas
JOIN akvo_tree_registration_species
	ON akvo_tree_registration_areas.identifier_akvo	= akvo_tree_registration_species.identifier_akvo
	GROUP BY akvo_tree_registration_areas.contract_number),

CTE_registered_tree_number_monitored_sites AS (select akvo_tree_registration_areas.contract_number,
SUM(akvo_tree_registration_areas.tree_number) as total_registered_trees_on_monitored_sites
from akvo_tree_registration_areas
INNER JOIN
(SELECT DISTINCT akvo_tree_monitoring_areas.identifier_akvo
 FROM akvo_tree_monitoring_areas) table_x
ON table_x.identifier_akvo = akvo_tree_registration_areas.identifier_akvo
group by akvo_tree_registration_areas.contract_number),

cte_monitored_tree_number_monitored_sites AS (select table_y.contract_number,
SUM(table_y.monitored_tree_number) as monitored_tree_number
from (SELECT MAX(CALC_TAB_monitoring_calculations_per_site_by_partner.monitoring_strata),
CALC_TAB_monitoring_calculations_per_site_by_partner.contract_number,
CALC_TAB_monitoring_calculations_per_site_by_partner."Total nr trees on site (registered/monitored)" as monitored_tree_number
FROM CALC_TAB_monitoring_calculations_per_site_by_partner
WHERE CALC_TAB_monitoring_calculations_per_site_by_partner.monitoring_strata > 0
GROUP BY CALC_TAB_monitoring_calculations_per_site_by_partner.contract_number,
CALC_TAB_monitoring_calculations_per_site_by_partner."Total nr trees on site (registered/monitored)"
order by CALC_TAB_monitoring_calculations_per_site_by_partner.contract_number) table_y
GROUP BY table_y.contract_number)

	SELECT
	CTE_total_tree_registrations.country,
	CTE_total_tree_registrations.organisation,
	CTE_total_tree_registrations.contract_number AS "Contract number",
	CTE_total_tree_registrations."Nr of sites registered" AS "Total number of sites registered",
	CTE_total_tree_registrations."Registered tree number" AS "Total number of trees registered",
	cte_registered_tree_number_monitored_sites.total_registered_trees_on_monitored_sites AS "Total nr of registered trees on sites that have been monitored",
	CTE_total_tree_registrations."Latest submitted registration",
	CTE_tree_monitoring.nr_of_monitorings AS "Total number of monitoring submissions",
	CTE_tree_monitoring.nr_sites_monitored AS "Number of sites monitored at least 1 time",
	CTE_tree_monitoring."Latest submitted monitoring",
	cte_monitored_tree_number_monitored_sites.monitored_tree_number,
	ROUND(cte_monitored_tree_number_monitored_sites.monitored_tree_number/
	  NULLIF(cte_registered_tree_number_monitored_sites.total_registered_trees_on_monitored_sites,0)*100
	 ,2) AS "Survived tree percentage on monitored sites",
	CTE_tree_species."Number of tree species registered"

	FROM CTE_total_tree_registrations
	LEFT JOIN CTE_tree_monitoring
	ON CTE_tree_monitoring.contract_number = CTE_total_tree_registrations.contract_number
	LEFT JOIN CTE_tree_species
	ON CTE_tree_species.contract_number = CTE_total_tree_registrations.contract_number
	LEFT JOIN cte_monitored_tree_number_monitored_sites
	ON CTE_tree_monitoring.contract_number = cte_monitored_tree_number_monitored_sites.contract_number
	LEFT JOIN cte_registered_tree_number_monitored_sites
	ON cte_registered_tree_number_monitored_sites.contract_number = CTE_tree_monitoring.contract_number;'''


conn.commit()


create_a16 = '''CREATE TABLE CALC_TAB_Error_check_on_nursery_registration AS select
akvo_nursery_registration.identifier_akvo,
akvo_nursery_registration.nursery_name,
akvo_nursery_registration.organisation,
akvo_nursery_registration.full_tree_capacity as "maximum/full tree capacity of nursery",
akvo_nursery_monitoring.number_trees_produced_currently,
akvo_nursery_monitoring.month_planting_stock,
akvo_nursery_registration.submission as registration_date,
akvo_nursery_monitoring.submission_date as monitoring_date,
species.species_list as "species currently produced in nursery",
nr_photos_monitoring.counted_photos_monitoring as "nr of photos taken during monitoring",
nr_photos_registration.counted_photos_registration as "nr of photos taken during registration",
akvo_nursery_registration.centroid_coord

from akvo_nursery_monitoring
JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = akvo_nursery_monitoring.identifier_akvo
JOIN
(Select STRING_AGG(akvo_nursery_monitoring_tree_species.tree_species_latin,' | ') species_list,
	akvo_nursery_monitoring_tree_species.instance
FROM akvo_nursery_monitoring_tree_species
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring_tree_species.instance = akvo_nursery_monitoring.instance
GROUP BY akvo_nursery_monitoring_tree_species.instance) species
ON akvo_nursery_monitoring.instance = species.instance
JOIN
(select COUNT(akvo_nursery_monitoring_photos.instance) counted_photos_monitoring, akvo_nursery_monitoring_photos.instance
	 FROM akvo_nursery_monitoring_photos
	 group by akvo_nursery_monitoring_photos.instance) nr_photos_monitoring
ON akvo_nursery_monitoring.instance = nr_photos_monitoring.instance

JOIN (select COUNT(akvo_nursery_registration_photos.instance) counted_photos_registration, akvo_nursery_registration_photos.instance
	 FROM akvo_nursery_registration_photos
	 group by akvo_nursery_registration_photos.instance) nr_photos_registration
ON akvo_nursery_registration.instance = nr_photos_registration.instance

group by akvo_nursery_registration.organisation, akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.submission_date,
akvo_nursery_registration.full_tree_capacity,
akvo_nursery_monitoring.number_trees_produced_currently,
akvo_nursery_monitoring.month_planting_stock,
akvo_nursery_registration.submission,
akvo_nursery_registration.nursery_name,
species.species_list,
nr_photos_monitoring.counted_photos_monitoring,
nr_photos_registration.counted_photos_registration,
akvo_nursery_registration.identifier_akvo,
akvo_nursery_registration.centroid_coord

order by akvo_nursery_monitoring.submission_date desc;'''

conn.commit()

create_a17 = ''' CREATE TABLE CALC_TAB_Error_partner_report_on_nursery_registration
AS SELECT
identifier_akvo,
nursery_name,
organisation,
"maximum/full tree capacity of nursery",
number_trees_produced_currently,
month_planting_stock,
registration_date,
monitoring_date,
"species currently produced in nursery",
"nr of photos taken during registration",
"nr of photos taken during monitoring",

CASE
WHEN "nr of photos taken during registration" < 4
THEN 'Not enough photos have been taken from the nursery during the initial registration. Should be 4 photos as minimum'
WHEN "nr of photos taken during registration" >= 4
THEN 'Enough photos have been taken from the nursery (4 photos). Correctly done'
END AS "Check nr of photos taken during registration of the nursery",

CASE
WHEN "nr of photos taken during monitoring" < 4
THEN 'Not enough photos have been taken from the nursery during the monitoring. Should be 4 photos as minimum'
WHEN "nr of photos taken during monitoring" >= 4
THEN 'Enough photos have been taken from the nursery (4 photos). Correctly done'
END AS "Check nr of photos taken during monitoring of the nursery",

centroid_coord

FROM CALC_TAB_Error_check_on_nursery_registration'''

conn.commit()

create_a18 = '''CREATE TABLE akvo_ecosia_tree_area_monitoring AS
SELECT
akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_registration_areas.country,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_monitoring_areas.display_name,
akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.akvo_form_version,
akvo_tree_monitoring_areas.method_selection,
monitoring_results.COUNT-1 AS "Number of times site has been monitored",
akvo_tree_registration_areas.tree_number AS "Registered tree number",
monitoring_results."Latest monitored nr trees on site",
monitoring_results."Latest monitored percentage of survived trees",
monitoring_results."Latest monitoring submission date",
akvo_tree_registration_areas.polygon,
akvo_tree_registration_areas.multipoint,
akvo_tree_registration_areas.centroid_coord
FROM akvo_tree_monitoring_areas
LEFT JOIN akvo_tree_registration_areas
ON akvo_tree_monitoring_areas.identifier_akvo = akvo_tree_registration_areas.identifier_akvo
LEFT JOIN (SELECT CALC_TAB_monitoring_calculations_per_site_by_partner.identifier_akvo,
		   COUNT(CALC_TAB_monitoring_calculations_per_site_by_partner.monitoring_strata),
		   MAX(CALC_TAB_monitoring_calculations_per_site_by_partner."Total nr trees on site (registered/monitored)") AS "Latest monitored nr trees on site",
MAX(CALC_TAB_monitoring_calculations_per_site_by_partner."Percentage of survived trees") AS "Latest monitored percentage of survived trees",
MAX(CALC_TAB_monitoring_calculations_per_site_by_partner.submission) AS "Latest monitoring submission date"
FROM CALC_TAB_monitoring_calculations_per_site_by_partner
GROUP BY CALC_TAB_monitoring_calculations_per_site_by_partner.identifier_akvo) monitoring_results
ON akvo_tree_monitoring_areas.identifier_akvo = monitoring_results.identifier_akvo
GROUP BY akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_registration_areas.country,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_monitoring_areas.display_name,
akvo_tree_monitoring_areas.submission_year,
akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.akvo_form_version,
akvo_tree_monitoring_areas.test,
monitoring_results."Latest monitored nr trees on site",
monitoring_results."Latest monitored percentage of survived trees",
monitoring_results."Latest monitoring submission date",
akvo_tree_registration_areas.polygon,
akvo_tree_registration_areas.multipoint,
akvo_tree_registration_areas.centroid_coord,
monitoring_results.COUNT,
akvo_tree_registration_areas.tree_number,
akvo_tree_monitoring_areas.method_selection;'''

conn.commit()

create_a19 = '''CREATE TABLE akvo_ecosia_nursery_monitoring AS
SELECT
akvo_nursery_monitoring.*,
akvo_nursery_registration.nursery_name,
akvo_nursery_registration.organisation,
akvo_nursery_registration.centroid_coord
FROM akvo_nursery_monitoring
LEFT JOIN akvo_nursery_registration
ON akvo_nursery_monitoring.identifier_akvo = akvo_nursery_registration.identifier_akvo;'''

conn.commit()

create_a20 = '''CREATE TABLE error_partner_report_on_site_registration AS
SELECT * FROM CALC_TAB_Error_partner_report_on_site_registration;'''

conn.commit()

create_a21 = ''' CREATE TABLE error_partner_report_on_nursery_registration
AS SELECT * FROM CALC_TAB_Error_partner_report_on_nursery_registration;'''

conn.commit()

create_a22 = ''' CREATE TABLE akvo_ecosia_tree_area_registration
AS SELECT * FROM akvo_tree_registration_areas_updated;'''

conn.commit()

create_a23 = ''' CREATE TABLE akvo_ecosia_nursery_registration
AS SELECT * FROM AKVO_nursery_registration;'''

conn.commit()

create_a24 = ''' CREATE TABLE akvo_ecosia_contract_overview
AS SELECT * FROM calc_tab_tree_submissions_per_contract;'''

conn.commit()

create_a25 = ''' CREATE TABLE akvo_ecosia_tree_photo_registration
AS SELECT akvo_tree_registration_areas_updated.country,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
AKVO_tree_registration_photos.* FROM AKVO_tree_registration_photos
JOIN akvo_tree_registration_areas_updated
ON AKVO_tree_registration_photos.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo'''

conn.commit()

create_a26 = ''' CREATE TABLE s4g_ecosia_site_health
AS SELECT akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.country,
akvo_tree_registration_areas.tree_number AS number_trees_registered,
S4G_API_health_indicators.health_index,
S4G_API_health_indicators.health_index_normalized,
S4G_API_health_indicators.health_trend,
S4G_API_health_indicators.health_trend_normalized,
akvo_tree_registration_areas.centroid_coord

FROM akvo_tree_registration_areas
JOIN S4G_API_health_indicators
ON akvo_tree_registration_areas.identifier_akvo = S4G_API_health_indicators.identifier_akvo'''

conn.commit()

create_a27 = ''' CREATE TABLE s4g_ecosia_fires
AS SELECT akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
s4g_api_fires.detection_date,
s4g_api_fires.confidence_level,
s4g_api_fires.area_ha AS "area_affected_ha",
akvo_tree_registration_areas.centroid_coord

FROM akvo_tree_registration_areas
JOIN S4G_API_fires
ON akvo_tree_registration_areas.identifier_akvo = S4G_API_fires.identifier_akvo'''

conn.commit()

create_a28 = ''' CREATE TABLE s4g_ecosia_deforestation
AS SELECT akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
S4G_API_deforestation.deforestation_date,
S4G_API_deforestation.deforestation_nr_alerts,
S4G_API_deforestation.deforestation_area AS "area_affected_ha",
akvo_tree_registration_areas.centroid_coord

FROM akvo_tree_registration_areas
JOIN S4G_API_deforestation
ON akvo_tree_registration_areas.identifier_akvo = S4G_API_deforestation.identifier_akvo'''

conn.commit()

create_a29 = ''' CREATE TABLE s4g_ecosia_landuse_cover
AS SELECT akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
s4g_API_landcover_change.year AS year_lc_change,
s4g_API_landcover_change.month AS month_lc_change,
s4g_API_landcover_change.water,
s4g_API_landcover_change.trees,
s4g_API_landcover_change.grass,
s4g_API_landcover_change.flooded_vegetation,
s4g_API_landcover_change.crops,
s4g_API_landcover_change.shrub_scrub,
s4g_API_landcover_change.built,
s4g_API_landcover_change.bare,
s4g_API_landcover_change.snow_ice,
akvo_tree_registration_areas.centroid_coord

FROM akvo_tree_registration_areas
JOIN s4g_API_landcover_change
ON akvo_tree_registration_areas.identifier_akvo = s4g_API_landcover_change.identifier_akvo'''

conn.commit()

create_a30 = ''' CREATE TABLE s4g_ecosia_data_quality
AS SELECT akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.organisation,
S4G_API_data_quality.partner_site_id,
S4G_API_data_quality.contract_number,
S4G_API_data_quality.country,
S4G_API_data_quality.trees_planted AS number_trees_registered,
S4G_API_data_quality.nr_photos_taken,
S4G_API_data_quality.issues AS geometric_sum_errors_detected,
S4G_API_data_quality.invalid_polygon AS geometric_error_polygon,
S4G_API_data_quality.invalid_point AS geometric_error_point,
S4G_API_data_quality.unconnected AS geometric_error_sliver_polygon,
S4G_API_data_quality.area_too_large AS geometric_error_area_too_large,
S4G_API_data_quality.area_too_small AS geometric_error_area_too_small,
S4G_API_data_quality.overlap AS geometric_error_area_boundary_overlap_other_site,
S4G_API_data_quality.circumference_too_large AS geometric_error_boundary_too_large,
S4G_API_data_quality.site_not_in_country AS geometric_error_site_not_in_country,
S4G_API_data_quality.area_water_ha AS geometric_error_water_body_located_in_site,
S4G_API_data_quality.area_urban_ha AS geometric_error_urban_body_located_in_site,
S4G_API_data_quality.artificially_created_polygon AS buffer_area_around_point_location,
akvo_tree_registration_areas.centroid_coord

FROM akvo_tree_registration_areas
JOIN S4G_API_data_quality
ON akvo_tree_registration_areas.identifier_akvo = S4G_API_data_quality.identifier_akvo

ORDER BY S4G_API_data_quality.issues DESC'''

conn.commit()

create_a31 = '''CREATE TABLE superset_ecosia_nursery_registration
AS SELECT
akvo_nursery_registration.*,
CALC_TAB_Error_partner_report_on_nursery_registration."species currently produced in nursery",
CALC_TAB_Error_partner_report_on_nursery_registration."nr of photos taken during registration",
CALC_TAB_Error_partner_report_on_nursery_registration."Check nr of photos taken during registration of the nursery"

FROM akvo_nursery_registration
JOIN CALC_TAB_Error_partner_report_on_nursery_registration
ON CALC_TAB_Error_partner_report_on_nursery_registration.identifier_akvo = akvo_nursery_registration.identifier_akvo;'''

conn.commit()


create_a32 = '''CREATE TABLE superset_ecosia_tree_registration
AS SELECT
t.*,
s4g_ecosia_data_quality.nr_photos_taken AS "number of tree photos taken",
s4g_ecosia_data_quality.geometric_sum_errors_detected AS "total nr of mapping errors detected",
s4g_ecosia_data_quality.geometric_error_polygon AS "site was mapped with too few points (less than 3)",
s4g_ecosia_data_quality.geometric_error_point AS "site was mapped wrongly",
s4g_ecosia_data_quality.geometric_error_sliver_polygon AS "mapped site has multiple areas",
s4g_ecosia_data_quality.geometric_error_area_too_large AS "area of the site is unrealisticly large",
s4g_ecosia_data_quality.geometric_error_area_too_small AS "area of the site is unrealisticly small",
s4g_ecosia_data_quality.geometric_error_area_boundary_overlap_other_site AS "mapped site has overlap with another site",
s4g_ecosia_data_quality.geometric_error_boundary_too_large AS "boundary of mapped site seems unrealistic",
s4g_ecosia_data_quality.geometric_error_site_not_in_country AS "mapped site not located in country of project",
s4g_ecosia_data_quality.geometric_error_water_body_located_in_site AS "mapped site contains water area",
s4g_ecosia_data_quality.geometric_error_urban_body_located_in_site AS "mapped site contains urban area",
s4g_ecosia_data_quality.buffer_area_around_point_location AS "artificial 50m buffer placed around mapped site (points)",

S4G_API_health_indicators.health_index,
S4G_API_health_indicators.health_index_normalized,
S4G_API_health_indicators.health_trend,
S4G_API_health_indicators.health_trend_normalized,

json_build_object(
'type', 'Polygon',
'geometry', ST_AsGeoJSON(t.polygon)::json)::text as geojson

FROM
akvo_tree_registration_areas_updated AS t
LEFT JOIN s4g_ecosia_data_quality
ON t.identifier_akvo = s4g_ecosia_data_quality.identifier_akvo
LEFT JOIN S4G_API_health_indicators
ON t.identifier_akvo = S4G_API_health_indicators.identifier_akvo;
--where t.polygon NOTNULL;
'''

conn.commit()

create_a33 = '''CREATE TABLE superset_ecosia_tree_monitoring
AS SELECT akvo_tree_monitoring_areas.*,
akvo_tree_registration_areas_updated.country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.lat_y,
akvo_tree_registration_areas_updated.lon_x
FROM akvo_tree_monitoring_areas
LEFT JOIN akvo_tree_registration_areas_updated
ON akvo_tree_monitoring_areas.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo;'''

conn.commit()



create_a34 = '''CREATE TABLE superset_ecosia_s4g_site_health
AS SELECT
s4g_ecosia_site_health.*,
akvo_tree_registration_areas.lat_y,
akvo_tree_registration_areas.lon_x

FROM s4g_ecosia_site_health
JOIN akvo_tree_registration_areas
ON akvo_tree_registration_areas.identifier_akvo = s4g_ecosia_site_health.identifier_akvo;'''

conn.commit()

# THIS ONE IS DE-ACTIVATED IN THE EXECUTE!!
create_a35 = '''CREATE TABLE superset_ecosia_tree_registration_point
AS SELECT

akvo_tree_registration_areas_updated.*,
s4g_ecosia_data_quality.nr_photos_taken AS "number of tree photos taken",
s4g_ecosia_data_quality.geometric_sum_errors_detected AS "total nr of mapping errors detected",
s4g_ecosia_data_quality.geometric_error_polygon AS "site was mapped with too few points (less than 3)",
s4g_ecosia_data_quality.geometric_error_point AS "site was mapped wrongly",
s4g_ecosia_data_quality.geometric_error_sliver_polygon AS "mapped site has multiple areas",
s4g_ecosia_data_quality.geometric_error_area_too_large AS "area of the site is unrealisticly large",
s4g_ecosia_data_quality.geometric_error_area_too_small AS "area of the site is unrealisticly small",
s4g_ecosia_data_quality.geometric_error_area_boundary_overlap_other_site AS "mapped site has overlap with another site",
s4g_ecosia_data_quality.geometric_error_boundary_too_large AS "boundary of mapped site seems unrealistic",
s4g_ecosia_data_quality.geometric_error_site_not_in_country AS "mapped site not located in country of project",
s4g_ecosia_data_quality.geometric_error_water_body_located_in_site AS "mapped site contains water area",
s4g_ecosia_data_quality.geometric_error_urban_body_located_in_site AS "mapped site contains urban area",
s4g_ecosia_data_quality.buffer_area_around_point_location AS "artificial 50m buffer placed around mapped site (points)",

FROM
akvo_tree_registration_areas_updated
JOIN s4g_ecosia_data_quality
ON akvo_tree_registration_areas_updated.identifier_akvo = s4g_ecosia_data_quality.identifier_akvo
where polygon ISNULL;'''

conn.commit()

create_a36 = '''CREATE TABLE superset_ecosia_nursery_monitoring
AS SELECT
akvo_nursery_registration.country,
LOWER(akvo_nursery_registration.organisation) AS organisation,
akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.*,
akvo_nursery_registration.lat_y,
akvo_nursery_registration.lon_x

FROM akvo_nursery_monitoring
JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = akvo_nursery_monitoring.identifier_akvo;'''

conn.commit()

create_a37 = '''CREATE TABLE superset_ecosia_nursery_monitoring_species
AS SELECT
akvo_nursery_registration.country,
LOWER(akvo_nursery_registration.organisation) AS organisation,
akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.submission_date,
akvo_nursery_monitoring_tree_species.*

FROM akvo_nursery_monitoring_tree_species
JOIN akvo_nursery_registration
ON akvo_nursery_monitoring_tree_species.identifier_akvo = akvo_nursery_registration.identifier_akvo
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring.instance = akvo_nursery_monitoring_tree_species.instance;'''

conn.commit()

create_a38 = '''CREATE TABLE superset_ecosia_nursery_registration_pictures
AS SELECT
akvo_nursery_registration.country,
LOWER(akvo_nursery_registration.organisation) AS organisation,
akvo_nursery_registration.nursery_name,
akvo_nursery_registration.submission AS submission_date,
akvo_nursery_registration_photos.*

FROM akvo_nursery_registration_photos
JOIN akvo_nursery_registration
ON akvo_nursery_registration_photos.identifier_akvo =  akvo_nursery_registration.identifier_akvo;'''

conn.commit()

create_a39 = '''CREATE TABLE superset_ecosia_nursery_monitoring_pictures
AS SELECT
akvo_nursery_registration.country,
LOWER(akvo_nursery_registration.organisation) AS organisation,
akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.submission_date,
akvo_nursery_monitoring_photos.*

FROM akvo_nursery_monitoring_photos
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring.instance = akvo_nursery_monitoring_photos.instance
JOIN akvo_nursery_registration
ON akvo_nursery_monitoring_photos.identifier_akvo =  akvo_nursery_registration.identifier_akvo;'''

conn.commit()

create_a40 = '''CREATE TABLE superset_ecosia_tree_registration_pictures
AS SELECT
akvo_tree_registration_areas.country,
LOWER(akvo_tree_registration_areas.organisation) AS organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.submission AS submission_date,
akvo_tree_registration_photos.*

FROM
akvo_tree_registration_photos
JOIN akvo_tree_registration_areas
ON akvo_tree_registration_areas.identifier_akvo = akvo_tree_registration_photos.identifier_akvo;'''

conn.commit()

create_a41 = '''CREATE TABLE superset_ecosia_tree_registration_species
AS SELECT
akvo_tree_registration_areas.country,
LOWER(akvo_tree_registration_areas.organisation) AS organisation,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.submission AS submission_date,
AKVO_tree_registration_species.*

FROM AKVO_tree_registration_species
JOIN akvo_tree_registration_areas
ON AKVO_tree_registration_species.identifier_akvo = akvo_tree_registration_areas.identifier_akvo;'''

conn.commit()

create_a42 = '''CREATE TABLE superset_ecosia_s4g_fires
AS SELECT

s4g_ecosia_fires.*,
akvo_tree_registration_areas.lat_y,
akvo_tree_registration_areas.lon_x

FROM s4g_ecosia_fires
JOIN akvo_tree_registration_areas
ON akvo_tree_registration_areas.identifier_akvo = s4g_ecosia_fires.identifier_akvo;'''

conn.commit()

create_a43 = '''CREATE TABLE superset_ecosia_s4g_deforestation
AS SELECT
s4g_ecosia_deforestation.*,
akvo_tree_registration_areas.lat_y,
akvo_tree_registration_areas.lon_x

FROM s4g_ecosia_deforestation
JOIN akvo_tree_registration_areas
ON akvo_tree_registration_areas.identifier_akvo = s4g_ecosia_deforestation.identifier_akvo;'''


conn.commit()

create_a17_mkec = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM kenya_mkec;

GRANT USAGE ON SCHEMA PUBLIC TO kenya_mkec;
GRANT USAGE ON SCHEMA HEROKU_EXT TO kenya_mkec;

GRANT SELECT ON TABLE akvo_ecosia_tree_area_registration TO kenya_mkec;
GRANT SELECT ON TABLE akvo_ecosia_tree_area_monitoring TO kenya_mkec;
GRANT SELECT ON TABLE akvo_ecosia_nursery_registration TO kenya_mkec;
GRANT SELECT ON TABLE akvo_ecosia_nursery_monitoring TO kenya_mkec;
GRANT SELECT ON TABLE error_partner_report_on_site_registration TO kenya_mkec;
GRANT SELECT ON TABLE error_partner_report_on_nursery_registration TO kenya_mkec;
GRANT SELECT ON TABLE akvo_ecosia_contract_overview TO kenya_mkec;
GRANT SELECT ON TABLE akvo_ecosia_tree_photo_registration TO kenya_mkec;


DROP POLICY IF EXISTS mkec_policy ON akvo_tree_registration_areas_updated;
DROP POLICY IF EXISTS mkec_policy ON akvo_tree_monitoring_areas_geom;
DROP POLICY IF EXISTS mkec_policy ON akvo_nursery_registration;
DROP POLICY IF EXISTS mkec_policy ON akvo_nursery_monitoring_geom;
DROP POLICY IF EXISTS mkec_policy ON CALC_TAB_Error_partner_report_on_site_registration;
DROP POLICY IF EXISTS mkec_policy ON CALC_TAB_Error_partner_report_on_nursery_registration;
DROP POLICY IF EXISTS mkec_policy ON calc_tab_tree_submissions_per_contract;

DROP POLICY IF EXISTS mkec_policy ON akvo_ecosia_tree_area_registration;
DROP POLICY IF EXISTS mkec_policy ON akvo_ecosia_tree_area_monitoring;
DROP POLICY IF EXISTS mkec_policy ON akvo_ecosia_nursery_registration;
DROP POLICY IF EXISTS mkec_policy ON akvo_ecosia_nursery_monitoring;
DROP POLICY IF EXISTS mkec_policy ON error_partner_report_on_site_registration;
DROP POLICY IF EXISTS mkec_policy ON error_partner_report_on_nursery_registration;
DROP POLICY IF EXISTS mkec_policy ON akvo_ecosia_contract_overview;
DROP POLICY IF EXISTS mkec_policy ON akvo_ecosia_tree_photo_registration;

ALTER TABLE akvo_ecosia_tree_area_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_tree_area_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_nursery_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE error_partner_report_on_site_registration enable ROW LEVEL SECURITY;
ALTER TABLE error_partner_report_on_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_contract_overview enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_tree_photo_registration enable ROW LEVEL SECURITY;

CREATE POLICY mkec_policy ON akvo_ecosia_tree_area_registration TO kenya_mkec USING (organisation = 'Mount Kenya Environmental Conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_tree_area_monitoring TO kenya_mkec USING (EXISTS (SELECT * FROM akvo_ecosia_tree_area_registration
WHERE akvo_ecosia_tree_area_registration.organisation = 'Mount Kenya Environmental Conservation'
AND akvo_ecosia_tree_area_monitoring.identifier_akvo = akvo_ecosia_tree_area_registration.identifier_akvo));
CREATE POLICY mkec_policy ON akvo_ecosia_nursery_registration TO kenya_mkec USING (organisation = 'Mount Kenya Environmental Conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_nursery_monitoring TO kenya_mkec USING (EXISTS (SELECT * FROM akvo_ecosia_nursery_registration
WHERE akvo_ecosia_nursery_registration.organisation = 'Mount Kenya Environmental Conservation'
AND akvo_ecosia_nursery_monitoring.identifier_akvo = akvo_ecosia_nursery_registration.identifier_akvo));
CREATE POLICY mkec_policy ON error_partner_report_on_site_registration TO kenya_mkec USING (error_partner_report_on_site_registration.name_organisation = 'Mount Kenya Environmental Conservation');
CREATE POLICY mkec_policy ON error_partner_report_on_nursery_registration TO kenya_mkec USING (error_partner_report_on_nursery_registration.organisation = 'Mount Kenya Environmental Conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_contract_overview TO kenya_mkec USING (akvo_ecosia_contract_overview.organisation = 'mount kenya environmental conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_tree_photo_registration TO kenya_mkec USING (akvo_ecosia_tree_photo_registration.organisation = 'Mount Kenya Environmental Conservation');'''


conn.commit()

create_a18_fdia = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM nicaragua_fdia;

GRANT USAGE ON SCHEMA PUBLIC TO nicaragua_fdia;
GRANT USAGE ON SCHEMA HEROKU_EXT TO nicaragua_fdia;

GRANT SELECT ON TABLE akvo_ecosia_tree_area_registration TO nicaragua_fdia;
GRANT SELECT ON TABLE akvo_ecosia_tree_area_monitoring TO nicaragua_fdia;
GRANT SELECT ON TABLE akvo_ecosia_nursery_registration TO nicaragua_fdia;
GRANT SELECT ON TABLE akvo_ecosia_nursery_monitoring TO nicaragua_fdia;
GRANT SELECT ON TABLE error_partner_report_on_site_registration TO nicaragua_fdia;
GRANT SELECT ON TABLE error_partner_report_on_nursery_registration TO nicaragua_fdia;
GRANT SELECT ON TABLE akvo_ecosia_contract_overview TO nicaragua_fdia;
GRANT SELECT ON TABLE akvo_ecosia_tree_photo_registration TO nicaragua_fdia;


DROP POLICY IF EXISTS fdia_policy ON akvo_tree_registration_areas_updated;
DROP POLICY IF EXISTS fdia_policy ON akvo_tree_registration_areas;
DROP POLICY IF EXISTS fdia_policy ON akvo_tree_monitoring_areas_geom;
DROP POLICY IF EXISTS fdia_policy ON akvo_nursery_registration;
DROP POLICY IF EXISTS fdia_policy ON akvo_nursery_monitoring_geom;
DROP POLICY IF EXISTS fdia_policy ON CALC_TAB_Error_partner_report_on_site_registration;
DROP POLICY IF EXISTS fdia_policy ON CALC_TAB_Error_partner_report_on_nursery_registration;
DROP POLICY IF EXISTS fdia_policy ON calc_tab_tree_submissions_per_contract;


DROP POLICY IF EXISTS fdia_policy ON akvo_ecosia_tree_area_registration;
DROP POLICY IF EXISTS fdia_policy ON akvo_ecosia_tree_area_monitoring;
DROP POLICY IF EXISTS fdia_policy ON akvo_ecosia_nursery_registration;
DROP POLICY IF EXISTS fdia_policy ON akvo_ecosia_nursery_monitoring;
DROP POLICY IF EXISTS fdia_policy ON error_partner_report_on_site_registration;
DROP POLICY IF EXISTS fdia_policy ON error_partner_report_on_nursery_registration;
DROP POLICY IF EXISTS fdia_policy ON akvo_ecosia_contract_overview;
DROP POLICY IF EXISTS fdia_policy ON akvo_ecosia_tree_photo_registration;

ALTER TABLE akvo_ecosia_tree_area_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_tree_area_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_nursery_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE error_partner_report_on_site_registration enable ROW LEVEL SECURITY;
ALTER TABLE error_partner_report_on_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_contract_overview enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_tree_photo_registration enable ROW LEVEL SECURITY;

CREATE POLICY fdia_policy ON akvo_ecosia_tree_area_registration TO nicaragua_fdia USING (organisation = 'Fundacion DIA');
CREATE POLICY fdia_policy ON akvo_ecosia_tree_area_monitoring TO nicaragua_fdia USING (EXISTS (SELECT * FROM akvo_ecosia_tree_area_registration
WHERE akvo_ecosia_tree_area_registration.organisation = 'Fundacion DIA'
AND akvo_ecosia_tree_area_monitoring.identifier_akvo = akvo_ecosia_tree_area_registration.identifier_akvo));
CREATE POLICY fdia_policy ON akvo_ecosia_nursery_registration TO nicaragua_fdia USING (organisation = 'Fundacion DIA');
CREATE POLICY fdia_policy ON akvo_ecosia_nursery_monitoring TO nicaragua_fdia USING (EXISTS (SELECT * FROM akvo_ecosia_nursery_registration
WHERE akvo_ecosia_nursery_registration.organisation = 'Fundacion DIA'
AND akvo_ecosia_nursery_monitoring.identifier_akvo = akvo_ecosia_nursery_registration.identifier_akvo));
CREATE POLICY fdia_policy ON error_partner_report_on_site_registration TO nicaragua_fdia USING (error_partner_report_on_site_registration.name_organisation = 'Fundacion DIA');
CREATE POLICY fdia_policy ON error_partner_report_on_nursery_registration TO nicaragua_fdia USING (error_partner_report_on_nursery_registration.organisation = 'Fundacion DIA');
CREATE POLICY fdia_policy ON akvo_ecosia_contract_overview TO nicaragua_fdia USING (akvo_ecosia_contract_overview.organisation = 'fundacion dia');
CREATE POLICY fdia_policy ON akvo_ecosia_tree_photo_registration TO nicaragua_fdia USING (akvo_ecosia_tree_photo_registration.organisation = 'Fundacion DIA');'''

conn.commit()

create_a19_haf = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM morocco_haf;

GRANT USAGE ON SCHEMA PUBLIC TO morocco_haf;
GRANT USAGE ON SCHEMA HEROKU_EXT TO morocco_haf;

GRANT SELECT ON TABLE akvo_ecosia_tree_area_registration TO morocco_haf;
GRANT SELECT ON TABLE akvo_ecosia_tree_area_monitoring TO morocco_haf;
GRANT SELECT ON TABLE akvo_ecosia_nursery_registration TO morocco_haf;
GRANT SELECT ON TABLE akvo_ecosia_nursery_monitoring TO morocco_haf;
GRANT SELECT ON TABLE error_partner_report_on_site_registration TO morocco_haf;
GRANT SELECT ON TABLE error_partner_report_on_nursery_registration TO morocco_haf;
GRANT SELECT ON TABLE akvo_ecosia_contract_overview TO morocco_haf;
GRANT SELECT ON TABLE akvo_ecosia_tree_photo_registration TO morocco_haf;

DROP POLICY IF EXISTS haf_policy ON akvo_tree_registration_areas_updated;
DROP POLICY IF EXISTS haf_policy ON akvo_tree_registration_areas;
DROP POLICY IF EXISTS haf_policy ON akvo_tree_monitoring_areas_geom;
DROP POLICY IF EXISTS haf_policy ON akvo_nursery_registration;
DROP POLICY IF EXISTS haf_policy ON akvo_nursery_monitoring_geom;
DROP POLICY IF EXISTS haf_policy ON CALC_TAB_Error_partner_report_on_site_registration;
DROP POLICY IF EXISTS haf_policy ON CALC_TAB_Error_partner_report_on_nursery_registration;
DROP POLICY IF EXISTS haf_policy ON calc_tab_tree_submissions_per_contract;

DROP POLICY IF EXISTS haf_policy ON akvo_ecosia_tree_area_registration;
DROP POLICY IF EXISTS haf_policy ON akvo_ecosia_tree_area_monitoring;
DROP POLICY IF EXISTS haf_policy ON akvo_ecosia_nursery_registration;
DROP POLICY IF EXISTS haf_policy ON akvo_ecosia_nursery_monitoring;
DROP POLICY IF EXISTS haf_policy ON error_partner_report_on_site_registration;
DROP POLICY IF EXISTS haf_policy ON error_partner_report_on_nursery_registration;
DROP POLICY IF EXISTS haf_policy ON akvo_ecosia_contract_overview;
DROP POLICY IF EXISTS haf_policy ON akvo_ecosia_tree_photo_registration;

ALTER TABLE akvo_ecosia_tree_area_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_tree_area_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_nursery_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE error_partner_report_on_site_registration enable ROW LEVEL SECURITY;
ALTER TABLE error_partner_report_on_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_contract_overview enable ROW LEVEL SECURITY;
ALTER TABLE akvo_ecosia_tree_photo_registration enable ROW LEVEL SECURITY;

CREATE POLICY haf_policy ON akvo_ecosia_tree_area_registration TO morocco_haf USING (organisation = 'High Atlas Foundation');
CREATE POLICY haf_policy ON akvo_ecosia_tree_area_monitoring TO morocco_haf USING (EXISTS (SELECT * FROM akvo_ecosia_tree_area_registration
WHERE akvo_ecosia_tree_area_registration.organisation = 'High Atlas Foundation'
AND akvo_ecosia_tree_area_monitoring.identifier_akvo = akvo_ecosia_tree_area_registration.identifier_akvo));
CREATE POLICY haf_policy ON akvo_ecosia_nursery_registration TO morocco_haf USING (organisation = 'High Atlas Foundation');
CREATE POLICY haf_policy ON akvo_ecosia_nursery_monitoring TO morocco_haf USING (EXISTS (SELECT * FROM akvo_ecosia_nursery_registration
WHERE akvo_ecosia_nursery_registration.organisation = 'High Atlas Foundation'
AND akvo_ecosia_nursery_monitoring.identifier_akvo = akvo_ecosia_nursery_registration.identifier_akvo));
CREATE POLICY haf_policy ON error_partner_report_on_site_registration TO morocco_haf USING (error_partner_report_on_site_registration.name_organisation = 'High Atlas Foundation');
CREATE POLICY haf_policy ON error_partner_report_on_nursery_registration TO morocco_haf USING (error_partner_report_on_nursery_registration.organisation = 'High Atlas Foundation');
CREATE POLICY haf_policy ON akvo_ecosia_contract_overview TO morocco_haf USING (akvo_ecosia_contract_overview.name_organisation = 'high atlas foundation');
CREATE POLICY haf_policy ON akvo_ecosia_tree_photo_registration TO morocco_haf USING (akvo_ecosia_tree_photo_registration.organisation = 'High Atlas Foundation');'''

conn.commit()

create_a20_ecosia_superset = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ecosia_superset;

GRANT USAGE ON SCHEMA PUBLIC TO ecosia_superset;
GRANT USAGE ON SCHEMA HEROKU_EXT TO ecosia_superset;

GRANT SELECT ON TABLE superset_ecosia_nursery_registration TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_monitoring TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_s4g_site_health TO ecosia_superset;
--GRANT SELECT ON TABLE superset_ecosia_tree_registration_point TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_monitoring TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_monitoring_species TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_registration_pictures TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_monitoring_pictures TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration_pictures TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration_species TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_s4g_fires TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_s4g_deforestation TO ecosia_superset;


DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_registration;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_monitoring;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_s4g_site_health;
--DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration_point;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_monitoring;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_monitoring_species;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_registration_pictures;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_monitoring_pictures;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration_pictures;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration_species;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_s4g_fires;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_s4g_deforestation;


ALTER TABLE superset_ecosia_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_s4g_site_health enable ROW LEVEL SECURITY;
--ALTER TABLE superset_ecosia_tree_registration_point enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_monitoring_species enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_registration_pictures enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_monitoring_pictures enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration_pictures enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration_species enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_s4g_fires enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_s4g_deforestation enable ROW LEVEL SECURITY;


CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_registration TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_monitoring TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_s4g_site_health TO ecosia_superset USING (true);
--CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration_point TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_monitoring TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_monitoring_species TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_registration_pictures TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_monitoring_pictures TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration_pictures TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration_species TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_s4g_fires TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_s4g_deforestation TO ecosia_superset USING (true);'''


conn.commit()


# Execute drop tables
cur.execute(drop_tables)
conn.commit()

# Execute create tables
cur.execute(create_a1)
cur.execute(create_a2)
cur.execute(create_a3)
cur.execute(create_a4)
cur.execute(create_a5)
cur.execute(create_a6)
cur.execute(create_a7)
cur.execute(create_a8)
cur.execute(create_a9)
cur.execute(create_a10)
cur.execute(create_a11)
cur.execute(create_a12)
cur.execute(create_a13)
cur.execute(create_a14)
cur.execute(create_a15)
cur.execute(create_a16)
cur.execute(create_a17)
cur.execute(create_a18)
cur.execute(create_a19)
cur.execute(create_a20)
cur.execute(create_a21)
cur.execute(create_a22)
cur.execute(create_a23)
cur.execute(create_a24)
cur.execute(create_a25)
cur.execute(create_a26)
cur.execute(create_a27)
cur.execute(create_a28)
cur.execute(create_a29)
cur.execute(create_a30)
cur.execute(create_a31)
cur.execute(create_a32)
cur.execute(create_a33)
cur.execute(create_a34)
#cur.execute(create_a35)
cur.execute(create_a36)
cur.execute(create_a37)
cur.execute(create_a38)
cur.execute(create_a39)
cur.execute(create_a40)
cur.execute(create_a41)
cur.execute(create_a42)
cur.execute(create_a43)


cur.execute(create_a17_mkec)
cur.execute(create_a18_fdia)
cur.execute(create_a20_ecosia_superset)

conn.commit()

cur.execute('''ALTER TABLE CALC_GEOM_locations_registration_versus_externalaudits ADD COLUMN distance_to_registration_m INTEGER;''')
conn.commit()

cur.execute('''SELECT * FROM CALC_GEOM_locations_registration_versus_externalaudits;''')
conn.commit()

rows = cur.fetchall()

for row in rows:
    instance = row[2]
    location_registration = (row[6:8])
    location_audit = (row[8:10])
    distance_m = (geodesic(location_registration, location_audit).m)

    cur.execute('''UPDATE CALC_GEOM_locations_registration_versus_externalaudits SET distance_to_registration_m = %s WHERE instance = %s;''', (distance_m, instance))
    conn.commit()

cur.close()
