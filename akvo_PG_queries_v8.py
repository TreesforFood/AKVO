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

drop_a1 = '''DROP TABLE IF EXISTS CALC_TAB_tree_submissions_per_contract;'''
drop_a2 = '''DROP TABLE IF EXISTS CALC_GEOM_PCQ_calculations_per_site_by_partner;'''
drop_a4 = '''DROP TABLE IF EXISTS CALC_TAB_Check_on_registered_polygons;'''
drop_a5 = '''DROP TABLE IF EXISTS CALC_GEOM_Error_check_on_registered_polygons;'''
drop_a6 = '''DROP TABLE IF EXISTS CALC_GEOM_PCQ_calculations_per_site_by_external_audit;'''
drop_a7 = '''DROP TABLE IF EXISTS CALC_GEOM_Trees_counted_per_site_by_external_audit;'''
drop_a8 = '''DROP TABLE IF EXISTS CALC_GEOM_AKVO_tree_registration_submissions_today;'''
drop_a9 = '''DROP TABLE IF EXISTS CALC_GEOM_AKVO_nursery_registration_submissions_today;'''
drop_a10 = '''DROP TABLE IF EXISTS CALC_GEOM_AKVO_check_photo_registrations;'''
drop_a11 = '''DROP TABLE IF EXISTS CALC_GEOM_AKVO_check_species_registrations;'''
drop_a12 = '''DROP TABLE IF EXISTS CALC_GEOM_locations_registration_versus_externalaudits;'''
drop_a13 = '''DROP TABLE IF EXISTS CALC_GEOM_Trees_counted_per_site_by_partner;'''
drop_a14 = '''DROP TABLE IF EXISTS CALC_TAB_overall_statistics;'''

conn.commit()

create_a1 = '''CREATE TABLE CALC_TAB_tree_submissions_per_contract
AS SELECT
country AS "Country",
organisation AS "Name organisation",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
count(*) AS "Number of site registrations",
"count_monitorings".count AS "Number of sites monitored",
"count_species".count AS "Number of registered tree species",
max(AKVO_Tree_registration_areas.submission) AS "Most recent registration submission",
"count_monitorings".latest_monitoring_submission AS "Most recent monitoring submission",
SUM(AKVO_Tree_registration_areas.tree_number) AS "Registered tree number at present"

FROM AKVO_Tree_registration_areas

LEFT JOIN (SELECT count(AKVO_Tree_monitoring_areas.identifier_akvo), AKVO_Tree_registration_areas.contract_number,
MAX(AKVO_Tree_monitoring_areas.submission) AS latest_monitoring_submission
FROM  akvo_tree_monitoring_areas JOIN akvo_tree_registration_areas ON
AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
GROUP BY AKVO_Tree_registration_areas.contract_number)
AS "count_monitorings" ON "count_monitorings".contract_number = AKVO_Tree_registration_areas.contract_number

LEFT JOIN (SELECT count(DISTINCT AKVO_Tree_registration_species.lat_name_species), AKVO_Tree_registration_areas.contract_number FROM AKVO_Tree_registration_species JOIN akvo_tree_registration_areas ON
AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_species.identifier_akvo
GROUP BY AKVO_Tree_registration_areas.contract_number)
AS "count_species" ON "count_species".contract_number = AKVO_Tree_registration_areas.contract_number

WHERE AKVO_Tree_registration_areas.test = '' OR AKVO_Tree_registration_areas.test = 'This is real, valid data'

GROUP BY
AKVO_Tree_registration_areas.contract_number,
AKVO_Tree_registration_areas.Country,
AKVO_Tree_registration_areas.Organisation,
"count_monitorings".count,
"count_species".count,
"count_monitorings".latest_monitoring_submission
ORDER BY AKVO_Tree_registration_areas.contract_number;'''
conn.commit()


# Works well
create_a2 = '''
--THIS IS THE MAIN QUERY TABLE WITH A SELF JOIN IN ORDER TO GROUP
CREATE TABLE CALC_GEOM_PCQ_calculations_per_site_by_partner AS

-- Create CTE with aggregation on instances. This is important to get 1 single label strata per instance
WITH pcq_instances_grouped AS (SELECT
akvo_tree_monitoring_pcq.identifier_akvo,
akvo_tree_monitoring_pcq.instance,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_monitoring_areas.submission,
akvo_tree_monitoring_areas.submission_year
FROM AKVO_Tree_monitoring_areas
JOIN akvo_tree_monitoring_pcq ON AKVO_Tree_monitoring_areas.instance = akvo_tree_monitoring_pcq.instance
JOIN AKVO_Tree_registration_areas ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
WHERE (AKVO_Tree_monitoring_areas.test = '' OR AKVO_Tree_monitoring_areas.test = 'This is real, valid data')
GROUP BY
akvo_tree_monitoring_pcq.identifier_akvo,
akvo_tree_monitoring_pcq.instance,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_monitoring_areas.submission,
akvo_tree_monitoring_areas.submission_year
ORDER BY akvo_tree_monitoring_areas.submission),

-- Create CTE to calculate relative distances between submission dates (for each instance)
table_label_strata AS (SELECT pcq_instances_grouped.identifier_akvo, pcq_instances_grouped.instance,
pcq_instances_grouped.submission,
CASE
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  <180 THEN CONCAT(pcq_instances_grouped.submission_year,'-180')
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  ISNULL THEN CONCAT(pcq_instances_grouped.submission_year,'-180')
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  Between 181 AND 360 THEN CONCAT(pcq_instances_grouped.submission_year,'-360')
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  Between 361 AND 540 THEN CONCAT(pcq_instances_grouped.submission_year,'-540')
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  Between 541 AND 720 THEN CONCAT(pcq_instances_grouped.submission_year,'-720')
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  Between 721 AND 900 THEN CONCAT(pcq_instances_grouped.submission_year,'-900')
WHEN (pcq_instances_grouped.submission-LAG(pcq_instances_grouped.submission)
OVER (PARTITION BY pcq_instances_grouped.identifier_akvo ORDER BY pcq_instances_grouped.submission ASC))
	  Between 901 AND 1080 THEN CONCAT(pcq_instances_grouped.submission_year,'-1080')
END AS monitoring_strata
FROM pcq_instances_grouped),

-- Create CTE to group instances on strata level. This table is used to group the main query table
pcq_instances_merged AS (
SELECT table_label_strata.instance, table_label_strata.monitoring_strata,
table_label_strata.submission
FROM table_label_strata
GROUP BY table_label_strata.instance, table_label_strata.monitoring_strata, table_label_strata.submission)

SELECT akvo_tree_monitoring_pcq.identifier_akvo,
MAX(pcq_instances_merged.submission) AS "Latest monitoring submission date",
pcq_instances_merged.monitoring_strata,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.organisation,
count(akvo_tree_monitoring_pcq.identifier_akvo) AS "Number of samples (4 trees/sample) taken in montoring period",
akvo_tree_registration_areas.calc_area,
ROUND(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2) AS "Average tree distance (m)",
ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000),0) AS "Monitored tree density (trees/ha)",
CAST(ROUND((((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0))*10000)*AKVO_Tree_registration_areas.calc_area),0) AS INT)
AS "Monitored total nr trees on site",
AKVO_Tree_registration_areas.polygon
FROM akvo_tree_monitoring_pcq
JOIN akvo_tree_registration_areas
ON akvo_tree_monitoring_pcq.identifier_akvo = akvo_tree_registration_areas.identifier_akvo
JOIN table_label_strata
ON table_label_strata.identifier_akvo = akvo_tree_monitoring_pcq.identifier_akvo
INNER JOIN
pcq_instances_merged
ON pcq_instances_merged.instance = akvo_tree_monitoring_pcq.instance
GROUP BY
akvo_tree_monitoring_pcq.identifier_akvo,
pcq_instances_merged.monitoring_strata,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.organisation,
akvo_tree_registration_areas.calc_area,
akvo_tree_registration_areas.id_planting_site,
AKVO_Tree_registration_areas.polygon

UNION ALL

SELECT
akvo_tree_registration_areas.identifier_akvo,
akvo_tree_registration_areas.submission AS "Registration submission date",
CONCAT(akvo_tree_registration_areas.submission_year,'-0') AS monitoring_strata,
akvo_tree_registration_areas.id_planting_site,
akvo_tree_registration_areas.contract_number,
akvo_tree_registration_areas.organisation,
NULL as number_of_samples,
akvo_tree_registration_areas.calc_area,
ROUND(100/NULLIF(SQRT(akvo_tree_registration_areas.tree_number/NULLIF(akvo_tree_registration_areas.calc_area,0)),0),2) AS "Average tree distance (m)",
ROUND((akvo_tree_registration_areas.tree_number/NULLIF(akvo_tree_registration_areas.calc_area,0)),0) AS "Registered tree density (trees/ha)",
akvo_tree_registration_areas.tree_number,
AKVO_Tree_registration_areas.polygon
FROM akvo_tree_registration_areas;'''

conn.commit()

create_a4 = '''CREATE TABLE CALC_TAB_Check_on_registered_polygons
AS SELECT
AKVO_Tree_registration_photos.identifier_akvo,
AKVO_Tree_registration_areas.instance,
AKVO_Tree_registration_areas.submitter AS "Submitter of registration",
AKVO_Tree_registration_areas.submission AS "Submission Date",
AKVO_Tree_registration_areas.organisation As "Name Organisation",
AKVO_Tree_registration_areas.country AS "Country",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.estimated_area AS "Estimated area (ha)",
calc_area AS "GIS calculated area (ha)",


(SELECT COUNT(*) FROM AKVO_Tree_registration_photos WHERE AKVO_Tree_registration_areas.polygon NOTNULL
AND AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_photos.identifier_akvo)
AS "Total number of photos taken",

(SELECT COUNT(*) FROM AKVO_Tree_registration_photos WHERE
AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_photos.identifier_akvo AND AKVO_Tree_registration_areas.polygon NOTNULL AND
AKVO_Tree_registration_photos.photo_location NOTNULL) AS "Number of photos with geotag",

(SELECT COUNT(*) FROM AKVO_Tree_registration_species WHERE AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_species.identifier_akvo
AND AKVO_Tree_registration_areas.polygon NOTNULL) AS "Number of tree species registered",

(SELECT COUNT(*) FROM AKVO_Tree_registration_species WHERE AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_species.identifier_akvo
AND AKVO_Tree_registration_areas.polygon NOTNULL
AND AKVO_Tree_registration_species.number_species = 0) AS "Number of tree species with 0 number",

ROUND(COUNT(*)/NULLIF(AKVO_Tree_registration_areas.calc_area,0),2) AS "Photo density (photos/ha)",

number_coord_polygon AS "Number of points in registered polygon",

ROUND(AKVO_Tree_registration_areas.tree_number/NULLIF(AKVO_Tree_registration_areas.calc_area,0),0) AS "Registered tree density (trees/ha)",

AKVO_Tree_registration_areas.polygon

FROM AKVO_Tree_registration_areas LEFT JOIN AKVO_Tree_registration_photos
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_registration_photos.identifier_akvo
LEFT JOIN AKVO_Tree_registration_species
ON AKVO_Tree_registration_species.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo
WHERE (AKVO_Tree_registration_areas.test = '' OR AKVO_Tree_registration_areas.test = 'This is real, valid data')
AND AKVO_Tree_registration_areas.polygon NOTNULL
GROUP BY AKVO_Tree_registration_areas.identifier_akvo, akvo_tree_registration_photos.identifier_akvo, akvo_tree_registration_areas.instance,
akvo_tree_registration_areas.submitter, akvo_tree_registration_areas.submission, akvo_tree_registration_areas.organisation, akvo_tree_registration_areas.country,
akvo_tree_registration_areas.id_planting_site, akvo_tree_registration_areas.contract_number, akvo_tree_registration_areas.estimated_area, akvo_tree_registration_areas.calc_area,
akvo_tree_registration_areas.polygon, akvo_tree_registration_areas.number_coord_polygon, akvo_tree_registration_areas.tree_number
ORDER BY AKVO_Tree_registration_areas.submission desc;'''

conn.commit()

create_a5 = '''CREATE TABLE CALC_GEOM_Error_check_on_registered_polygons AS
SELECT *,

CASE
WHEN "Total number of photos taken" < 4
THEN 'Too few photos taken. Should be at least 4'
WHEN "Photo density (photos/ha)" < 1 AND "Estimated area (ha)" < 35
THEN 'Too few photos taken. Should be at least 1 per hectare'
WHEN "Photo density (photos/ha)" < 1 AND "Estimated area (ha)" > 35 AND "Total number of photos taken" < 35
THEN 'Too few photos taken. At least 35 photos should have been taken'
ELSE 'Sufficient nr of photos have been taken for this site'
END AS "Check nr of photos",

CASE
WHEN "Total number of photos taken" > "Number of photos with geotag"
THEN 'Not all photos have a geotag'
ELSE 'All photos have a geotag'
END AS "Check geotag of photos",

CASE
WHEN "Number of points in registered polygon" < 4 AND "Number of points in registered polygon" > 0
THEN 'Polygons does not have enough points. Should be 4 at minimum but, preferably at least 8'
ELSE 'Sufficient nr of points (>=4) have been taken to map the area'
END AS "Check nr of points in polygon",

CASE
WHEN "Registered tree density (trees/ha)" > 2000
THEN 'Registered tree density seems high (>2000 trees/ha). Confirm if this is correct'
ELSE 'Tree density seems within reasonable limits (<2000 trees/ha)'
END AS "Check on tree density"

FROM CALC_TAB_Check_on_registered_polygons
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
					 GROUP BY identifier_akvo)

SELECT
-- Implement results from registrations
AKVO_Tree_registration_areas.centroid_coord,
AKVO_Tree_registration_areas.identifier_akvo AS "Identifier AKVO",
AKVO_Tree_external_audits_pcq.instance AS "Audit instance",
AKVO_Tree_registration_areas.organisation AS "Name organisation",
AKVO_Tree_registration_areas.submitter AS "Submitter of registration data",
AKVO_Tree_external_audits_areas.submitter AS "Name auditor",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.estimated_area AS "Estimated area registered polygon",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.calc_area AS "Calculated area registered polygon",
AKVO_Tree_external_audits_areas.calc_area AS "Calculated area of audit polygon",
AKVO_Tree_registration_areas.tree_number AS "Registered nr trees by partner",

CASE
WHEN AKVO_Tree_registration_areas.calc_area NOTNULL AND AKVO_Tree_registration_areas.tree_number NOTNULL
THEN CAST(SQRT(AKVO_Tree_registration_areas.calc_area*10000)/NULLIF(SQRT(AKVO_Tree_registration_areas.tree_number),0) AS NUMERIC(8,2))
END AS "Registered avg tree distance (m)",

-- Implement results from latest monitoring for the sites (if any)
CASE
WHEN MAX(CALC_GEOM_PCQ_calculations_per_site_by_partner."Latest monitoring submission date") NOTNULL
THEN CALC_GEOM_PCQ_calculations_per_site_by_partner."Monitored tree density (trees/ha)"
END AS "Latest monitored tree density (trees/ha)",

ROUND(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2) AS "Average audited tree distance",

ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000),0) AS "Audited tree density (trees/ha)",

CASE
WHEN AKVO_Tree_external_audits_areas.calc_area NOTNULL
THEN ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_external_audits_areas.calc_area,0)),0)
ELSE ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_registration_areas.calc_area,0)),0)
END AS "Total audited nr trees for this site",

CASE
WHEN AKVO_Tree_external_audits_areas.calc_area NOTNULL
THEN ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_external_audits_areas.calc_area,0)/NULLIF(AKVO_Tree_registration_areas.tree_number,0)*100),0)
ELSE ROUND((1/NULLIF(POWER(((AVG(Q1_dist) + AVG(Q2_dist) + AVG(Q3_dist) + AVG(Q4_dist))/4),2),0)*10000*NULLIF(AKVO_Tree_registration_areas.calc_area,0)/NULLIF(AKVO_Tree_registration_areas.tree_number,0)*100),0)
END AS "% survived trees",

species_audited."Number of species audited",
species_audited."Species audited"

FROM AKVO_Tree_external_audits_areas
JOIN AKVO_Tree_external_audits_pcq ON AKVO_Tree_external_audits_areas.identifier_akvo = AKVO_Tree_external_audits_pcq.identifier_akvo
JOIN AKVO_Tree_registration_areas ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
LEFT JOIN species_audited
ON AKVO_Tree_external_audits_areas.identifier_akvo = species_audited.identifier_akvo
LEFT JOIN CALC_GEOM_PCQ_calculations_per_site_by_partner
ON CALC_GEOM_PCQ_calculations_per_site_by_partner.identifier_akvo = AKVO_Tree_registration_areas.identifier_akvo

GROUP BY AKVO_Tree_external_audits_pcq.instance, akvo_tree_registration_areas.centroid_coord,
akvo_tree_registration_areas.identifier_akvo, akvo_tree_registration_areas.organisation,
AKVO_Tree_registration_areas.submitter, AKVO_Tree_external_audits_areas.submitter,
AKVO_Tree_registration_areas.id_planting_site, AKVO_Tree_registration_areas.estimated_area,
AKVO_Tree_registration_areas.contract_number, AKVO_Tree_registration_areas.calc_area,
AKVO_Tree_external_audits_areas.calc_area,AKVO_Tree_registration_areas.tree_number,
species_audited."Species audited",
species_audited."Number of species audited",
CALC_GEOM_PCQ_calculations_per_site_by_partner."Monitored tree density (trees/ha)"

ORDER BY AKVO_Tree_registration_areas.organisation, AKVO_Tree_registration_areas.id_planting_site;'''

conn.commit()

create_a7 = '''CREATE TABLE CALC_GEOM_Trees_counted_per_site_by_external_audit
AS SELECT AKVO_Tree_registration_areas.centroid_coord,
AKVO_Tree_registration_areas.identifier_akvo,
AKVO_Tree_registration_areas.organisation AS "Name organisation",
AKVO_Tree_registration_areas.submitter AS "Name submitter registration data",
AKVO_Tree_external_audits_areas.submitter AS "Name auditor",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.tree_number AS "Nr. trees registered",
AKVO_Tree_external_audits_areas.manual_tree_count AS "Nr. trees counted by auditor"
FROM AKVO_Tree_registration_areas JOIN AKVO_Tree_external_audits_areas
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
WHERE AKVO_Nursery_registration.submission = current_date;'''

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
where AKVO_Tree_registration_areas.contract_number = 115
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

), summary_stats_nurseries AS(
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

SELECT
1 AS sno, 'Total tree registration instances in database' AS statistic, total_instances AS value
FROM summary_stats_trees
UNION
SELECT 2, 'Total tree monitoring instances in database', total_sites_monitored
FROM summary_stats_monitoring
UNION
SELECT 3, 'Total number of trees in database', total_tree_number
FROM summary_stats_trees
UNION
SELECT 4, 'Largest estimated site in database (ha)', largest_area_ha
FROM summary_stats_trees
UNION
SELECT 5, 'Smalles estimated site in database (ha)', smallest_area_ha
FROM summary_stats_trees
UNION
SELECT 6, '50th percentile of area size', percentile_50
FROM summary_stats_trees
UNION
SELECT 7, '90th percentile of area size', percentile_90
FROM summary_stats_trees
UNION
SELECT 8, '95th percentile of area size', percentile_95
FROM summary_stats_trees
UNION
SELECT 9, '99th percentile of area size', percentile_99
FROM summary_stats_trees
UNION
SELECT 10, 'Number of nursery registrations', total_nursery_instances
FROM summary_stats_nurseries
UNION
SELECT 11, 'Number of tree species', number_of_species
FROM summary_stats_species
UNION
SELECT 12, 'Number of registered photos', number_of_photos_registered
FROM summary_stats_photos)
SELECT * FROM row_summary_stats
ORDER BY sno
;'''

conn.commit()

# Execute create tables
cur.execute(drop_a1)
cur.execute(drop_a2)
cur.execute(drop_a4)
cur.execute(drop_a5)
cur.execute(drop_a6)
cur.execute(drop_a7)
cur.execute(drop_a8)
cur.execute(drop_a9)
cur.execute(drop_a10)
cur.execute(drop_a11)
cur.execute(drop_a12)
cur.execute(drop_a13)
cur.execute(drop_a14)

cur.execute(create_a1)
cur.execute(create_a2)
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

conn.commit()

#cur.close()

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
