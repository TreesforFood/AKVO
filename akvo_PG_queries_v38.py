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

drop_tables = '''
DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated;
DROP TABLE IF EXISTS CALC_TAB_monitoring_calculations_per_site_by_partner;
DROP TABLE IF EXISTS CALC_TAB_monitoring_calculations_per_site;
DROP TABLE IF EXISTS CALC_TAB_linear_regression_results;
DROP TABLE IF EXISTS CALC_TAB_overall_statistics;
DROP TABLE IF EXISTS CALC_TAB_tree_submissions_per_contract;
DROP TABLE IF EXISTS akvo_ecosia_contract_overview;
DROP TABLE IF EXISTS superset_ecosia_nursery_registration;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_polygon;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_point;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_species;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring;
DROP TABLE IF EXISTS superset_ecosia_nursery_registration_pictures;
DROP TABLE IF EXISTS superset_ecosia_nursery_registration_photos;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_pictures;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_photos;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_pictures;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_photos;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_species;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_species;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_pictures;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_photos;
DROP TABLE IF EXISTS superset_ecosia_tree_registration;
DROP TABLE IF EXISTS superset_ecosia_geolocations;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_light;
DROP TABLE IF EXISTS superset_ecosia_tree_distribution_unregistered_farmers;
DROP TABLE IF EXISTS superset_ecosia_site_registration_unregistered_farmers;
DROP TABLE IF EXISTS superset_ecosia_contract_overview;
DROP TABLE IF EXISTS AKVO_tree_registration_areas_updated_KANOP;
DROP TABLE IF EXISTS CALC_TAB_Error_partner_report_on_nursery_registration;
DROP TABLE IF EXISTS CALC_TAB_Error_check_on_nursery_registration;'''
conn.commit()


create_a1 = '''
CREATE TABLE akvo_tree_registration_areas_updated
AS

-- JOIN the ODK registration table, the tree distribution table (of unregistered farmers) and ('normal') tree registration table to get all columns for the registration table.

WITH join_tree_distribution_and_registration AS (SELECT
a.identifier_akvo,
a.displayname,
a.device_id,
a.instance,
a.submission_date,
a.submitter,
a.form_version,
'AKVO' AS source_data,
b.country,
b.organisation,
b.contract_number,
a.test,
a.name_region_village_planting_site,
a.name_owner_planting_site,
a.photo_owner_planting_site,
a.gender_owner_planting_site,
a.comment_enumerator,
a.more_less_200_trees,
a.date_tree_planting,
a.estimated_tree_number_planted,
a.estimated_area,
a.area_ha,
a.number_coord_pol,
a.centroid_coord,
a.polygon,
a.confirm_plant_location_own_land,
a.one_multiple_planting_sites,
a.nr_trees_given_away,
a.nr_trees_received,
b.name_site_id_tree_planting,
b.check_ownership_land,
b.url_photo_receiver_trees,
b.location_house_tree_receiver,
b.confirm_planting_location,
b.url_signature_tree_receiver,
b.total_number_trees_received,
b.check_ownership_trees,
b.gender_tree_receiver,
b.name_tree_receiver

FROM akvo_site_registration_distributed_trees AS a
INNER JOIN akvo_tree_distribution_unregistered_farmers AS b
ON a.identifier_akvo = b.identifier_akvo),

-- Merge (UNION) all data from the akvo tree registration form with the data from the tree registration of unregistered farmers
-- Many columns are listed here to align all column data and make sure the table remains as input for following queries

union_tree_registration_tree_registration_unreg_farmers AS (
SELECT
c.identifier_akvo,
c.display_name,
c.device_id,
c.instance,
c.submission,
c.submission_year,
c.submissiontime,
c.submitter,
c.modifiedat,
c.akvo_form_version::varchar(10),
'AKVO' AS source_data,
c.country,
c.test,
c.organisation,
c.contract_number,
c.id_planting_site,
c.land_title,
c.name_village,
c.name_region,
c.name_owner,
c.photo_owner,
c.gender_owner,
c.objective_site,
c.site_preparation,
c.planting_technique,
c.planting_system,
c.remark,
c.nr_trees_option,
c.planting_date,
c.tree_number,
c.estimated_area,
c.calc_area,
c.lat_y,
c.lon_x,
c.number_coord_polygon,
c.centroid_coord,
c.polygon,
c.multipoint,
'normal tree registration' AS data_source,
'n/a' AS confirm_plant_location_own_land,
'n/a' AS one_multiple_planting_sites,
0 AS nr_trees_given_away,
0 AS nr_trees_received,
'n/a' AS url_photo_receiver_trees,
'n/a' AS location_house_tree_receiver,
'n/a' AS confirm_planting_location,
'n/a' AS url_signature_tree_receiver,
0 AS total_number_trees_received,
'n/a' AS check_ownership_trees,
'n/a' AS gender_tree_receiver,
'n/a' AS name_tree_receiver
FROM akvo_tree_registration_areas AS c

	UNION ALL

SELECT
d.identifier_akvo,
d.displayname,
d.device_id,
d.instance,
d.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,
d.submitter,
'n/a' AS modifiedat,
d.form_version::varchar(10),
'AKVO' AS source_data,
d.country,
d.test,
d.organisation,
d.contract_number,
d.name_site_id_tree_planting,
d.check_ownership_land,
d.name_region_village_planting_site,
'n/a' AS name_region,
d.name_owner_planting_site,
d.photo_owner_planting_site,
d.gender_owner_planting_site,
'n/a' AS objective_site,
'n/a' AS site_preparation,
'n/a' AS planting_technique,
'n/a' AS planting_system,
d.comment_enumerator,
d.more_less_200_trees,
d.date_tree_planting,
d.estimated_tree_number_planted AS tree_number,
d.estimated_area,
d.area_ha,
NULL AS lat_y,
NULL AS lon_x,
d.number_coord_pol,
d.centroid_coord,
d.polygon,
NULL AS multipoint,
'tree registration unregistered farmers' AS data_source,
d.confirm_plant_location_own_land,
d.one_multiple_planting_sites,
d.nr_trees_given_away,
d.nr_trees_received,
d.url_photo_receiver_trees,
d.location_house_tree_receiver,
d.confirm_planting_location,
d.url_signature_tree_receiver,
d.total_number_trees_received,
d.check_ownership_trees,
d.gender_tree_receiver,
d.name_tree_receiver

FROM join_tree_distribution_and_registration AS d

UNION ALL

SELECT
h.submissionid_odk as identifier_akvo,
CONCAT(h.contract_number,' - ', h.id_planting_site, ' - ', h.name_owner) AS displayname,
h.device_id,
0 AS instance,
h.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,
h.submitter,
'n/a' AS modifiedat,
h.odk_form_version AS form_version,
'ODK' AS source_data,
h.country,

CASE
WHEN h.test = 'test_data'
THEN ''
WHEN h.test = 'valid_data'
THEN 'This is real, valid data'
END AS test,

h.organisation,
h.contract_number,
h.id_planting_site AS name_site_id_tree_planting,
h.land_title AS check_ownership_land,
'n/a' AS name_region_village_planting_site,
'n/a' AS name_region,
h.name_owner AS name_owner_planting_site,
h.photo_owner AS photo_owner_planting_site,
h.gender_owner AS gender_owner_planting_site,
'n/a' AS objective_site,
'n/a' AS site_preparation,
h.planting_technique,
'n/a' AS planting_system,
h.remark AS comment_enumerator,
h.nr_trees_option AS more_less_200_trees,
h.planting_date AS planting_date,
h.tree_number AS tree_number,
h.calc_area AS estimated_area,
h.calc_area AS area_ha,
NULL AS lat_y,
NULL AS lon_x,
6 AS number_coord_pol,
h.centroid_coord,
h.polygon,
NULL AS confirm_plant_location_own_land,
'n/a' AS one_multiple_planting_sites,
'n/a' AS confirm_plant_location_own_land,
'n/a' AS one_multiple_planting_sites,
0 AS nr_trees_given_away,
0 AS nr_trees_received,
'n/a' AS url_photo_receiver_trees,
'n/a' AS location_house_tree_receiver,
'n/a' AS confirm_planting_location,
'n/a' AS url_signature_tree_receiver,
0 AS total_number_trees_received,
'n/a' AS check_ownership_trees,
'n/a' AS gender_tree_receiver,
'n/a' AS name_tree_receiver

FROM odk_tree_registration_main AS h)

SELECT * FROM union_tree_registration_tree_registration_unreg_farmers
WHERE organisation != '';

-- Add GEOMETRIC CHECK columns so that they can later be populated
ALTER TABLE akvo_tree_registration_areas_updated
ADD species_latin text,

--The columns below are UPDATED with the geometric error script (AKVO_database_PG_queries_v1_sql_geometric_error_detection.py)
ADD self_intersection BOOLEAN,
ADD overlap TEXT,
ADD outside_country BOOLEAN,
ADD check_200_trees BOOLEAN,
ADD check_duplicate_polygons TEXT,
ADD needle_shape BOOLEAN,
ADD total_nr_geometric_errors INTEGER;


-- Transpose all species from multiple rows into 1 row
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

WITH updates_polygon AS (SELECT identifier_akvo, polygon_remapped
FROM akvo_tree_monitoring_remapped_areas
WHERE polygon_remapped NOTNULL
order by submission DESC
LIMIT 1)

UPDATE akvo_tree_registration_areas_updated
SET
polygon = updates_polygon.polygon_remapped
FROM updates_polygon
WHERE akvo_tree_registration_areas_updated.identifier_akvo = updates_polygon.identifier_akvo;

UPDATE akvo_tree_registration_areas_updated
SET
number_coord_polygon = akvo_tree_monitoring_remapped_areas.number_coord_polygon_remapped
FROM akvo_tree_monitoring_remapped_areas
WHERE akvo_tree_registration_areas_updated.identifier_akvo
= akvo_tree_monitoring_remapped_areas.identifier_akvo
AND akvo_tree_monitoring_remapped_areas.polygon_remapped NOTNULL;

UPDATE akvo_tree_registration_areas_updated
SET calc_area = 0.2
WHERE polygon ISNULL

-- Correct polygons with self-intersections
-- UPDATE akvo_tree_registration_areas_updated
-- SET polygon_corr_self_intersections = ST_buffer(polygon,0.0)
-- WHERE polygon NOTNULL
-- AND self_intersection = true

;'''

conn.commit()


# Works well
create_a2 = '''CREATE TABLE CALC_TAB_monitoring_calculations_per_site AS

--- Plot dates of all MAIN TAB monitorings (COUNTS AND PCQ's)
WITH plot_dates_monitorings AS (SELECT
akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_monitoring_areas.submission AS monitoring_submission,

-- Classify the options is needed to prevent seperate groups
CASE
WHEN akvo_tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
THEN 'PCQ'
WHEN akvo_tree_monitoring_areas.method_selection = 'The trees were counted'
THEN 'Tree count'
WHEN akvo_tree_monitoring_areas.method_selection = 'We used our own inventory method'
THEN 'Own method'
ELSE 'Unknown'
END AS method_selection,

'monitoring_data' AS procedure,
TO_DATE(akvo_tree_registration_areas_updated.planting_date, 'YYYY-MM-DD') AS planting_date
FROM akvo_tree_monitoring_areas
LEFT JOIN akvo_tree_registration_areas_updated
ON akvo_tree_monitoring_areas.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo),

-- Plot dates of all MAIN TAB audits (COUNTS AND PCQ's)
plot_dates_audits AS (SELECT
akvo_tree_external_audits_areas.identifier_akvo,
akvo_tree_external_audits_areas.instance,
akvo_tree_external_audits_areas.test AS test_monitoring,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_external_audits_areas.submission AS monitoring_submission,

-- Classify the options is needed to prevent seperate groups
CASE
WHEN AKVO_Tree_external_audits_areas.option_tree_count = 'More than 200 trees planted. Determine tree number with PCQ method'
OR AKVO_Tree_external_audits_areas.option_tree_count = 'I want to use the PCQ method anyway'
THEN 'PCQ'
ELSE 'Tree count'
END AS method_selection,

'audit_data' AS procedure,
TO_DATE(akvo_tree_registration_areas_updated.planting_date, 'YYYY-MM-DD') AS planting_date
FROM akvo_tree_external_audits_areas
LEFT JOIN akvo_tree_registration_areas_updated
ON akvo_tree_external_audits_areas.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo),

-- Combine date results from audits and monitorings (COUNTS AND PCQ's)
combine_monitorings_audits AS (
SELECT * FROM plot_dates_monitorings
UNION ALL
SELECT * FROM plot_dates_audits),

-- Calculate time differences between planting date and audits/monitorings and give them a label strata (on instance level)
table_label_strata AS (SELECT
combine_monitorings_audits.identifier_akvo,
combine_monitorings_audits.instance,
combine_monitorings_audits.test,
combine_monitorings_audits.method_selection,
combine_monitorings_audits.procedure,
combine_monitorings_audits.planting_date,
combine_monitorings_audits.monitoring_submission AS submission,
combine_monitorings_audits.monitoring_submission - planting_date AS difference_days_reg_monitoring,
CAST((combine_monitorings_audits.monitoring_submission - planting_date)*1.0/365 * 1.0 AS DECIMAL(7,1)) AS difference_years_reg_monitoring,

-- Calculate label-strata. Include rule that if a monitoring and a registration is carried out on the same day (= 0 days difference)
-- the label-strata for that monitoring instance receives a value 180 (and not 0)
CASE
WHEN CEILING((combine_monitorings_audits.monitoring_submission - planting_date)*1.0/180)*180 > 0
THEN CEILING((combine_monitorings_audits.monitoring_submission - planting_date)*1.0/180)*180
ELSE 180
END AS label_strata

FROM combine_monitorings_audits
order by identifier_akvo),

-- List number of PCQ samples of audits and monitorings
list_pcq_samples AS (SELECT
identifier_akvo, instance
FROM akvo_tree_monitoring_pcq
UNION ALL
SELECT identifier_akvo, instance
FROM akvo_tree_external_audits_pcq),

-- Calculate number of PCQ samples of audits and monitorings
count_pcq_samples AS (SELECT
list_pcq_samples.identifier_akvo, table_label_strata.label_strata,
COUNT(list_pcq_samples.instance) AS number_pcq_samples
FROM list_pcq_samples
LEFT JOIN table_label_strata
ON table_label_strata.instance = list_pcq_samples.instance
GROUP BY
list_pcq_samples.identifier_akvo,
table_label_strata.label_strata),


-- Get all PCQ MONITORIG DISTANCE values into 1 column in order to calculate the average
merge_pcq_monitoring_dist AS (select
identifier_akvo,
instance,
Q1_dist AS pcq_results_merged_monitoring
FROM AKVO_Tree_monitoring_pcq
UNION ALL
Select identifier_akvo,
instance,
Q2_dist AS pcq_results_merged_monitoring
FROM AKVO_Tree_monitoring_pcq
UNION ALL
Select identifier_akvo,
instance,
Q3_dist AS pcq_results_merged_monitoring
FROM AKVO_Tree_monitoring_pcq
UNION ALL
Select identifier_akvo,
instance,
Q4_dist AS pcq_results_merged_monitoring
FROM AKVO_Tree_monitoring_pcq),

-- Get all PCQ MONITORIG HEIGHT values into 1 column in order to calculate the average
merge_pcq_monitoring_hgt AS (select
identifier_akvo,
instance,
Q1_hgt AS pcq_results_merged_monitoring_hgt
FROM AKVO_Tree_monitoring_pcq
UNION ALL
Select identifier_akvo,
instance,
Q2_hgt AS pcq_results_merged_monitoring_hgt
FROM AKVO_Tree_monitoring_pcq
UNION ALL
Select identifier_akvo,
instance,
Q3_hgt AS pcq_results_merged_monitoring_hgt
FROM AKVO_Tree_monitoring_pcq
UNION ALL
Select identifier_akvo,
instance,
Q4_hgt AS pcq_results_merged_monitoring_hgt
FROM AKVO_Tree_monitoring_pcq),

-- Calculate average DISTANCES from PCQ monitoring values
pcq_monitoring_avg_dist AS (SELECT
merge_pcq_monitoring_dist.identifier_akvo, table_label_strata.label_strata,
AVG(pcq_results_merged_monitoring) AS pcq_results_merged_monitoring
FROM merge_pcq_monitoring_dist
JOIN table_label_strata
ON merge_pcq_monitoring_dist.instance = table_label_strata.instance
WHERE merge_pcq_monitoring_dist.pcq_results_merged_monitoring > 0
AND merge_pcq_monitoring_dist.pcq_results_merged_monitoring < 200
GROUP BY merge_pcq_monitoring_dist.identifier_akvo,
--table_label_strata.instance,
table_label_strata.label_strata),

-- Calculate average HEIGHT from PCQ monitoring values
pcq_monitoring_avg_hgt AS (SELECT
merge_pcq_monitoring_hgt.identifier_akvo, table_label_strata.label_strata,
AVG(pcq_results_merged_monitoring_hgt) AS pcq_results_merged_monitoring_hgt
FROM merge_pcq_monitoring_hgt
JOIN table_label_strata
ON merge_pcq_monitoring_hgt.instance = table_label_strata.instance
WHERE merge_pcq_monitoring_hgt.pcq_results_merged_monitoring_hgt > 0
AND merge_pcq_monitoring_hgt.pcq_results_merged_monitoring_hgt < 100
GROUP BY merge_pcq_monitoring_hgt.identifier_akvo,
--table_label_strata.instance,
table_label_strata.label_strata),


-- Get all PCQ AUDIT DISTANCE values into 1 column in order to calculate the average
merge_pcq_audit_dist AS (select
identifier_akvo, instance,
q1_dist AS pcq_results_merged_audit
FROM AKVO_Tree_external_audits_pcq
UNION ALL
Select identifier_akvo, instance,
q2_dist AS pcq_results_merged_audit
FROM AKVO_Tree_external_audits_pcq
UNION ALL
Select identifier_akvo, instance,
q3_dist AS pcq_results_merged_audit
FROM AKVO_Tree_external_audits_pcq
UNION ALL
Select identifier_akvo, instance,
q4_dist AS pcq_results_merged_audit
FROM AKVO_Tree_external_audits_pcq),

-- Get all PCQ AUDIT HEIGHT values into 1 column in order to calculate the average
merge_pcq_audit_hgt AS (select
identifier_akvo, instance,
q1_hgt AS pcq_results_merged_audit_hgt
FROM AKVO_Tree_external_audits_pcq
UNION ALL
Select identifier_akvo, instance,
q2_hgt AS pcq_results_merged_audit_hgt
FROM AKVO_Tree_external_audits_pcq
UNION ALL
Select identifier_akvo, instance,
q3_hgt AS pcq_results_merged_audit_hgt
FROM AKVO_Tree_external_audits_pcq
UNION ALL
Select identifier_akvo, instance,
q4_hgt AS pcq_results_merged_audit_hgt
FROM AKVO_Tree_external_audits_pcq),

-- Calculate average DISTANCES from PCQ audit values
pcq_audit_avg_dist AS (SELECT
merge_pcq_audit_dist.identifier_akvo, table_label_strata.label_strata,
AVG(pcq_results_merged_audit) AS pcq_results_merged_audit
FROM merge_pcq_audit_dist
JOIN table_label_strata
ON merge_pcq_audit_dist.instance = table_label_strata.instance
WHERE merge_pcq_audit_dist.pcq_results_merged_audit > 0
AND merge_pcq_audit_dist.pcq_results_merged_audit < 200
GROUP BY merge_pcq_audit_dist.identifier_akvo,
table_label_strata.label_strata),

-- Calculate average HEIGHT from PCQ audit values
pcq_audit_avg_hgt AS (SELECT
merge_pcq_audit_hgt.identifier_akvo, table_label_strata.label_strata,
AVG(pcq_results_merged_audit_hgt) AS pcq_results_merged_audit_hgt
FROM merge_pcq_audit_hgt
JOIN table_label_strata
ON merge_pcq_audit_hgt.instance = table_label_strata.instance
WHERE merge_pcq_audit_hgt.pcq_results_merged_audit_hgt > 0
AND merge_pcq_audit_hgt.pcq_results_merged_audit_hgt < 100
GROUP BY merge_pcq_audit_hgt.identifier_akvo,
table_label_strata.label_strata),

-- Calculate AVERAGE tree count results for the MONITORING COUNTS (in case multiple COUNTS are carried out in the same label_strata)
count_monitoring_avg_count AS (SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.instance,
--AKVO_Tree_monitoring_counts.instance,
table_label_strata.label_strata,

CASE
WHEN
SUM(AKVO_Tree_monitoring_counts.number_species) NOTNULL AND SUM(AKVO_Tree_monitoring_areas.number_living_trees) ISNULL
THEN SUM(AKVO_Tree_monitoring_counts.number_species)
WHEN SUM(AKVO_Tree_monitoring_counts.number_species) ISNULL AND SUM(AKVO_Tree_monitoring_areas.number_living_trees) NOTNULL
THEN AKVO_Tree_monitoring_areas.number_living_trees
WHEN SUM(AKVO_Tree_monitoring_counts.number_species) NOTNULL AND SUM(AKVO_Tree_monitoring_areas.number_living_trees) NOTNULL
THEN SUM(AKVO_Tree_monitoring_counts.number_species)
WHEN SUM(AKVO_Tree_monitoring_counts.number_species) ISNULL AND SUM(AKVO_Tree_monitoring_areas.number_living_trees) ISNULL
THEN NULL
END AS nr_trees_monitored

FROM AKVO_Tree_monitoring_areas
JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance
LEFT JOIN akvo_tree_monitoring_counts
ON akvo_tree_monitoring_counts.instance = AKVO_Tree_monitoring_areas.instance
GROUP BY
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.instance,
--akvo_tree_monitoring_counts.instance,
AKVO_Tree_monitoring_areas.number_living_trees,
table_label_strata.label_strata),

-- Calculate AVERAGE tree count results for the AUDIT COUNTS (in case multiple COUNTS are carried out in the same label_strata)
count_audit_avg_count AS (SELECT
akvo_tree_external_audits_areas.identifier_akvo,
akvo_tree_external_audits_areas.instance,
table_label_strata.label_strata,

CASE
WHEN
SUM(akvo_tree_external_audits_counts.number_species) NOTNULL
THEN SUM(akvo_tree_external_audits_counts.number_species)
WHEN SUM(akvo_tree_external_audits_counts.number_species) ISNULL AND akvo_tree_external_audits_areas.audit_reported_trees NOTNULL
THEN akvo_tree_external_audits_areas.audit_reported_trees
ELSE NULL
END AS nr_trees_monitored

FROM akvo_tree_external_audits_areas
JOIN table_label_strata
ON akvo_tree_external_audits_areas.instance = table_label_strata.instance
LEFT JOIN akvo_tree_external_audits_counts
ON akvo_tree_external_audits_counts.instance = akvo_tree_external_audits_areas.instance
GROUP BY
akvo_tree_external_audits_areas.identifier_akvo,
akvo_tree_external_audits_areas.instance,
table_label_strata.label_strata,
akvo_tree_external_audits_areas.audit_reported_trees),


--List the enumerators. Some monitoring submissions were done at the same time by different submittors.
--We need to bundle them into 1 column so they don't appear as seperate submissions in the result table
submittors_monitoring AS (SELECT akvo_tree_monitoring_areas.identifier_akvo, table_label_strata.label_strata, STRING_AGG(akvo_tree_monitoring_areas.submitter,' | ') AS submitter
FROM akvo_tree_monitoring_areas
JOIN table_label_strata
ON akvo_tree_monitoring_areas.instance = table_label_strata.instance
GROUP BY akvo_tree_monitoring_areas.identifier_akvo,
table_label_strata.label_strata),

--List the enumerators. Some monitoring submissions were done at the same time by different submittors.
--We need to bundle them into 1 column so they don't appear as seperate submissions in the result table
submittors_audit AS (SELECT akvo_tree_external_audits_areas.identifier_akvo, table_label_strata.label_strata, STRING_AGG(akvo_tree_external_audits_areas.submitter,' | ') AS submitter
FROM akvo_tree_external_audits_areas
JOIN table_label_strata
ON akvo_tree_external_audits_areas.instance = table_label_strata.instance
GROUP BY akvo_tree_external_audits_areas.identifier_akvo,
table_label_strata.label_strata),

-- Group the "site impressions" from MONITORINGS on label_strata level + identifier. This enables
-- to add site impressions to the each label strata (for a specific monitoring period)
site_impressions_monitoring AS (SELECT akvo_tree_monitoring_areas.identifier_akvo, table_label_strata.label_strata, STRING_AGG(akvo_tree_monitoring_areas.site_impression,' | ') AS site_impressions
FROM akvo_tree_monitoring_areas
JOIN table_label_strata
ON akvo_tree_monitoring_areas.instance = table_label_strata.instance
GROUP BY akvo_tree_monitoring_areas.identifier_akvo,
table_label_strata.label_strata),

-- Group the "site impressions" from AUDITS on label_strata level + identifier. This enables
-- to add all site impressions to the each label strata (for a specific monitoring period)
site_impressions_audit AS (SELECT akvo_tree_external_audits_areas.identifier_akvo, table_label_strata.label_strata, STRING_AGG(akvo_tree_external_audits_areas.impression_site,' | ') AS site_impressions
FROM akvo_tree_external_audits_areas
JOIN table_label_strata
ON akvo_tree_external_audits_areas.instance = table_label_strata.instance
GROUP BY akvo_tree_external_audits_areas.identifier_akvo,
table_label_strata.label_strata),

-- Sub CTE table to calculate PCQ MONITORING results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this temporary CTE table is more easy.
calc_interm_results_tree_numbers_pcq_monitoring AS (
SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.display_name,
AKVO_Tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_monitoring.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Monitoring' AS procedure,
'PCQ' AS data_collection_method,

ROUND(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2) AS avg_tree_distance_m,

ROUND((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000),0) AS avg_tree_density,

ROUND((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * Akvo_tree_registration_areas_updated.calc_area,0) as nr_trees_monitored,

count_pcq_samples.number_pcq_samples,

Akvo_tree_registration_areas_updated.planting_date AS planting_date,

MAX(table_label_strata.submission) AS latest_monitoring_submission,

MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

CASE
WHEN AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
THEN ROUND(((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * Akvo_tree_registration_areas_updated.calc_area)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,2)
ELSE ROUND(((SUM(AKVO_Tree_monitoring_areas.number_living_trees)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0))*100),0)
END AS perc_trees_survived,

CASE
WHEN AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
THEN ROUND(pcq_results_merged_monitoring_hgt,2)
ElSE AVG(akvo_tree_monitoring_areas.avg_tree_height)
END AS avg_tree_height,

site_impressions_monitoring.site_impressions

FROM AKVO_Tree_monitoring_areas
LEFT JOIN AKVO_Tree_monitoring_pcq
ON AKVO_Tree_monitoring_areas.instance = AKVO_Tree_monitoring_pcq.instance
LEFT JOIN Akvo_tree_registration_areas_updated
ON AKVO_Tree_monitoring_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance
LEFT JOIN pcq_monitoring_avg_dist
ON pcq_monitoring_avg_dist.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND pcq_monitoring_avg_dist.label_strata = table_label_strata.label_strata
LEFT JOIN pcq_monitoring_avg_hgt
ON pcq_monitoring_avg_hgt.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND pcq_monitoring_avg_hgt.label_strata = table_label_strata.label_strata
LEFT JOIN count_pcq_samples
ON count_pcq_samples.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND count_pcq_samples.label_strata = table_label_strata.label_strata
LEFT JOIN submittors_monitoring
ON submittors_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND submittors_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_monitoring
ON site_impressions_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND site_impressions_monitoring.label_strata = table_label_strata.label_strata

where AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.organisation,
submittors_monitoring.submitter,
site_impressions_monitoring.site_impressions,
akvo_tree_registration_areas_updated.contract_number,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
count_pcq_samples.number_pcq_samples,
Akvo_tree_registration_areas_updated.planting_date,
AKVO_Tree_monitoring_areas.method_selection,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
pcq_monitoring_avg_dist.pcq_results_merged_monitoring,
pcq_monitoring_avg_hgt.pcq_results_merged_monitoring_hgt,
AKVO_Tree_monitoring_areas.display_name),

-- Sub CTE table to calculate PCQ AUDIT results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issue of multiple rows combined with grouping problems. This is why this tenmporary CTE table is more easy.
calc_interm_results_tree_numbers_pcq_audit AS (SELECT
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.display_name,
AKVO_Tree_external_audits_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_audit.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Audit' AS procedure,
'PCQ' AS data_collection_method,

ROUND(pcq_audit_avg_dist.pcq_results_merged_audit,2) AS avg_tree_distance_m,

ROUND((1/(NULLIF(POWER(pcq_audit_avg_dist.pcq_results_merged_audit,2),0))*10000),0) AS avg_audit_tree_density,

ROUND((1/(NULLIF(POWER(pcq_audit_avg_dist.pcq_results_merged_audit,2),0))*10000) * Akvo_tree_registration_areas_updated.calc_area,0) as nr_trees_audited,

count_pcq_samples.number_pcq_samples,

Akvo_tree_registration_areas_updated.planting_date AS planting_date,
MAX(table_label_strata.submission) AS latest_audit_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_audit,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_audit,
table_label_strata.label_strata,

CASE
WHEN table_label_strata.method_selection = 'PCQ'
THEN ROUND(((1/(NULLIF(POWER(pcq_audit_avg_dist.pcq_results_merged_audit,2),0))*10000) * Akvo_tree_registration_areas_updated.calc_area)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,0)
ELSE 0
END AS perc_trees_survived,

CASE
WHEN table_label_strata.method_selection = 'PCQ'
THEN ROUND(pcq_audit_avg_hgt.pcq_results_merged_audit_hgt,2)
ELSE 0
END AS avg_tree_height,

site_impressions_audit.site_impressions

FROM AKVO_Tree_external_audits_areas
LEFT JOIN AKVO_Tree_external_audits_pcq
ON AKVO_Tree_external_audits_areas.identifier_akvo = AKVO_Tree_external_audits_pcq.identifier_akvo
LEFT JOIN Akvo_tree_registration_areas_updated
ON AKVO_Tree_external_audits_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_external_audits_areas.instance = table_label_strata.instance
LEFT JOIN pcq_audit_avg_dist
ON pcq_audit_avg_dist.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
AND pcq_audit_avg_dist.label_strata = table_label_strata.label_strata
LEFT JOIN pcq_audit_avg_hgt
ON pcq_audit_avg_hgt.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
AND pcq_audit_avg_hgt.label_strata = table_label_strata.label_strata
LEFT JOIN count_pcq_samples
ON count_pcq_samples.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
AND count_pcq_samples.label_strata = table_label_strata.label_strata
LEFT JOIN submittors_audit
ON submittors_audit.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
AND submittors_audit.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_audit
ON site_impressions_audit.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
AND site_impressions_audit.label_strata = table_label_strata.label_strata

WHERE table_label_strata.method_selection = 'PCQ' AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.test,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
count_pcq_samples.number_pcq_samples,
Akvo_tree_registration_areas_updated.planting_date,
table_label_strata.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_audit.submitter,
site_impressions_audit.site_impressions,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
pcq_audit_avg_dist.pcq_results_merged_audit,
pcq_audit_avg_hgt.pcq_results_merged_audit_hgt,
AKVO_Tree_external_audits_areas.display_name),

-- Sub CTE table to calculate COUNTS of MONITORING results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_count_monitoring AS (SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.display_name,
AKVO_Tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_monitoring.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Monitoring' AS procedure,
'Tree count' AS data_collection_method,

--0 AS avg_monitored_tree_distance,
--0 AS avg_audit_tree_density,

ROUND(100/NULLIF(SQRT(count_monitoring_avg_count.nr_trees_monitored/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2) AS avg_monitored_tree_distance,
ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.calc_area,0),0) AS avg_monitored_tree_density,
ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored),0) AS nr_trees_monitored,

0 AS nr_samples_pcq_monitoring,
Akvo_tree_registration_areas_updated.planting_date AS planting_date,
MAX(table_label_strata.submission) AS latest_monitoring_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(Akvo_tree_registration_areas_updated.tree_number,0)*100,2) AS perc_trees_survived,

ROUND(AVG(akvo_tree_monitoring_areas.avg_tree_height)::numeric,2) AS avg_tree_height,

site_impressions_monitoring.site_impressions

FROM AKVO_Tree_monitoring_areas
LEFT JOIN count_monitoring_avg_count
ON AKVO_Tree_monitoring_areas.instance = count_monitoring_avg_count.instance
LEFT JOIN Akvo_tree_registration_areas_updated
ON AKVO_Tree_monitoring_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance
LEFT JOIN submittors_monitoring
ON submittors_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND submittors_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_monitoring
ON site_impressions_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND site_impressions_monitoring.label_strata = table_label_strata.label_strata

WHERE AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.test,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
Akvo_tree_registration_areas_updated.planting_date,
AKVO_Tree_monitoring_areas.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_monitoring.submitter,
site_impressions_monitoring.site_impressions,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
AKVO_Tree_monitoring_areas.display_name,
count_monitoring_avg_count.nr_trees_monitored),

---------------
-- Sub CTE table to calculate OWN METHOD of MONITORING results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_own_method_monitoring AS (SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.display_name,
AKVO_Tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_monitoring.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Monitoring' AS procedure,
'Own method' AS data_collection_method,

--0 AS avg_monitored_tree_distance,
--0 AS avg_audit_tree_density,

ROUND(100/NULLIF(SQRT(count_monitoring_avg_count.nr_trees_monitored/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2) AS avg_monitored_tree_distance,
ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.calc_area,0),0) AS avg_monitored_tree_density,


ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored),0) AS nr_trees_monitored,

0 AS nr_samples_pcq_monitoring,
Akvo_tree_registration_areas_updated.planting_date AS planting_date,
MAX(table_label_strata.submission) AS latest_monitoring_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(Akvo_tree_registration_areas_updated.tree_number,0)*100,2) AS perc_trees_survived,

ROUND(AVG(akvo_tree_monitoring_areas.avg_tree_height)::numeric,2) AS avg_tree_height,

site_impressions_monitoring.site_impressions

FROM AKVO_Tree_monitoring_areas
LEFT JOIN count_monitoring_avg_count
ON AKVO_Tree_monitoring_areas.instance = count_monitoring_avg_count.instance
LEFT JOIN Akvo_tree_registration_areas_updated
ON AKVO_Tree_monitoring_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance
LEFT JOIN submittors_monitoring
ON submittors_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND submittors_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_monitoring
ON site_impressions_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
AND site_impressions_monitoring.label_strata = table_label_strata.label_strata

WHERE AKVO_Tree_monitoring_areas.method_selection = 'We used our own inventory method'
AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.test,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
Akvo_tree_registration_areas_updated.planting_date,
AKVO_Tree_monitoring_areas.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_monitoring.submitter,
site_impressions_monitoring.site_impressions,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
AKVO_Tree_monitoring_areas.display_name,
count_monitoring_avg_count.nr_trees_monitored),

-------------------------

-- Sub CTE table to calculate COUNTS of AUDIT results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_audit AS (SELECT
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.display_name,
AKVO_Tree_external_audits_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_audit.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Audit' AS procedure,
'Tree count' AS data_collection_method,

--0 AS avg_audit_tree_distance,
--0 AS avg_audit_tree_density,

ROUND(100/NULLIF(SQRT(count_audit_avg_count.nr_trees_monitored/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2) AS avg_audit_tree_distance,
ROUND(AVG(count_audit_avg_count.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.calc_area,0),0) AS avg_audit_tree_density,


ROUND(AVG(count_audit_avg_count.nr_trees_monitored),0) AS nr_trees_monitored,

0 AS nr_samples_pcq_audit,
Akvo_tree_registration_areas_updated.planting_date AS planting_date,
MAX(table_label_strata.submission) AS latest_audit_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_audit,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_audit,
table_label_strata.label_strata,

ROUND(AVG(count_audit_avg_count.nr_trees_monitored)/NULLIF(Akvo_tree_registration_areas_updated.tree_number,0)*100,2) AS perc_trees_survived,

ROUND(AVG(akvo_tree_external_audits_areas.audit_reported_tree_height),2) AS avg_tree_height_m,

site_impressions_audit.site_impressions

FROM akvo_tree_external_audits_areas
LEFT JOIN count_audit_avg_count
ON akvo_tree_external_audits_areas.instance = count_audit_avg_count.instance
LEFT JOIN Akvo_tree_registration_areas_updated
ON akvo_tree_external_audits_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON akvo_tree_external_audits_areas.instance = table_label_strata.instance
LEFT JOIN submittors_audit
ON submittors_audit.identifier_akvo = akvo_tree_external_audits_areas.identifier_akvo
AND submittors_audit.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_audit
ON site_impressions_audit.identifier_akvo = akvo_tree_external_audits_areas.identifier_akvo
AND site_impressions_audit.label_strata = table_label_strata.label_strata

WHERE table_label_strata.method_selection = 'Tree count' AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
akvo_tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.test,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
Akvo_tree_registration_areas_updated.planting_date,
table_label_strata.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_audit.submitter,
site_impressions_audit.site_impressions,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
AKVO_Tree_external_audits_areas.display_name,
count_audit_avg_count.nr_trees_monitored),

-- Add the POLYGON results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial registered tree number). Only for polygons
registration_results_polygon AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
akvo_tree_registration_areas_updated.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Registration' AS procedure,
'tree registration' AS data_collection_method,
ROUND(100/NULLIF(SQRT(akvo_tree_registration_areas_updated.tree_number/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2)
AS avg_registered_tree_distance_m,
ROUND((akvo_tree_registration_areas_updated.tree_number/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0)
AS avg_registered_tree_density,
akvo_tree_registration_areas_updated.tree_number AS nr_trees_monitored,
0 as nr_samples_pcq_registration,
akvo_tree_registration_areas_updated.planting_date,
akvo_tree_registration_areas_updated.submission AS latest_registration_submission,
0 AS nr_days_planting_date_registration,
0 AS nr_years_planting_date_registration,
0 AS label_strata,
100 AS "Percentage of trees survived",
0 AS "Average tree height (m)",
'No site impression yet because it is a first registration.' AS site_impressions

FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL),


-- Add the NON-polygon results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial tree number). Only for NON-polygons
registration_results_non_polygon AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
akvo_tree_registration_areas_updated.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Registration' AS procedure,
'tree registration' AS data_collection_method,
ROUND(100/NULLIF(SQRT(akvo_tree_registration_areas_updated.tree_number/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2)
AS avg_registered_tree_distance,
ROUND((akvo_tree_registration_areas_updated.tree_number/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0)
AS avg_registered_tree_density,
akvo_tree_registration_areas_updated.tree_number AS nr_trees_monitored,
0 as nr_samples_pcq_registration,
akvo_tree_registration_areas_updated.planting_date,
akvo_tree_registration_areas_updated.submission AS latest_registration_submission,
0 AS nr_days_planting_date_registration,
0 AS nr_years_planting_date_registration,
0 AS label_strata,
100 AS "Percentage of trees survived",
0 AS "Average tree height (m)",
'No site impression yet because it is a first registration.' AS site_impressions

FROM akvo_tree_registration_areas_updated
WHERE polygon ISNULL),

--UNION of the six tables (PCQ and COUNTS) FROM MONITORING and AUDITS and registration data (polygon and non-polygon)
monitoring_tree_numbers AS
(SELECT * FROM calc_interm_results_tree_numbers_pcq_monitoring
UNION ALL
SELECT * FROM calc_interm_results_tree_numbers_pcq_audit
UNION ALL
SELECT * FROM calc_interm_results_tree_numbers_count_monitoring
UNION ALL
SELECT * FROM calc_interm_results_tree_numbers_own_method_monitoring
UNION ALL
SELECT * FROM calc_interm_results_tree_numbers_audit
UNION ALL
SELECT * FROM registration_results_polygon
UNION ALL
SELECT * FROM registration_results_non_polygon)

SELECT * FROM monitoring_tree_numbers;'''

conn.commit()


# With this query we make sure that all photos (with a photo-id) get an 'https://akvoflow-201.s3.amazonaws.com/images' url for every photo (some in the API do not seem to have one)
create_a3 = '''
UPDATE akvo_tree_registration_areas_updated
SET photo_owner = CASE
WHEN photo_owner LIKE '%/%' AND '.' LIKE SUBSTRING(REVERSE(photo_owner), 4, 1)
THEN RIGHT(photo_owner, strpos(reverse(photo_owner),'/')-1)
END;

UPDATE akvo_tree_registration_areas_updated
SET photo_owner = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_owner)
WHERE photo_owner NOTNULL;

UPDATE akvo_tree_registration_photos
SET photo_url = CASE
WHEN photo_url LIKE '%/%' AND '.' LIKE SUBSTRING(REVERSE(photo_url), 4, 1)
THEN RIGHT(photo_url, strpos(reverse(photo_url),'/')-1)
END;

UPDATE akvo_tree_registration_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_url)
WHERE photo_url NOTNULL;

UPDATE AKVO_Tree_external_audits_photos
SET url_photo = CASE
WHEN url_photo LIKE '%/%' AND '.' LIKE SUBSTRING(REVERSE(url_photo), 4, 1)
THEN RIGHT(url_photo, strpos(reverse(url_photo),'/')-1)
END;

UPDATE AKVO_Tree_external_audits_photos
SET url_photo = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',url_photo)
WHERE url_photo NOTNULL;

UPDATE akvo_tree_monitoring_photos
SET photo_url = CASE
WHEN photo_url LIKE '%/%' AND '.' LIKE SUBSTRING(REVERSE(photo_url), 4, 1)
THEN RIGHT(photo_url, strpos(reverse(photo_url),'/')-1)
END;

UPDATE akvo_tree_monitoring_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_url)
WHERE photo_url NOTNULL;

UPDATE akvo_nursery_registration_photos
SET photo_url = CASE
WHEN photo_url LIKE '%/%' AND '.' LIKE SUBSTRING(REVERSE(photo_url), 4, 1)
THEN RIGHT(photo_url, strpos(reverse(photo_url),'/')-1)
END;

UPDATE akvo_nursery_registration_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_url)
WHERE photo_url NOTNULL;'''

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

LABEL_type_geometry AS (SELECT AKVO_Tree_registration_areas_updated.identifier_akvo, CASE
WHEN AKVO_Tree_registration_areas_updated.polygon NOTNULL
THEN 'polygon'
ELSE 'centroid point'
END AS Geometric_feature
FROM AKVO_Tree_registration_areas_updated)

SELECT
AKVO_Tree_registration_areas_updated.identifier_akvo,
AKVO_Tree_registration_areas_updated.instance,
AKVO_Tree_registration_areas_updated.submitter AS "Submitter of registration",
AKVO_Tree_registration_areas_updated.akvo_form_version AS "Form version used for registration",
AKVO_Tree_registration_areas_updated.submission AS "Submission Date",
LOWER(AKVO_Tree_registration_areas_updated.organisation) as organisation,
AKVO_Tree_registration_areas_updated.country AS "Country",
AKVO_Tree_registration_areas_updated.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas_updated.contract_number AS "Contract number",
AKVO_Tree_registration_areas_updated.tree_number AS "Registered tree number",
AKVO_Tree_registration_areas_updated.estimated_area AS "Estimated area (ha)",
calc_area AS "GIS calculated area (ha)",

COUNT_Total_number_of_photos_taken.count AS Total_number_of_photos_taken,

COUNT_Number_of_photos_with_geotag.count AS Number_of_photos_with_geotag,

COUNT_Number_of_tree_species_registered.count AS Number_of_tree_species_registered,

COUNT_Number_of_tree_species_with_0_number.count AS Number_of_tree_species_with_no_tree_number_indication,

LABEL_type_geometry.Geometric_feature,

ROUND(COUNT_Total_number_of_photos_taken.count/NULLIF(AKVO_Tree_registration_areas_updated.estimated_area,0),2) AS "Photo density (photos/ha)",

number_coord_polygon AS "Number of points in registered polygon",

ROUND(AKVO_Tree_registration_areas_updated.tree_number/NULLIF(AKVO_Tree_registration_areas_updated.calc_area,0),0) AS "Registered tree density (trees/ha)",

AKVO_Tree_registration_areas_updated.centroid_coord

FROM AKVO_Tree_registration_areas_updated
LEFT JOIN LABEL_type_geometry
ON AKVO_Tree_registration_areas_updated.identifier_akvo = LABEL_type_geometry.identifier_akvo
LEFT JOIN COUNT_Total_number_of_photos_taken
ON AKVO_Tree_registration_areas_updated.identifier_akvo = COUNT_Total_number_of_photos_taken.identifier_akvo
LEFT JOIN COUNT_Number_of_photos_with_geotag
ON COUNT_Number_of_photos_with_geotag.identifier_akvo = AKVO_Tree_registration_areas_updated.identifier_akvo
LEFT JOIN COUNT_Number_of_tree_species_registered
ON COUNT_Number_of_tree_species_registered.identifier_akvo = AKVO_Tree_registration_areas_updated.identifier_akvo
LEFT JOIN COUNT_Number_of_tree_species_with_0_number
ON COUNT_Number_of_tree_species_with_0_number.identifier_akvo = AKVO_Tree_registration_areas_updated.identifier_akvo

WHERE (AKVO_Tree_registration_areas_updated.test = '' OR AKVO_Tree_registration_areas_updated.test = 'This is real, valid data')

GROUP BY AKVO_Tree_registration_areas_updated.identifier_akvo, akvo_tree_registration_areas_updated.instance,
akvo_tree_registration_areas_updated.submitter, akvo_tree_registration_areas_updated.submission, akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.country,
akvo_tree_registration_areas_updated.id_planting_site, akvo_tree_registration_areas_updated.contract_number, akvo_tree_registration_areas_updated.estimated_area, akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.number_coord_polygon, akvo_tree_registration_areas_updated.tree_number,
Geometric_feature, COUNT_Total_number_of_photos_taken.count,COUNT_Number_of_photos_with_geotag.count,
COUNT_Number_of_tree_species_registered.count, COUNT_Number_of_tree_species_with_0_number.count,
AKVO_Tree_registration_areas_updated.akvo_form_version,
AKVO_Tree_registration_areas_updated.centroid_coord
ORDER BY AKVO_Tree_registration_areas_updated.submission desc;'''

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

create_a6 = '''
-- Below we determine the trend (linear regression) for survival percentage and tree height by calculating the slope- and intercept values
CREATE TABLE CALC_TAB_linear_regression_results AS WITH linear_regression_field_data AS (
SELECT
h.identifier_akvo,
h.id_planting_site,
table_g.calc_area,
table_g.maximum_label_strata,
table_g.perc_trees_survived,
table_g.avg_tree_height,
h.slope_survival_perc,
y_bar_max_survival_perc - x_bar_max * slope_survival_perc AS intercept_survival_perc,
h.slope_avg_tree_height,
y_bar_max_avg_tree_height - x_bar_max * slope_avg_tree_height AS intercept_avg_tree_height
FROM (
SELECT
s.identifier_akvo,
s.id_planting_site,
SUM((label_strata - x_bar_label_strata) * (perc_trees_survived - y_bar_survival_perc)) / NULLIF(sum((label_strata - x_bar_label_strata) * (label_strata - x_bar_label_strata)),0) AS slope_survival_perc,
SUM((label_strata - x_bar_label_strata) * (avg_tree_height - y_bar_avg_tree_height)) / NULLIF(sum((label_strata - x_bar_label_strata) * (label_strata - x_bar_label_strata)),0) AS slope_avg_tree_height,
MAX(x_bar_label_strata) AS x_bar_max,
MAX(y_bar_survival_perc) AS y_bar_max_survival_perc,
MAX(y_bar_avg_tree_height) AS y_bar_max_avg_tree_height
FROM (
SELECT
identifier_akvo,
id_planting_site,
label_strata,
perc_trees_survived,
avg_tree_height,
AVG(label_strata) OVER (PARTITION BY identifier_akvo) AS x_bar_label_strata,
AVG(perc_trees_survived) OVER (PARTITION BY identifier_akvo) AS y_bar_survival_perc,
AVG(avg_tree_height) OVER (PARTITION BY identifier_akvo) AS y_bar_avg_tree_height
FROM calc_tab_monitoring_calculations_per_site) s
GROUP BY s.identifier_akvo, s.id_planting_site) h

JOIN (SELECT
table_e.identifier_akvo,
table_e.label_strata AS maximum_label_strata,
table_e.perc_trees_survived,
table_e.avg_tree_height,
table_e.calc_area
FROM calc_tab_monitoring_calculations_per_site table_e
JOIN (SELECT identifier_akvo, MAX(label_strata) AS max_label_strata FROM calc_tab_monitoring_calculations_per_site
	 GROUP BY identifier_akvo) table_f
ON table_f.identifier_akvo = table_e.identifier_akvo
	 AND table_f.max_label_strata = table_e.label_strata) table_g
ON table_g.identifier_akvo = h.identifier_akvo)


-- Below we classify the site development by first calculating the prognosis on survival percentage and tree height in year 3, using the linear regression (y=mx+c)
-- The prognosis (survival perc in t=3 and tree height in t=3) are then used to classify the tree development on the sites
SELECT
linear_regression_field_data.identifier_akvo,
calc_tab_monitoring_calculations_per_site.country,
calc_tab_monitoring_calculations_per_site.organisation,
calc_tab_monitoring_calculations_per_site.contract_number,
calc_tab_monitoring_calculations_per_site.id_planting_site,
calc_tab_monitoring_calculations_per_site.data_collection_method,
linear_regression_field_data.calc_area,
COUNT(calc_tab_monitoring_calculations_per_site.label_strata) AS "number of label_strata (monitoring periods)",
linear_regression_field_data.maximum_label_strata AS "latest monitoring (max label_strata)",
linear_regression_field_data.perc_trees_survived AS "latest monitored tree survival perc (at max label_strata)",
linear_regression_field_data.avg_tree_height AS "latest monitored tree height (at max label_strata)",

ROUND(linear_regression_field_data.slope_survival_perc,6) AS site_linear_regression_slope_survival_perc,
ROUND(linear_regression_field_data.intercept_survival_perc,2) AS site_intercept_value_linear_regression_survival_perc,
ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) AS site_estimated_survival_perc_linear_regression_t3,
ROUND(linear_regression_field_data.slope_avg_tree_height::DECIMAL,6) AS site_linear_regression_slope_avg_tree_height,
ROUND(linear_regression_field_data.intercept_avg_tree_height::DECIMAL,2) AS site_intercept_value_linear_regression_avg_tree_height,
ROUND(((1080 * linear_regression_field_data.slope_avg_tree_height) + linear_regression_field_data.intercept_avg_tree_height)::DECIMAL,2) AS site_estimated_avg_tree_height_linear_regression_t3,

CASE
WHEN ROUND((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc,2) > 100
AND linear_regression_field_data.maximum_label_strata >= 1080
AND linear_regression_field_data.avg_tree_height NOTNULL
AND linear_regression_field_data.slope_survival_perc > 0
AND linear_regression_field_data.slope_avg_tree_height > 0
THEN 'tree development very likely positive (survival >100%, measured tree height > 2m and showing positive trend at t=3)'

WHEN ROUND((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc,2) > 100
AND linear_regression_field_data.maximum_label_strata <= 1080
AND linear_regression_field_data.slope_survival_perc > 0
AND ROUND(((1080 * linear_regression_field_data.slope_avg_tree_height) + linear_regression_field_data.intercept_avg_tree_height)::DECIMAL,2) > 2
AND linear_regression_field_data.slope_avg_tree_height > 0
THEN 'tree development very likely positive (survival >100%, estimated tree height > 2m and showing positive trend at t=3)'

WHEN ROUND((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc,2) BETWEEN 80 AND 100
AND ROUND(((1080 * linear_regression_field_data.slope_avg_tree_height) + linear_regression_field_data.intercept_avg_tree_height)::DECIMAL,2) > 1
AND linear_regression_field_data.slope_avg_tree_height > 0
THEN 'tree development likely positive (survival between 100% and 80%, tree height > 1m or showing positive trend at t=3)'

WHEN ROUND((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc,2) BETWEEN 60 AND 80
THEN 'tree development unsure (survival between 80% and 60%, no height criteria at t=3)'

WHEN ROUND((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc,2) BETWEEN 25 AND 60
AND linear_regression_field_data.slope_avg_tree_height <= 0
THEN 'tree development likely negative (survival between 60% and 25% and negative height trend at t=3)'

WHEN ROUND((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc,2) BETWEEN 0 AND 25
AND linear_regression_field_data.slope_avg_tree_height <= 0
THEN 'tree development very likely negative (survival between 25% and 0% and negative height trend at t=3)'

ELSE 'tree development unclear'

END AS prognosis_tree_development_site_level_linear_regression

FROM linear_regression_field_data
JOIN calc_tab_monitoring_calculations_per_site
ON linear_regression_field_data.identifier_akvo = calc_tab_monitoring_calculations_per_site.identifier_akvo

WHERE calc_tab_monitoring_calculations_per_site.label_strata > 0

GROUP BY
linear_regression_field_data.identifier_akvo,
calc_tab_monitoring_calculations_per_site.country,
calc_tab_monitoring_calculations_per_site.organisation,
calc_tab_monitoring_calculations_per_site.contract_number,
calc_tab_monitoring_calculations_per_site.id_planting_site,
calc_tab_monitoring_calculations_per_site.data_collection_method,
linear_regression_field_data.maximum_label_strata,
linear_regression_field_data.slope_survival_perc,
linear_regression_field_data.intercept_survival_perc,
site_estimated_survival_perc_linear_regression_t3,
site_linear_regression_slope_avg_tree_height,
site_intercept_value_linear_regression_avg_tree_height,
site_estimated_avg_tree_height_linear_regression_t3,
linear_regression_field_data.slope_avg_tree_height,
linear_regression_field_data.perc_trees_survived,
linear_regression_field_data.avg_tree_height,
linear_regression_field_data.calc_area;

ALTER TABLE CALC_TAB_linear_regression_results
ADD satellite_validation_discrete NUMERIC;

UPDATE CALC_TAB_linear_regression_results
SET satellite_validation_discrete =
CASE
WHEN
prognosis_tree_development_site_level_linear_regression = 'tree development very likely positive (survival >100%, measured tree height > 2m and showing positive trend at t=3)'
OR
prognosis_tree_development_site_level_linear_regression = 'tree development very likely positive (survival >100%, estimated tree height > 2m and showing positive trend at t=3)'
THEN 1
WHEN
prognosis_tree_development_site_level_linear_regression = 'tree development likely positive (survival between 100% and 80%, tree height > 1m or showing positive trend at t=3)'
OR
prognosis_tree_development_site_level_linear_regression = 'tree development unsure (survival between 80% and 60%, no height criteria at t=3)'
OR
prognosis_tree_development_site_level_linear_regression = 'tree development unclear'
THEN 0
WHEN
prognosis_tree_development_site_level_linear_regression = 'tree development likely negative (survival between 60% and 25% and negative height trend at t=3)'
OR
prognosis_tree_development_site_level_linear_regression = 'tree development very likely negative (survival between 25% and 0% and negative height trend at t=3)'
THEN -1
END;

ALTER TABLE CALC_TAB_linear_regression_results
ADD satellite_validation_continuous NUMERIC;

UPDATE CALC_TAB_linear_regression_results
SET satellite_validation_continuous =

(site_linear_regression_slope_survival_perc * "latest monitored tree survival perc (at max label_strata)")
+
(site_linear_regression_slope_avg_tree_height * "latest monitored tree height (at max label_strata)")*1000;
'''

conn.commit()

create_a7 = '''CREATE TABLE CALC_TAB_linear_regression_results_satellite_validation
AS select
*
FROM calc_tab_linear_regression_results
WHERE
calc_area > 4 AND satellite_validation_discrete = 1
OR satellite_validation_discrete = -1;'''

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
AS Select AKVO_Tree_registration_areas_updated.centroid_coord,
AKVO_Tree_registration_areas_updated.contract_number AS "Contract number",
AKVO_Tree_registration_areas_updated.id_planting_site,
SUM(AKVO_Tree_monitoring_counts.number_species) AS "Number of trees counted by species",
COUNT(*) AS "Number of species counted",
AKVO_Tree_registration_areas_updated.tree_number AS "Registered tree number",
AKVO_Tree_monitoring_areas.*
FROM AKVO_Tree_monitoring_areas
JOIN AKVO_Tree_monitoring_counts
ON AKVO_Tree_monitoring_areas.instance = AKVO_Tree_monitoring_counts.instance
JOIN AKVO_Tree_registration_areas_updated
ON AKVO_Tree_registration_areas_updated.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo
and AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'

GROUP BY
AKVO_Tree_monitoring_counts.instance,
AKVO_Tree_registration_areas_updated.contract_number,
AKVO_Tree_registration_areas_updated.id_planting_site,
AKVO_Tree_registration_areas_updated.centroid_coord,
AKVO_Tree_registration_areas_updated.tree_number,
AKVO_Tree_registration_areas_updated.identifier_akvo,
akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_monitoring_areas.device_id,
akvo_tree_monitoring_areas.instance,
akvo_tree_monitoring_areas.submission_year,
akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.akvo_form_version,
akvo_tree_monitoring_areas.avg_tree_height,
akvo_tree_monitoring_areas.number_living_trees,
akvo_tree_monitoring_areas.site_impression,
akvo_tree_monitoring_areas.method_selection,
akvo_tree_monitoring_areas.avg_circom_tree_count,
akvo_tree_monitoring_areas.test,
akvo_tree_monitoring_areas.avg_circom_tree_pcq,
akvo_tree_monitoring_areas.submission,
akvo_tree_monitoring_areas.display_name,
akvo_tree_monitoring_areas.location_monitoring;'''

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

-- This table is just to plot registered tree number, number of site registrations and registration dates on site level
AS WITH CTE_ranking_monitoring_audit_method AS (SELECT
calc_tab_monitoring_calculations_per_site.*,
CASE
WHEN calc_tab_monitoring_calculations_per_site.procedure = 'Audit' and data_collection_method = 'PCQ'
THEN 4
WHEN calc_tab_monitoring_calculations_per_site.procedure = 'Audit' and data_collection_method = 'Tree count'
THEN 3
WHEN calc_tab_monitoring_calculations_per_site.procedure = 'Monitoring' and data_collection_method = 'PCQ'
THEN 2
WHEN calc_tab_monitoring_calculations_per_site.procedure = 'Monitoring' and data_collection_method = 'Tree count'
THEN 1
ELSE 0
END AS rank_monitoring_audit_method
FROM calc_tab_monitoring_calculations_per_site),

CTE_total_tree_registrations AS (
SELECT
lower(akvo_tree_registration_areas_updated.country) as name_country,
lower(akvo_tree_registration_areas_updated.organisation) as organisation,
akvo_tree_registration_areas_updated.contract_number,
SUM(akvo_tree_registration_areas_updated.tree_number) AS "Registered tree number",
MAX(akvo_tree_registration_areas_updated.submission) AS "Latest submitted registration",
COUNT(*) AS "Nr of sites registered",
COUNT(DISTINCT akvo_tree_registration_areas_updated.identifier_akvo) AS "Number of site registrations"
FROM akvo_tree_registration_areas_updated
WHERE (akvo_tree_registration_areas_updated.test = '' OR akvo_tree_registration_areas_updated.test = 'This is real, valid data')
AND NOT akvo_tree_registration_areas_updated.id_planting_site = 'placeholder' AND NOT country = '' AND NOT organisation = ''
GROUP BY
akvo_tree_registration_areas_updated.contract_number,
name_country,
organisation),

-- This table unifies all submissions from audits and monitorings on instance level.
-- This is a preperatory table to label all instances with a label_strata
CTE_union_monitorings_audits AS (SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.instance,
AKVO_Tree_monitoring_areas.submission
FROM AKVO_Tree_monitoring_areas
UNION ALL
SELECT
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.instance,
AKVO_Tree_external_audits_areas.submission
FROM AKVO_Tree_external_audits_areas),

-- This table counts the total number of sites that were monitored or audited on contract level
CTE_tree_monitoring AS (
SELECT
akvo_tree_registration_areas_updated.contract_number,
COUNT(DISTINCT CTE_union_monitorings_audits.identifier_akvo) as nr_sites_monitored_audited,
COUNT(DISTINCT CTE_union_monitorings_audits.instance) as total_nr_monitorings_audits,
MAX(CTE_union_monitorings_audits.submission) AS "Latest submitted monitoring or audit"
FROM akvo_tree_registration_areas_updated
LEFT JOIN CTE_union_monitorings_audits
ON akvo_tree_registration_areas_updated.identifier_akvo = CTE_union_monitorings_audits.identifier_akvo
GROUP BY akvo_tree_registration_areas_updated.contract_number),

-- This table lists all tree species on contract level that were reported during the registration
CTE_tree_species AS (
SELECT
akvo_tree_registration_areas_updated.contract_number,
COUNT(DISTINCT akvo_tree_registration_species.lat_name_species) as "Number of tree species registered"
FROM akvo_tree_registration_areas_updated
LEFT JOIN akvo_tree_registration_species
ON akvo_tree_registration_areas_updated.identifier_akvo	= akvo_tree_registration_species.identifier_akvo
GROUP BY akvo_tree_registration_areas_updated.contract_number),

-- This table calculates results on contract for T=1
CTE_contract_level_monitoring_audit_results_t1 AS (SELECT
table_a.contract_number,
ROUND(SUM(perc_trees_survived * registered_tree_number)/SUM(registered_tree_number),2) AS weighted_avg_perc_tree_survival_t1,
COUNT(*) AS "number of sites monitored by partner in t=1",
ROUND(COUNT(*)*1.00/table_b.total_nr_planting_sites_per_contract*1.00*100,2) AS "percentage of sites monitored by partner in t=1",
ROUND(SUM(registered_tree_number::decimal)/NULLIF(table_b.total_nr_trees_registered_per_contract::decimal,0)*100,2) AS "percentage of trees monitored/audited in t=1",
ROUND(SUM(table_a.avg_tree_height::decimal*registered_tree_number)/NULLIF(SUM(registered_tree_number),0),1) AS "weighted avg tree_height in t1"
FROM CTE_ranking_monitoring_audit_method AS table_a
JOIN
-- Below the number of trees in the entire contract is calculated. This cannot be retrieved from the CTE_ranking_monitoring_audit_method
-- since this table only shows the registered trees (on site level)	multiple times for various label strata (0, 360, etc).
(SELECT contract_number,
COUNT(identifier_akvo) AS total_nr_planting_sites_per_contract,
SUM(tree_number) AS total_nr_trees_registered_per_contract
FROM akvo_tree_registration_areas_updated
GROUP BY contract_number) table_b
ON table_a.contract_number = table_b.contract_number
JOIN(
SELECT identifier_akvo,
MAX(label_strata) AS max_label_strata_t1,
MAX(rank_monitoring_audit_method) AS max_method_ranking_t1
FROM CTE_ranking_monitoring_audit_method
WHERE
label_strata > 0 AND label_strata <= 540
GROUP BY identifier_akvo) table_c
ON table_a.identifier_akvo = table_c.identifier_akvo
AND table_a.label_strata = table_c.max_label_strata_t1
AND table_a.rank_monitoring_audit_method = table_c.max_method_ranking_t1
GROUP BY table_a.contract_number, table_b.total_nr_trees_registered_per_contract,
table_b.total_nr_planting_sites_per_contract),

-- This table calculates results on contract for T=2
CTE_contract_level_monitoring_audit_results_t2 AS (SELECT
table_a.contract_number,
ROUND(SUM(perc_trees_survived * registered_tree_number)/SUM(registered_tree_number),2) AS weighted_avg_perc_tree_survival_t2,
COUNT(*) AS "number of sites monitored by partner in t=2",
ROUND(COUNT(*)*1.00/table_b.total_nr_planting_sites_per_contract*1.00*100,2) AS "percentage of sites monitored by partner in t=2",
ROUND(SUM(registered_tree_number::decimal)/NULLIF(table_b.total_nr_trees_registered_per_contract::decimal,0)*100,2) AS "percentage of trees monitored/audited in t=2",
ROUND(SUM(table_a.avg_tree_height::decimal*registered_tree_number)/NULLIF(SUM(registered_tree_number),0),1) AS "weighted avg tree_height in t2"
FROM CTE_ranking_monitoring_audit_method AS table_a
JOIN
-- Below the number of trees in the entire contract is calculated. This cannot be retrieved from the CTE_ranking_monitoring_audit_method
-- since this table only shows the registered trees (on site level)	for the sites that have been monitored.
(SELECT contract_number,
COUNT(identifier_akvo) AS total_nr_planting_sites_per_contract,
SUM(tree_number) AS total_nr_trees_registered_per_contract
FROM akvo_tree_registration_areas_updated
GROUP BY contract_number) table_b
ON table_a.contract_number = table_b.contract_number
JOIN(
SELECT identifier_akvo,
MAX(label_strata) AS max_label_strata_t2,
MAX(rank_monitoring_audit_method) AS max_method_ranking_t2
FROM CTE_ranking_monitoring_audit_method
WHERE
label_strata > 540 AND label_strata <= 900
GROUP BY identifier_akvo) table_c
ON table_a.identifier_akvo = table_c.identifier_akvo
AND table_a.label_strata = table_c.max_label_strata_t2
AND table_a.rank_monitoring_audit_method = table_c.max_method_ranking_t2
GROUP BY table_a.contract_number, table_b.total_nr_trees_registered_per_contract,
table_b.total_nr_planting_sites_per_contract),

-- This table calculates results on contract for T=3
CTE_contract_level_monitoring_audit_results_t3 AS (SELECT
table_a.contract_number,
ROUND(SUM(perc_trees_survived * registered_tree_number)/SUM(registered_tree_number),2) AS weighted_avg_perc_tree_survival_t3,
COUNT(*) AS "number of sites monitored by partner in t=3",
ROUND(COUNT(*)*1.00/table_b.total_nr_planting_sites_per_contract*1.00*100,2) AS "percentage of sites monitored by partner in t=3",
ROUND(SUM(registered_tree_number::decimal)/NULLIF(table_b.total_nr_trees_registered_per_contract::decimal,0)*100,2) AS "percentage of trees monitored/audited in t=3",
ROUND(SUM(table_a.avg_tree_height::decimal*registered_tree_number)/NULLIF(SUM(registered_tree_number),0),1) AS "weighted avg tree_height in t3"
FROM CTE_ranking_monitoring_audit_method AS table_a
JOIN
-- Below the number of trees in the entire contract is calculated. This cannot be retrieved from the CTE_ranking_monitoring_audit_method
-- since this table only shows the registered trees (on site level)	for the sites that have been monitored.
(SELECT contract_number,
COUNT(identifier_akvo) As total_nr_planting_sites_per_contract,
SUM(tree_number) AS total_nr_trees_registered_per_contract
FROM akvo_tree_registration_areas_updated
GROUP BY contract_number) table_b
ON table_a.contract_number = table_b.contract_number
JOIN(
SELECT identifier_akvo,
MAX(label_strata) AS max_label_strata_t3,
MAX(rank_monitoring_audit_method) AS max_method_ranking_t3
FROM CTE_ranking_monitoring_audit_method
WHERE
label_strata > 900
GROUP BY identifier_akvo) table_c
ON table_a.identifier_akvo = table_c.identifier_akvo
AND table_a.label_strata = table_c.max_label_strata_t3
AND table_a.rank_monitoring_audit_method = table_c.max_method_ranking_t3
GROUP BY table_a.contract_number, table_b.total_nr_trees_registered_per_contract,
table_b.total_nr_planting_sites_per_contract),

-- This table lists the maximum label_strata values for sites in t=1
CTE_site_level_monitoring_audit_results_t1 AS (SELECT
table_a.identifier_akvo,
table_a.contract_number,
table_a.nr_trees_monitored AS nr_trees_monitored_t1
FROM CTE_ranking_monitoring_audit_method AS table_a
JOIN
(SELECT identifier_akvo,
MAX(label_strata) AS max_label_strata_t1,
MAX(rank_monitoring_audit_method) AS max_method_ranking_t1
FROM CTE_ranking_monitoring_audit_method
WHERE
label_strata > 0 AND label_strata <= 360
GROUP BY identifier_akvo) table_b
ON table_a.identifier_akvo = table_b.identifier_akvo
AND table_a.label_strata = table_b.max_label_strata_t1),

-- This table lists the maximum label_strata values for sites in t=2
CTE_site_level_monitoring_audit_results_t2 AS (SELECT
table_a.identifier_akvo,
table_a.contract_number,
table_a.nr_trees_monitored AS nr_trees_monitored_t2,
table_b.max_label_strata_t2
FROM CTE_ranking_monitoring_audit_method AS table_a
JOIN
(SELECT identifier_akvo,
MAX(label_strata) AS max_label_strata_t2,
MAX(rank_monitoring_audit_method) AS max_method_ranking_t2
FROM CTE_ranking_monitoring_audit_method
WHERE
label_strata > 360 AND label_strata <= 720
GROUP BY identifier_akvo) table_b
ON table_a.identifier_akvo = table_b.identifier_akvo
AND table_a.label_strata = table_b.max_label_strata_t2),

-- This table lists the maximum label_strata values for sites in t=3
CTE_site_level_monitoring_audit_results_t3 AS (SELECT
table_a.identifier_akvo,
table_a.contract_number,
table_a.nr_trees_monitored AS nr_trees_monitored_t3,
table_b.max_label_strata_t3
FROM CTE_ranking_monitoring_audit_method AS table_a
JOIN
(SELECT identifier_akvo,
MAX(label_strata) AS max_label_strata_t3,
MAX(rank_monitoring_audit_method) AS max_method_ranking_t3
FROM CTE_ranking_monitoring_audit_method
WHERE
label_strata > 720
GROUP BY identifier_akvo) table_b
ON table_a.identifier_akvo = table_b.identifier_akvo
AND table_a.label_strata = table_b.max_label_strata_t3),

-- This table lists the monitoring results for sites that were been monitored in t=1. In case the site was monitored
-- the monitoring results are used. In a the site was NOT monitored, the weighted average survival percentage is used
-- to determine survived tree numbers in t=1
CTE_calculate_extrapolated_tree_number_site_level_t1 AS (SELECT
akvo_tree_registration_areas_updated.contract_number,

-- Calculate T=1 results on site level. Check if a monitoring exists. If so, use the survival percentage for the
-- specific site to calculate tree numbers in T=1. IF not, use the weighted average for the contract in T=1
CASE
WHEN CTE_site_level_monitoring_audit_results_t1.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
THEN CTE_site_level_monitoring_audit_results_t1.nr_trees_monitored_t1
WHEN CTE_site_level_monitoring_audit_results_t1.identifier_akvo ISNULL AND CTE_contract_level_monitoring_audit_results_t1.weighted_avg_perc_tree_survival_t1 NOTNULL
THEN ROUND(akvo_tree_registration_areas_updated.tree_number * CTE_contract_level_monitoring_audit_results_t1.weighted_avg_perc_tree_survival_t1/100,0)
-- For some contracts, 0 sites have been monitored yet. So there is no weighted average calculated. We put survival percentage oto NULL in year 1(?)
ELSE NULL -- Put to NULL if no monitoring happened at all for this contract? And do that here or in the CTE_contract_level_monitoring_audit_results_t2 table?
END AS extrapolated_tree_number_per_site_t1
FROM akvo_tree_registration_areas_updated
LEFT JOIN CTE_site_level_monitoring_audit_results_t1
ON CTE_site_level_monitoring_audit_results_t1.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN CTE_contract_level_monitoring_audit_results_t1
ON CTE_contract_level_monitoring_audit_results_t1.contract_number = akvo_tree_registration_areas_updated.contract_number),

-- This table lists the monitoring results for sites that were been monitored in t=2. In case the site was monitored
-- the monitoring results are used. In a the site was NOT monitored, the weighted average survival percentage is used
-- to determine survived tree numbers in t=2
CTE_calculate_extrapolated_tree_number_site_level_t2 AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.contract_number,

-- Calculate T=2 results on site level. Check if a monitoring exists. If so, use the survival percentage for the
-- specific site to calculate tree numbers in T=2. IF not, use the weighted average for the contract in T=2
CASE
WHEN CTE_site_level_monitoring_audit_results_t2.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
THEN CTE_site_level_monitoring_audit_results_t2.nr_trees_monitored_t2
WHEN CTE_site_level_monitoring_audit_results_t2.identifier_akvo ISNULL AND CTE_contract_level_monitoring_audit_results_t2.weighted_avg_perc_tree_survival_t2 NOTNULL
THEN ROUND(akvo_tree_registration_areas_updated.tree_number * CTE_contract_level_monitoring_audit_results_t2.weighted_avg_perc_tree_survival_t2/100,0)
-- For some contracts, 0 sites have been monitored yet. So there is no weighted average calculated. We put survival percentage on NULL in year 2(?)
ELSE NULL -- Put to NULL if no monitoring happened at all for this contract? And do that here or in the CTE_contract_level_monitoring_audit_results_t2 table?
END AS extrapolated_tree_number_per_site_t2

FROM akvo_tree_registration_areas_updated
LEFT JOIN CTE_site_level_monitoring_audit_results_t2
ON CTE_site_level_monitoring_audit_results_t2.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN CTE_contract_level_monitoring_audit_results_t2
ON CTE_contract_level_monitoring_audit_results_t2.contract_number = akvo_tree_registration_areas_updated.contract_number),

-- This table lists the monitoring results for sites that were been monitored in t=3. In case the site was monitored
-- the monitoring results are used. In a the site was NOT monitored, the weighted average survival percentage is used
-- to determine survived tree numbers in t=3
CTE_calculate_extrapolated_tree_number_site_level_t3 AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.tree_number,
CTE_site_level_monitoring_audit_results_t3.max_label_strata_t3,

-- Calculate T=3 results on site level. Check if a monitoring exists. If so, use the survival percentage for the
-- specific site to calculate tree numbers in T=3. IF not, use the weighted average for the contract in T=3
CASE
WHEN CTE_site_level_monitoring_audit_results_t3.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
THEN CTE_site_level_monitoring_audit_results_t3.nr_trees_monitored_t3
WHEN CTE_site_level_monitoring_audit_results_t3.identifier_akvo ISNULL AND CTE_contract_level_monitoring_audit_results_t3.weighted_avg_perc_tree_survival_t3 NOTNULL
THEN ROUND(akvo_tree_registration_areas_updated.tree_number * CTE_contract_level_monitoring_audit_results_t3.weighted_avg_perc_tree_survival_t3/100,0)
-- For some contracts, 0 sites have been monitored yet. So there is no weighted average calculated. We put survival percentage to NULL in year 3(?)
ELSE NULL -- Put to NULL if no monitoring happened at all for this contract? And do that here or in the CTE_contract_level_monitoring_audit_results_t3 table?
END AS extrapolated_tree_number_per_site_t3

FROM akvo_tree_registration_areas_updated
LEFT JOIN CTE_site_level_monitoring_audit_results_t3
ON CTE_site_level_monitoring_audit_results_t3.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN CTE_contract_level_monitoring_audit_results_t3
ON CTE_contract_level_monitoring_audit_results_t3.contract_number = akvo_tree_registration_areas_updated.contract_number),

-- In this table we calculate TOTAL tree numbers for the entire contract for T=1
CTE_calculate_extrapolated_tree_number_contract_level_t1 AS (SELECT
CTE_calculate_extrapolated_tree_number_site_level_t1.contract_number,
SUM(extrapolated_tree_number_per_site_t1) AS "total tree number in t=1"
FROM CTE_calculate_extrapolated_tree_number_site_level_t1
GROUP BY CTE_calculate_extrapolated_tree_number_site_level_t1.contract_number),

-- In this table we calculate TOTAL tree numbers for the entire contract for T=2
CTE_calculate_extrapolated_tree_number_contract_level_t2 AS (SELECT
CTE_calculate_extrapolated_tree_number_site_level_t2.contract_number,
SUM(extrapolated_tree_number_per_site_t2) AS "total tree number in t=2"
FROM CTE_calculate_extrapolated_tree_number_site_level_t2
GROUP BY CTE_calculate_extrapolated_tree_number_site_level_t2.contract_number),

-- In this table we calculate TOTAL tree numbers for the entire contract for T=3
CTE_calculate_extrapolated_tree_number_contract_level_t3 AS (SELECT
CTE_calculate_extrapolated_tree_number_site_level_t3.contract_number,
SUM(extrapolated_tree_number_per_site_t3) AS "total tree number in t=3"
FROM CTE_calculate_extrapolated_tree_number_site_level_t3
GROUP BY CTE_calculate_extrapolated_tree_number_site_level_t3.contract_number)

-- Create the FINAL result table:
SELECT
CTE_total_tree_registrations.name_country,
CTE_total_tree_registrations.organisation,
CTE_total_tree_registrations.contract_number AS "Contract number",
CTE_total_tree_registrations."Nr of sites registered" AS "Total number of planting sites registered at t=0",
CTE_total_tree_registrations."Registered tree number" AS "Total number of trees registered at t=0",
CTE_total_tree_registrations."Latest submitted registration" AS "Latest submitted registration at t=0",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t1."number of sites monitored by partner in t=1" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t1."number of sites monitored by partner in t=1"
ELSE 0
END AS "number of sites monitored/audited in t=1",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t1."percentage of sites monitored by partner in t=1" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t1."percentage of sites monitored by partner in t=1"
ELSE 0
END AS "percentage of sites monitored/audited in t=1",


CASE
WHEN CTE_contract_level_monitoring_audit_results_t1."percentage of trees monitored/audited in t=1" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t1."percentage of trees monitored/audited in t=1"
ELSE 0
END AS "percentage of trees monitored/audited in t=1",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t1.weighted_avg_perc_tree_survival_t1 NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t1.weighted_avg_perc_tree_survival_t1
ELSE NULL
END AS "weighted avg perc tree_survival in t=1",

CASE
WHEN CTE_calculate_extrapolated_tree_number_contract_level_t1."total tree number in t=1" NOTNULL
THEN CTE_calculate_extrapolated_tree_number_contract_level_t1."total tree number in t=1"
ELSE NULL
END AS "total tree number in t=1",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t1."weighted avg tree_height in t1" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t1."weighted avg tree_height in t1"
ELSE NULL
END AS "weighted avg tree_height in t1",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t2."number of sites monitored by partner in t=2" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t2."number of sites monitored by partner in t=2"
ELSE 0
END AS "number of sites monitored/audited in t=2",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t2."percentage of sites monitored by partner in t=2" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t2."percentage of sites monitored by partner in t=2"
ELSE 0
END AS "percentage of sites monitored/audited in t=2",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t2."percentage of trees monitored/audited in t=2" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t2."percentage of trees monitored/audited in t=2"
ELSE 0
END AS "percentage of trees monitored/audited in t=2",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t2.weighted_avg_perc_tree_survival_t2 NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t2.weighted_avg_perc_tree_survival_t2
ELSE NULL
END AS "weighted avg perc tree_survival in t=2",

CASE
WHEN CTE_calculate_extrapolated_tree_number_contract_level_t2."total tree number in t=2" NOTNULL
THEN CTE_calculate_extrapolated_tree_number_contract_level_t2."total tree number in t=2"
ELSE NULL
END AS "total tree number in t=2",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t2."weighted avg tree_height in t2" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t2."weighted avg tree_height in t2"
ELSE NULL
END AS "weighted avg tree_height in t=2",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t3."number of sites monitored by partner in t=3" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t3."number of sites monitored by partner in t=3"
ELSE 0
END AS "number of sites monitored/audited in t=>3",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t3."percentage of sites monitored by partner in t=3" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t3."percentage of sites monitored by partner in t=3"
ELSE 0
END AS "percentage of sites monitored/audited in t=>3",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t3."percentage of trees monitored/audited in t=3" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t3."percentage of trees monitored/audited in t=3"
ELSE 0
END AS "percentage of trees monitored/audited in t=>3",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t3.weighted_avg_perc_tree_survival_t3 NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t3.weighted_avg_perc_tree_survival_t3
ELSE NULL
END AS "weighted avg perc tree_survival in t=>3",

CASE
WHEN CTE_calculate_extrapolated_tree_number_contract_level_t3."total tree number in t=3" NOTNULL
THEN CTE_calculate_extrapolated_tree_number_contract_level_t3."total tree number in t=3"
ELSE NULL
END AS "total tree number in t=>3",

CASE
WHEN CTE_contract_level_monitoring_audit_results_t3."weighted avg tree_height in t3" NOTNULL
THEN CTE_contract_level_monitoring_audit_results_t3."weighted avg tree_height in t3"
ELSE NULL
END AS "weighted avg tree_height in t=>3",

CTE_tree_monitoring.nr_sites_monitored_audited AS "Total number of sites monitored/audited at least 1 time",
CTE_tree_species."Number of tree species registered"

FROM CTE_total_tree_registrations
LEFT JOIN CTE_tree_monitoring
ON CTE_tree_monitoring.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_tree_species
ON CTE_tree_species.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_contract_level_monitoring_audit_results_t1
ON CTE_contract_level_monitoring_audit_results_t1.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_contract_level_monitoring_audit_results_t2
ON CTE_contract_level_monitoring_audit_results_t2.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_contract_level_monitoring_audit_results_t3
ON CTE_contract_level_monitoring_audit_results_t3.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_calculate_extrapolated_tree_number_contract_level_t1
ON CTE_calculate_extrapolated_tree_number_contract_level_t1.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_calculate_extrapolated_tree_number_contract_level_t2
ON CTE_calculate_extrapolated_tree_number_contract_level_t2.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_calculate_extrapolated_tree_number_contract_level_t3
ON CTE_calculate_extrapolated_tree_number_contract_level_t3.contract_number = CTE_total_tree_registrations.contract_number;'''

conn.commit()


create_a16 = '''CREATE TABLE CALC_TAB_Error_check_on_nursery_registration AS select
akvo_nursery_registration.identifier_akvo,
akvo_nursery_registration.nursery_name,
LOWER(akvo_nursery_registration.organisation) as organisation,
akvo_nursery_registration.full_tree_capacity as "maximum/full tree capacity of nursery",
akvo_nursery_monitoring.number_trees_produced_currently,
akvo_nursery_monitoring.month_planting_stock,
akvo_nursery_registration.submission as registration_date,
akvo_nursery_monitoring.submission_date as monitoring_date,
species.species_list as "species currently produced in nursery",
nr_photos_monitoring.counted_photos_monitoring as "nr of photos taken during monitoring",
nr_photos_registration.counted_photos_registration as "nr of photos taken during registration",
akvo_nursery_registration.centroid_coord

FROM akvo_nursery_registration
LEFT JOIN akvo_nursery_monitoring
ON akvo_nursery_registration.identifier_akvo = akvo_nursery_monitoring.identifier_akvo
LEFT JOIN
(SELECT STRING_AGG(akvo_nursery_monitoring_tree_species.tree_species_latin,' | ') species_list,
	akvo_nursery_monitoring_tree_species.instance
FROM akvo_nursery_monitoring_tree_species
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring_tree_species.instance = akvo_nursery_monitoring.instance
GROUP BY akvo_nursery_monitoring_tree_species.instance) species
ON akvo_nursery_monitoring.instance = species.instance
LEFT JOIN
(SELECT COUNT(akvo_nursery_monitoring_photos.instance) counted_photos_monitoring, akvo_nursery_monitoring_photos.instance
	 FROM akvo_nursery_monitoring_photos
	 group by akvo_nursery_monitoring_photos.instance) nr_photos_monitoring
ON akvo_nursery_monitoring.instance = nr_photos_monitoring.instance

LEFT JOIN (select COUNT(akvo_nursery_registration_photos.instance) counted_photos_registration, akvo_nursery_registration_photos.instance
	 FROM akvo_nursery_registration_photos
	 group by akvo_nursery_registration_photos.instance) nr_photos_registration
ON akvo_nursery_registration.instance = nr_photos_registration.instance

group by akvo_nursery_registration.organisation,
akvo_nursery_registration.nursery_name,
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

FROM CALC_TAB_Error_check_on_nursery_registration;'''

conn.commit()

create_a18 = '''CREATE TABLE akvo_ecosia_tree_area_monitoring AS
SELECT
akvo_tree_monitoring_areas.identifier_akvo,
LOWER(akvo_tree_registration_areas_updated.country) as country,
LOWER(akvo_tree_registration_areas_updated.organisation) as organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_monitoring_areas.display_name,
akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.akvo_form_version,
akvo_tree_monitoring_areas.method_selection,
monitoring_results.COUNT-1 AS "Number of times site has been monitored",
akvo_tree_registration_areas_updated.tree_number AS "Registered tree number",
monitoring_results."Latest monitored nr trees on site",
monitoring_results."Latest monitored percentage of survived trees",
monitoring_results."Latest monitoring submission date",
akvo_tree_registration_areas_updated.polygon,
akvo_tree_registration_areas_updated.multipoint,
akvo_tree_registration_areas_updated.centroid_coord
FROM akvo_tree_monitoring_areas
LEFT JOIN akvo_tree_registration_areas_updated
ON akvo_tree_monitoring_areas.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo

LEFT JOIN (SELECT
CALC_TAB_monitoring_calculations_per_site.identifier_akvo,
COUNT(CALC_TAB_monitoring_calculations_per_site.label_strata),
MAX(CALC_TAB_monitoring_calculations_per_site.nr_trees_monitored) AS "Latest monitored nr trees on site",
MAX(CALC_TAB_monitoring_calculations_per_site.perc_trees_survived) AS "Latest monitored percentage of survived trees",
MAX(CALC_TAB_monitoring_calculations_per_site.latest_monitoring_submission) AS "Latest monitoring submission date"
FROM CALC_TAB_monitoring_calculations_per_site
GROUP BY CALC_TAB_monitoring_calculations_per_site.identifier_akvo) monitoring_results

ON akvo_tree_monitoring_areas.identifier_akvo = monitoring_results.identifier_akvo

GROUP BY akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_registration_areas_updated.country,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_monitoring_areas.display_name,
akvo_tree_monitoring_areas.submission_year,
akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.akvo_form_version,
akvo_tree_monitoring_areas.test,
monitoring_results."Latest monitored nr trees on site",
monitoring_results."Latest monitored percentage of survived trees",
monitoring_results."Latest monitoring submission date",
akvo_tree_registration_areas_updated.polygon,
akvo_tree_registration_areas_updated.multipoint,
akvo_tree_registration_areas_updated.centroid_coord,
monitoring_results.COUNT,
akvo_tree_registration_areas_updated.tree_number,
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
AS SELECT
LOWER(akvo_tree_registration_areas_updated.country) as country,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
AKVO_tree_registration_photos.* FROM AKVO_tree_registration_photos
JOIN akvo_tree_registration_areas_updated
ON AKVO_tree_registration_photos.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo'''

conn.commit()

create_a26 = ''' CREATE TABLE s4g_ecosia_site_health
AS SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.organisation) as organisation,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.contract_number,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
akvo_tree_registration_areas_updated.tree_number AS number_trees_registered,
S4G_API_health_indicators.health_index,
S4G_API_health_indicators.health_index_normalized,
S4G_API_health_indicators.health_trend,
S4G_API_health_indicators.health_trend_normalized,
akvo_tree_registration_areas_updated.centroid_coord

FROM akvo_tree_registration_areas_updated
JOIN S4G_API_health_indicators
ON akvo_tree_registration_areas_updated.identifier_akvo = S4G_API_health_indicators.identifier_akvo'''

conn.commit()

create_a27 = ''' CREATE TABLE s4g_ecosia_fires
AS SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
s4g_api_fires.detection_date,
s4g_api_fires.confidence_level,
s4g_api_fires.area_ha AS "area_affected_ha",
akvo_tree_registration_areas_updated.centroid_coord

FROM akvo_tree_registration_areas_updated
JOIN S4G_API_fires
ON akvo_tree_registration_areas_updated.identifier_akvo = S4G_API_fires.identifier_akvo'''

conn.commit()

create_a28 = ''' CREATE TABLE s4g_ecosia_deforestation
AS SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) as country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
S4G_API_deforestation.deforestation_date,
S4G_API_deforestation.deforestation_nr_alerts,
S4G_API_deforestation.deforestation_area AS "area_affected_ha",
akvo_tree_registration_areas_updated.centroid_coord

FROM akvo_tree_registration_areas_updated
JOIN S4G_API_deforestation
ON akvo_tree_registration_areas_updated.identifier_akvo = S4G_API_deforestation.identifier_akvo'''

conn.commit()

create_a29 = ''' CREATE TABLE s4g_ecosia_landuse_cover
AS SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.organisation) as organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
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
akvo_tree_registration_areas_updated.centroid_coord

FROM akvo_tree_registration_areas_updated
JOIN s4g_API_landcover_change
ON akvo_tree_registration_areas_updated.identifier_akvo = s4g_API_landcover_change.identifier_akvo'''

conn.commit()

create_a30 = ''' CREATE TABLE s4g_ecosia_data_quality
AS SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.organisation) as organisation,
S4G_API_data_quality.partner_site_id,
S4G_API_data_quality.contract_number,
LOWER(S4G_API_data_quality.country),
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
akvo_tree_registration_areas_updated.centroid_coord

FROM akvo_tree_registration_areas_updated
JOIN S4G_API_data_quality
ON akvo_tree_registration_areas_updated.identifier_akvo = S4G_API_data_quality.identifier_akvo

ORDER BY S4G_API_data_quality.issues DESC'''

conn.commit()


create_a31 = '''CREATE TABLE superset_ecosia_nursery_registration
AS SELECT
akvo_nursery_registration.identifier_akvo,
akvo_nursery_registration.display_name,
akvo_nursery_registration.submitter,
akvo_nursery_registration.instance,
akvo_nursery_registration.submission,
LOWER(akvo_nursery_registration.country) AS country,
akvo_nursery_registration.test,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_nursery_registration.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-')))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/')))
ElSE
LOWER(akvo_nursery_registration.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_nursery_registration.nursery_type,
akvo_nursery_registration.nursery_name,
akvo_nursery_registration.newly_established,
akvo_nursery_registration.full_tree_capacity,
akvo_nursery_registration.lat_y,
akvo_nursery_registration.lon_x
--CALC_TAB_Error_partner_report_on_nursery_registration."species currently produced in nursery",
--CALC_TAB_Error_partner_report_on_nursery_registration."nr of photos taken during registration",
--CALC_TAB_Error_partner_report_on_nursery_registration."Check nr of photos taken during registration of the nursery"

FROM akvo_nursery_registration;
--LEFT JOIN CALC_TAB_Error_partner_report_on_nursery_registration
--ON CALC_TAB_Error_partner_report_on_nursery_registration.identifier_akvo = akvo_nursery_registration.identifier_akvo;

UPDATE superset_ecosia_nursery_registration
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_nursery_registration
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = ''
OR test = 'This is no test data';

UPDATE superset_ecosia_nursery_registration
SET organisation = LOWER(organisation);'''

conn.commit()


create_a32 = '''CREATE TABLE superset_ecosia_tree_registration
AS WITH COUNT_Total_number_of_photos_taken AS (

SELECT identifier_akvo, photo_url FROM AKVO_Tree_registration_photos
UNION ALL
SELECT identifier_akvo, photo_url FROM AKVO_Tree_monitoring_photos
UNION ALL
SELECT identifier_akvo, url_photo FROM AKVO_Tree_external_audits_photos
UNION ALL
SELECT identifier_akvo, photo_owner FROM AKVO_tree_registration_areas_updated
WHERE AKVO_tree_registration_areas_updated.photo_owner NOTNULL
UNION ALL
SELECT identifier_akvo, photo_url_4 FROM akvo_site_registration_distributed_trees_photos),

count_total_number_photos_per_site AS (SELECT identifier_akvo, COUNT(identifier_akvo) AS total_nr_photos
FROM COUNT_Total_number_of_photos_taken
GROUP BY identifier_akvo),

count_number_tree_species_registered AS (SELECT identifier_akvo, COUNT(*) AS nr_species_registered
FROM AKVO_Tree_registration_species
GROUP BY AKVO_Tree_registration_species.identifier_akvo),

COUNT_number_photos_png_format AS (
SELECT
identifier_akvo,
COUNT(identifier_akvo) AS nr_photos_png_format
FROM COUNT_Total_number_of_photos_taken
WHERE RIGHT(COUNT_Total_number_of_photos_taken.photo_url, 4) = '.png'
GROUP BY identifier_akvo)


SELECT
t.identifier_akvo,
t.display_name,
t.instance,
t.device_id,
t.submission,
t.submission_year,
t.submitter,
t.akvo_form_version,
t.country,
t.test,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
POWER(ASCII(LEFT(LOWER(t.organisation),1)),3),
POWER(ASCII(LEFT(LOWER(t.organisation),2)),2),
SQRT(POWER(ASCII(LEFT(LOWER(t.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

t.organisation,

CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-') - 2))
WHEN POSITION(' -' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-')))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/') - 1))
WHEN POSITION(' /' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/')))
ElSE
LOWER(t.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-')-1)))
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-'))))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/'))))
WHEN POSITION('/ ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

t.contract_number AS sub_contract,
t.id_planting_site,
t.land_title,
t.name_village,
t.name_region,
t.name_owner,
t.gender_owner,
t.objective_site,
t.site_preparation,
t.planting_technique,
t.planting_system,
t.remark,
t.nr_trees_option,
TO_DATE(t.planting_date, 'YYYY-MM-DD') AS planting_date,
t.tree_number,
t.estimated_area,
t.calc_area,
t.data_source AS akvo_form_used_for_data_collection,

-- The columns below are specifically from the "Tree registration unregistered farmers" table. Rows from other tables will be empty for these columns

t.confirm_plant_location_own_land,
t.one_multiple_planting_sites,
t.nr_trees_given_away,
t.nr_trees_received,
t.location_house_tree_receiver,
t.confirm_planting_location,
t.url_signature_tree_receiver,
t.total_number_trees_received,
t.check_ownership_trees,
t.gender_tree_receiver,
t.name_tree_receiver,
t.lat_y,
t.lon_x,
t.number_coord_polygon AS nr_points_in_polygon,

CASE
WHEN count_total_number_photos_per_site.total_nr_photos NOTNULL
THEN count_total_number_photos_per_site.total_nr_photos
WHEN count_total_number_photos_per_site.total_nr_photos ISNULL
THEN 0
END AS "number of tree photos taken",

CASE
WHEN COUNT_number_photos_png_format.nr_photos_png_format NOTNULL
THEN COUNT_number_photos_png_format.nr_photos_png_format
WHEN COUNT_number_photos_png_format.nr_photos_png_format ISNULL
THEN 0
END AS nr_photos_png_format,


count_number_tree_species_registered.nr_species_registered,
t.self_intersection AS polygon_has_selfintersection,
t.overlap AS polygon_has_overlap_with_other_polygon,
t.outside_country AS polygon_overlaps_country_boundary,
t.check_200_trees AS more_200_trees_no_polygon,
t.check_duplicate_polygons,
t.needle_shape AS polygon_is_spatially_distorted,
t.total_nr_geometric_errors AS total_nr_polygon_errors_found,

json_build_object(
'type', 'Polygon',
'geometry', ST_AsGeoJSON(t.polygon)::json)::text as geojson

FROM
akvo_tree_registration_areas_updated AS t

LEFT JOIN count_total_number_photos_per_site
ON count_total_number_photos_per_site.identifier_akvo = t.identifier_akvo
LEFT JOIN count_number_tree_species_registered
ON count_number_tree_species_registered.identifier_akvo = t.identifier_akvo
LEFT JOIN COUNT_number_photos_png_format
ON COUNT_number_photos_png_format.identifier_akvo = t.identifier_akvo;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_registration
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_registration
--SET contract = CAST(sub_contract AS INTEGER);
SET contract = TRUNC(sub_contract);


UPDATE superset_ecosia_tree_registration
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_tree_registration
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = ''
OR test = 'This is no test data';

UPDATE superset_ecosia_tree_registration
SET organisation = LOWER(organisation);

UPDATE superset_ecosia_tree_registration
SET country = LOWER(country);'''

conn.commit()

create_a33 = '''CREATE TABLE superset_ecosia_tree_monitoring
AS SELECT

CALC_TAB_monitoring_calculations_per_site.identifier_akvo,
CALC_TAB_monitoring_calculations_per_site.display_name,
LOWER(CALC_TAB_monitoring_calculations_per_site.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(CALC_TAB_monitoring_calculations_per_site.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(CALC_TAB_monitoring_calculations_per_site.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(CALC_TAB_monitoring_calculations_per_site.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN CALC_TAB_monitoring_calculations_per_site.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(CALC_TAB_monitoring_calculations_per_site.organisation)),
			LENGTH(CALC_TAB_monitoring_calculations_per_site.organisation) - POSITION('-' IN CALC_TAB_monitoring_calculations_per_site.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(CALC_TAB_monitoring_calculations_per_site.organisation)),
			LENGTH(CALC_TAB_monitoring_calculations_per_site.organisation) - POSITION('-' IN CALC_TAB_monitoring_calculations_per_site.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(CALC_TAB_monitoring_calculations_per_site.organisation)),
			LENGTH(CALC_TAB_monitoring_calculations_per_site.organisation) - POSITION('-' IN CALC_TAB_monitoring_calculations_per_site.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(CALC_TAB_monitoring_calculations_per_site.organisation) AS organisation,

CASE
WHEN POSITION('-' IN CALC_TAB_monitoring_calculations_per_site.organisation) > 0
THEN LOWER(left(CALC_TAB_monitoring_calculations_per_site.organisation, strpos(CALC_TAB_monitoring_calculations_per_site.organisation, '-') - 2))
WHEN POSITION('/' IN CALC_TAB_monitoring_calculations_per_site.organisation) > 0
THEN LOWER(left(CALC_TAB_monitoring_calculations_per_site.organisation, strpos(CALC_TAB_monitoring_calculations_per_site.organisation, '/') - 1))
ElSE
LOWER(CALC_TAB_monitoring_calculations_per_site.organisation)
END AS partner,

CASE
WHEN POSITION('-' IN CALC_TAB_monitoring_calculations_per_site.organisation) > 0
THEN LOWER(right(CALC_TAB_monitoring_calculations_per_site.organisation, (LENGTH(CALC_TAB_monitoring_calculations_per_site.organisation) - strpos(CALC_TAB_monitoring_calculations_per_site.organisation, '-')-1)))
WHEN POSITION('/' IN CALC_TAB_monitoring_calculations_per_site.organisation) > 0
THEN LOWER(right(CALC_TAB_monitoring_calculations_per_site.organisation, (LENGTH(CALC_TAB_monitoring_calculations_per_site.organisation) - strpos(CALC_TAB_monitoring_calculations_per_site.organisation, '/'))))
ElSE
''
END AS sub_partner,

CALC_TAB_monitoring_calculations_per_site.submitter,
CALC_TAB_monitoring_calculations_per_site.contract_number AS sub_contract,
CALC_TAB_monitoring_calculations_per_site.id_planting_site,
CALC_TAB_monitoring_calculations_per_site.calc_area,
CALC_TAB_monitoring_calculations_per_site.registered_tree_number,
CALC_TAB_monitoring_calculations_per_site.test,
CALC_TAB_monitoring_calculations_per_site.procedure,
CALC_TAB_monitoring_calculations_per_site.data_collection_method,
CALC_TAB_monitoring_calculations_per_site.avg_tree_distance_m,
CALC_TAB_monitoring_calculations_per_site.avg_tree_density,
CALC_TAB_monitoring_calculations_per_site.nr_trees_monitored,
CALC_TAB_monitoring_calculations_per_site.number_pcq_samples,
CALC_TAB_monitoring_calculations_per_site.planting_date,
CALC_TAB_monitoring_calculations_per_site.latest_monitoring_submission,
CALC_TAB_monitoring_calculations_per_site.nr_days_registration_monitoring,
CALC_TAB_monitoring_calculations_per_site.nr_years_registration_monitoring,
CALC_TAB_monitoring_calculations_per_site.label_strata,
CALC_TAB_monitoring_calculations_per_site.perc_trees_survived,
CALC_TAB_monitoring_calculations_per_site.avg_tree_height,
CALC_TAB_monitoring_calculations_per_site.site_impressions

FROM CALC_TAB_monitoring_calculations_per_site;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_monitoring
ADD contract NUMERIC(20,0);

UPDATE superset_ecosia_tree_monitoring
SET contract = TRUNC(sub_contract);'''

conn.commit()

create_a34 = '''CREATE TABLE superset_ecosia_s4g_site_health
AS SELECT
s4g_ecosia_site_health.identifier_akvo,
s4g_ecosia_site_health.display_name,
LOWER(s4g_ecosia_site_health.organisation) as organisation,
s4g_ecosia_site_health.id_planting_site,
s4g_ecosia_site_health.contract_number,
LOWER(s4g_ecosia_site_health.country) AS country,
s4g_ecosia_site_health.number_trees_registered,
s4g_ecosia_site_health.health_index,
s4g_ecosia_site_health.health_index_normalized,
s4g_ecosia_site_health.health_trend,
s4g_ecosia_site_health.health_trend_normalized,
s4g_ecosia_site_health.centroid_coord,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(s4g_ecosia_site_health.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(s4g_ecosia_site_health.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(s4g_ecosia_site_health.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN s4g_ecosia_site_health.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(s4g_ecosia_site_health.organisation)),
			LENGTH(s4g_ecosia_site_health.organisation) - POSITION('-' IN s4g_ecosia_site_health.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(s4g_ecosia_site_health.organisation)),
			LENGTH(s4g_ecosia_site_health.organisation) - POSITION('-' IN s4g_ecosia_site_health.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(s4g_ecosia_site_health.organisation)),
			LENGTH(s4g_ecosia_site_health.organisation) - POSITION('-' IN s4g_ecosia_site_health.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

akvo_tree_registration_areas_updated.lat_y,
akvo_tree_registration_areas_updated.lon_x

FROM s4g_ecosia_site_health
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_registration_areas_updated.identifier_akvo = s4g_ecosia_site_health.identifier_akvo;'''

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
s4g_ecosia_data_quality.buffer_area_around_point_location AS "artificial 50m buffer placed around mapped site (points)"

FROM
akvo_tree_registration_areas_updated
JOIN s4g_ecosia_data_quality
ON akvo_tree_registration_areas_updated.identifier_akvo = s4g_ecosia_data_quality.identifier_akvo
where polygon ISNULL;'''

conn.commit()

create_a36 = '''CREATE TABLE superset_ecosia_nursery_monitoring
AS SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_nursery_registration.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-')))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/')))
ElSE
LOWER(akvo_nursery_registration.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.*,
akvo_nursery_registration.lat_y,
akvo_nursery_registration.lon_x

FROM akvo_nursery_monitoring
JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = akvo_nursery_monitoring.identifier_akvo;

UPDATE superset_ecosia_nursery_monitoring
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_nursery_monitoring
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = ''
OR test = 'Valid data'
OR test = 'This is no test data';'''

conn.commit()

create_a37 = '''CREATE TABLE superset_ecosia_nursery_monitoring_species
AS SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_nursery_registration.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-')))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/')))
ElSE
LOWER(akvo_nursery_registration.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.submission_date,
akvo_nursery_monitoring_tree_species.*

FROM akvo_nursery_monitoring_tree_species
JOIN akvo_nursery_registration
ON akvo_nursery_monitoring_tree_species.identifier_akvo = akvo_nursery_registration.identifier_akvo
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring.instance = akvo_nursery_monitoring_tree_species.instance;'''

conn.commit()

create_a38 = '''CREATE TABLE superset_ecosia_nursery_registration_photos
AS SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_nursery_registration.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-')))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/')))
ElSE
LOWER(akvo_nursery_registration.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_nursery_registration.nursery_name,
akvo_nursery_registration.submission AS submission_date,
akvo_nursery_registration_photos.*

FROM akvo_nursery_registration_photos
JOIN akvo_nursery_registration
ON akvo_nursery_registration_photos.identifier_akvo =  akvo_nursery_registration.identifier_akvo;

ALTER TABLE superset_ecosia_nursery_registration_photos
ADD lat_y REAL;

ALTER TABLE superset_ecosia_nursery_registration_photos
ADD lon_x REAL;

UPDATE superset_ecosia_nursery_registration_photos
SET
lat_y = ST_Y(centroid_coord::geometry),
lon_x = ST_X(centroid_coord::geometry)
WHERE centroid_coord NOTNULL;

ALTER TABLE superset_ecosia_nursery_registration_photos
ADD photo_url_preset TEXT;

UPDATE superset_ecosia_nursery_registration_photos
SET photo_url_preset = CONCAT('<img src="', photo_url, '" alt="s3 image" height=200/>')
WHERE photo_url NOTNULL;'''

conn.commit()

create_a39 = '''CREATE TABLE superset_ecosia_nursery_monitoring_photos
AS SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_nursery_registration.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '-')))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(left(akvo_nursery_registration.organisation, strpos(akvo_nursery_registration.organisation, '/')))
ElSE
LOWER(akvo_nursery_registration.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_nursery_registration.nursery_name,
akvo_nursery_monitoring.submission_date,
akvo_nursery_monitoring_photos.*

FROM akvo_nursery_monitoring_photos
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring.instance = akvo_nursery_monitoring_photos.instance
JOIN akvo_nursery_registration
ON akvo_nursery_monitoring_photos.identifier_akvo =  akvo_nursery_registration.identifier_akvo;

ALTER TABLE superset_ecosia_nursery_monitoring_photos
ADD lat_y REAL;

ALTER TABLE superset_ecosia_nursery_monitoring_photos
ADD lon_x REAL;

UPDATE superset_ecosia_nursery_monitoring_photos
SET
lat_y = ST_Y(centroid_coord::geometry),
lon_x = ST_X(centroid_coord::geometry)
WHERE centroid_coord NOTNULL;

UPDATE superset_ecosia_nursery_monitoring_photos
SET photo_url = RIGHT(photo_url, strpos(reverse(photo_url),'/'));

UPDATE superset_ecosia_nursery_monitoring_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images',photo_url);

ALTER TABLE superset_ecosia_nursery_monitoring_photos
ADD photo_url_preset TEXT;

UPDATE superset_ecosia_nursery_monitoring_photos
SET photo_url_preset = CONCAT('<img src="', photo_url, '" alt="s3 image" height=200/>')
WHERE photo_url NOTNULL;'''

conn.commit()

create_a40 = '''CREATE TABLE superset_ecosia_tree_registration_photos
AS SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Tree registration' AS procedure,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,
akvo_tree_registration_photos.identifier_akvo,
akvo_tree_registration_photos.instance,
akvo_tree_registration_photos.photo_url,
akvo_tree_registration_photos.photo_geotag_location,
akvo_tree_registration_photos.photo_gps_location

FROM
akvo_tree_registration_photos
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_photos.identifier_akvo

-- With this UNION below we collect the photo urls of farmers taken during registration. These photos are stored
-- in the tree registration areas table instead of the tree registration photos table
UNION ALL

SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Tree registration' AS procedure,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.instance,

akvo_tree_registration_areas_updated.photo_owner AS photo_url,

NULL AS photo_geotag_location,
akvo_tree_registration_areas_updated.centroid_coord AS photo_gps_location
FROM akvo_tree_registration_areas_updated
where akvo_tree_registration_areas_updated.photo_owner NOTNULL
AND NOT akvo_tree_registration_areas_updated.photo_owner ~ '^\s*$';  -- This last row is needed as not all empty rows are captured by the NOTNULL (strange)

ALTER TABLE superset_ecosia_tree_registration_photos
ADD lat_y REAL;

ALTER TABLE superset_ecosia_tree_registration_photos
ADD lon_x REAL;

UPDATE superset_ecosia_tree_registration_photos
SET
lat_y = ST_Y(photo_gps_location::geometry),
lon_x = ST_X(photo_gps_location::geometry)
WHERE photo_gps_location NOTNULL;

UPDATE superset_ecosia_tree_registration_photos
SET
lat_y = ST_Y(photo_geotag_location::geometry),
lon_x = ST_X(photo_geotag_location::geometry)
WHERE photo_geotag_location NOTNULL;

ALTER TABLE superset_ecosia_tree_registration_photos
ADD photo_url_preset TEXT;

UPDATE superset_ecosia_tree_registration_photos
SET photo_url_preset = CONCAT('<img src="', photo_url, '" alt="s3 image" height=200/>')
WHERE photo_url NOTNULL;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_registration_photos
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_registration_photos
SET contract = TRUNC(sub_contract);'''

conn.commit()


create_a41 = '''CREATE TABLE superset_ecosia_tree_registration_species
AS SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,


LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,
AKVO_tree_registration_species.*

FROM AKVO_tree_registration_species
JOIN akvo_tree_registration_areas_updated
ON AKVO_tree_registration_species.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_registration_species
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_registration_species
SET contract = TRUNC(sub_contract);'''

conn.commit()

create_a42 = '''CREATE TABLE superset_ecosia_s4g_fires
AS SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) as country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.id_planting_site,
s4g_ecosia_fires.detection_date,
s4g_ecosia_fires.confidence_level,
s4g_ecosia_fires."area_affected_ha",
akvo_tree_registration_areas_updated.centroid_coord,

akvo_tree_registration_areas_updated.lat_y,
akvo_tree_registration_areas_updated.lon_x

FROM s4g_ecosia_fires
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_registration_areas_updated.identifier_akvo = s4g_ecosia_fires.identifier_akvo;'''

conn.commit()

create_a43 = '''CREATE TABLE superset_ecosia_s4g_deforestation
AS SELECT

s4g_ecosia_deforestation.identifier_akvo,
s4g_ecosia_deforestation.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.id_planting_site,
s4g_ecosia_deforestation.deforestation_date,
s4g_ecosia_deforestation.deforestation_nr_alerts,
s4g_ecosia_deforestation."area_affected_ha",
akvo_tree_registration_areas_updated.centroid_coord,

akvo_tree_registration_areas_updated.lat_y,
akvo_tree_registration_areas_updated.lon_x

FROM s4g_ecosia_deforestation
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_registration_areas_updated.identifier_akvo = s4g_ecosia_deforestation.identifier_akvo;'''


create_a44 = '''CREATE TABLE superset_ecosia_geolocations
-- Here we convert the polygon areas from WKT format to geojson string format that can be read by superset
AS WITH
wkt_polygons_to_geojson AS (
SELECT
t.identifier_akvo,
t.instance,
t.submission,
t.test,
LOWER(t.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(t.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(t.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(t.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(t.organisation) AS organisation,

CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-') - 2))
WHEN POSITION(' -' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-')))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/') - 1))
WHEN POSITION(' /' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/')))
ElSE
LOWER(t.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-')-1)))
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-'))))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/'))))
WHEN POSITION('/ ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

t.contract_number AS sub_contract,
t.display_name,
'tree registration' AS procedure,
'locations_more_200_trees' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'locations_more_200_trees',
        'geometry',   ST_AsGeoJSON(t.polygon)::json)

    ))::text AS superset_geojson
FROM akvo_tree_registration_areas_updated AS t
where t.polygon NOTNULL
GROUP BY t.identifier_akvo, t.instance, t.test, t.organisation, t.contract_number, t.display_name,
t.submission, t.country),


-- Here we convert the centroid-point locations from WKT format to geojson string format that can be read by superset
buffer_around_200_trees_centroids AS (SELECT
identifier_akvo,
t.instance,
t.submission,
t.test,
LOWER(t.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(t.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(t.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(t.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(t.organisation) AS organisation,

CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-') - 2))
WHEN POSITION(' -' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-')))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/') - 1))
WHEN POSITION(' /' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/')))
ElSE
LOWER(t.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-')-1)))
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-'))))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/'))))
WHEN POSITION('/ ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

t.contract_number AS sub_contract,
t.display_name,
'tree registration' AS procedure,
ST_Buffer(t.centroid_coord,25) as buffer
FROM akvo_tree_registration_areas_updated AS t
WHERE t.polygon ISNULL),

-- Here we convert the buffer polygon areas (WKT format) to geojson string format that can be read by superset
wkt_buffer_200_trees_areas_to_geojson AS (
SELECT
t.identifier_akvo,
t.instance,
t.submission,
t.test,
LOWER(t.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(t.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(t.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(t.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(t.organisation)),
			LENGTH(t.organisation) - POSITION('-' IN t.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(t.organisation) AS organisation,

CASE
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-') - 2))
WHEN POSITION(' -' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '-')))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/') - 1))
WHEN POSITION(' /' IN t.organisation) > 0
THEN LOWER(left(t.organisation, strpos(t.organisation, '/')))
ElSE
LOWER(t.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-')-1)))
WHEN POSITION('-' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '-'))))
WHEN POSITION('/' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/'))))
WHEN POSITION('/ ' IN t.organisation) > 0
THEN LOWER(right(t.organisation, (LENGTH(t.organisation) - strpos(t.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

t.sub_contract,
t.display_name,
'tree registration' AS procedure,
'locations_less_200_trees' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'locations_less_200_trees',
        'geometry',   ST_AsGeoJSON(t.buffer)::json)

    ))::text AS superset_geojson

FROM buffer_around_200_trees_centroids AS t
group by t.identifier_akvo, t.instance, t.test, t.organisation, t.sub_contract, t.display_name,
t.submission, t.country),

-- Here we convert the PCQ MONITORING sample point locations from WKT format to geojson string format that can be read by superset
wkt_pcq_samples_monitoring_to_geojson AS
(SELECT
pcq_samples_monitorings.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.submission,
akvo_tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.display_name,
'PCQ sample location monitoring' AS procedure,

'PCQ sample location' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'PCQ sample locations monitoring',
        'geometry',   ST_AsGeoJSON(pcq_samples_monitorings.pcq_location)::json)

    ))::text AS superset_geojson
FROM akvo_tree_monitoring_pcq AS pcq_samples_monitorings
JOIN akvo_tree_registration_areas_updated
ON pcq_samples_monitorings.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_monitoring_areas
ON akvo_tree_monitoring_areas.identifier_akvo = pcq_samples_monitorings.identifier_akvo
GROUP BY pcq_samples_monitorings.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),

-- Here we convert the photo (GEOTAG) locations (TREE REGISTRATION) from WKT format to geojson string format that can be read by superset
wkt_photo_registration_geotag_to_geojson AS
(SELECT
tree_registration_photos_geotag.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.submission,
akvo_tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.display_name,
'Photo registration' AS procedure,

'Photo registration' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'Photo registration',
        'geometry',   ST_AsGeoJSON(tree_registration_photos_geotag.photo_geotag_location)::json)

    ))::text AS superset_geojson


FROM akvo_tree_registration_photos AS tree_registration_photos_geotag
JOIN akvo_tree_registration_areas_updated
ON tree_registration_photos_geotag.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_monitoring_areas
ON akvo_tree_monitoring_areas.identifier_akvo = tree_registration_photos_geotag.identifier_akvo
WHERE tree_registration_photos_geotag.photo_geotag_location NOTNULL
GROUP BY tree_registration_photos_geotag.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),

-- Here we convert the photo (GPS) locations (TREE REGISTRATION) from WKT format to geojson string format that can be read by superset
wkt_photo_registration_gps_to_geojson AS
(SELECT
tree_registration_photos_gps.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.submission,
akvo_tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.display_name,
'Photo registration' AS procedure,

'Photo registration' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'Photo registration',
        'geometry',   ST_AsGeoJSON(tree_registration_photos_gps.photo_gps_location)::json)

    ))::text AS superset_geojson
FROM akvo_tree_registration_photos AS tree_registration_photos_gps
JOIN akvo_tree_registration_areas_updated
ON tree_registration_photos_gps.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_monitoring_areas
ON akvo_tree_monitoring_areas.identifier_akvo = tree_registration_photos_gps.identifier_akvo
WHERE tree_registration_photos_gps.photo_geotag_location ISNULL
GROUP BY tree_registration_photos_gps.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),

-- Here we convert the PCQ sample point AUDIT locations from WKT format to geojson string format that can be read by superset
wkt_pcq_samples_audit_to_geojson AS
(SELECT
pcq_samples_audits.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.submission,
akvo_tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,


akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.display_name,
'PCQ sample location monitoring' AS procedure,

'PCQ sample location' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'PCQ sample locations monitoring',
        'geometry',   ST_AsGeoJSON(pcq_samples_audits.pcq_location)::json)

    ))::text AS superset_geojson
FROM akvo_tree_external_audits_pcq AS pcq_samples_audits
JOIN akvo_tree_registration_areas_updated
ON pcq_samples_audits.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_monitoring_areas
ON akvo_tree_monitoring_areas.identifier_akvo = pcq_samples_audits.identifier_akvo
GROUP BY pcq_samples_audits.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),


-- Here we convert the COUNT sample MONITORING locations from WKT format to geojson string format that can be read by superset
wkt_count_samples_monitoring_to_geojson AS
(SELECT
count_samples_monitoring.identifier_akvo,
count_samples_monitoring.instance,
akvo_tree_registration_areas_updated.submission,
count_samples_monitoring.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.display_name,
'COUNT sample location monitoring' AS procedure,

'Monitoring COUNT location' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'COUNT sample location monitoring',
        'geometry',   ST_AsGeoJSON(count_samples_monitoring.location_monitoring)::json)

    ))::text AS superset_geojson
FROM akvo_tree_monitoring_areas AS count_samples_monitoring
JOIN akvo_tree_registration_areas_updated
ON count_samples_monitoring.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
GROUP BY count_samples_monitoring.identifier_akvo,
count_samples_monitoring.instance,
akvo_tree_registration_areas_updated.organisation,
count_samples_monitoring.test,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),


-- Here we convert the COUNT sample AUDIT locations from WKT format to geojson string format that can be read by superset
wkt_count_samples_audit_to_geojson AS
(SELECT
count_samples_audit.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.submission,
akvo_tree_monitoring_areas.test,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
akvo_tree_registration_areas_updated.display_name,
'COUNT sample location audit' AS procedure,

'Audit COUNT location' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'COUNT sample location audit',
        'geometry',   ST_AsGeoJSON(count_samples_audit.location_external_audit)::json)

    ))::text AS superset_geojson
FROM akvo_tree_external_audits_areas AS count_samples_audit
JOIN akvo_tree_registration_areas_updated
ON count_samples_audit.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_monitoring_areas
ON akvo_tree_monitoring_areas.identifier_akvo = count_samples_audit.identifier_akvo
GROUP BY count_samples_audit.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_monitoring_areas.test,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country)


SELECT * FROM wkt_polygons_to_geojson
UNION ALL
SELECT * FROM wkt_buffer_200_trees_areas_to_geojson
UNION ALL
SELECT * FROM wkt_pcq_samples_monitoring_to_geojson
UNION ALL
SELECT * FROM wkt_photo_registration_geotag_to_geojson
UNION ALL
SELECT * FROM wkt_photo_registration_gps_to_geojson
UNION ALL
SELECT * FROM wkt_pcq_samples_audit_to_geojson
UNION ALL
SELECT * FROM wkt_count_samples_monitoring_to_geojson
UNION All
SELECT * FROM wkt_count_samples_audit_to_geojson;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_geolocations
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_geolocations
SET contract = TRUNC(sub_contract);'''

conn.commit()


create_a45 = '''CREATE TABLE superset_ecosia_tree_monitoring_photos
AS SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,


-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Monitoring' as procedure,
akvo_tree_monitoring_photos.identifier_akvo,
akvo_tree_monitoring_photos.instance,
akvo_tree_monitoring_areas.submitter,
akvo_tree_monitoring_areas.submission,
akvo_tree_monitoring_photos.photo_url,
akvo_tree_monitoring_photos.photo_location

FROM akvo_tree_monitoring_photos
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_monitoring_photos.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_monitoring_areas
ON akvo_tree_monitoring_photos.instance = akvo_tree_monitoring_areas.instance

UNION All

SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas_updated.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas_updated.organisation)),
			LENGTH(akvo_tree_registration_areas_updated.organisation) - POSITION('-' IN akvo_tree_registration_areas_updated.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,

CASE
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-') - 2))
WHEN POSITION(' -' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '-')))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/') - 1))
WHEN POSITION(' /' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(left(akvo_tree_registration_areas_updated.organisation, strpos(akvo_tree_registration_areas_updated.organisation, '/')))
ElSE
LOWER(akvo_tree_registration_areas_updated.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '-'))))
WHEN POSITION('/' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_tree_registration_areas_updated.organisation) > 0
THEN LOWER(right(akvo_tree_registration_areas_updated.organisation, (LENGTH(akvo_tree_registration_areas_updated.organisation) - strpos(akvo_tree_registration_areas_updated.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Audit' as procedure,

AKVO_Tree_external_audits_photos.identifier_akvo,
AKVO_Tree_external_audits_photos.instance,
akvo_tree_external_audits_areas.submitter,
akvo_tree_external_audits_areas.submission,
AKVO_Tree_external_audits_photos.url_photo AS photo_url,
AKVO_Tree_external_audits_photos.location_photo AS photo_location

FROM AKVO_Tree_external_audits_photos
JOIN akvo_tree_registration_areas_updated
ON AKVO_Tree_external_audits_photos.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo
JOIN akvo_tree_external_audits_areas
ON AKVO_Tree_external_audits_photos.instance = akvo_tree_external_audits_areas.instance;

ALTER TABLE superset_ecosia_tree_monitoring_photos
ADD lat_y REAL;

ALTER TABLE superset_ecosia_tree_monitoring_photos
ADD lon_x REAL;

UPDATE superset_ecosia_tree_monitoring_photos
SET
lat_y = ST_Y(photo_location::geometry),
lon_x = ST_X(photo_location::geometry)
WHERE photo_location NOTNULL;

ALTER TABLE superset_ecosia_tree_monitoring_photos
ADD photo_url_preset TEXT;

UPDATE superset_ecosia_tree_monitoring_photos
SET photo_url_preset = CONCAT('<img src="', photo_url, '" alt="s3 image" height=200/>')
WHERE photo_url NOTNULL;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_monitoring_photos
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_monitoring_photos
SET contract = TRUNC(sub_contract);'''

conn.commit()

create_a46 = '''CREATE TABLE superset_ecosia_tree_registration_light
AS SELECT

table_t.identifier_akvo,
table_t.display_name,
table_t.device_id,
table_t.instance,
table_t.submission,
table_t.akvo_form_version,
LOWER(table_t.country) AS country,
table_t.test,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN table_t.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(table_t.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(table_t.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(table_t.organisation),3)),4))) AS NUMERIC)
ELSE NULL
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN table_t.organisation) > 0 AND table_t.organisation NOTNULL
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(table_t.organisation)),
			LENGTH(table_t.organisation) - POSITION('-' IN table_t.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(table_t.organisation)),
			LENGTH(table_t.organisation) - POSITION('-' IN table_t.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(table_t.organisation)),
			LENGTH(table_t.organisation) - POSITION('-' IN table_t.organisation) - 3)),2)) AS NUMERIC)
ELSE 0
END AS partnercode_sub,

LOWER(table_t.organisation) AS organisation,

CASE
WHEN POSITION('-' IN table_t.organisation) > 0
THEN LOWER(left(table_t.organisation, strpos(table_t.organisation, '-') - 2))
WHEN POSITION(' -' IN table_t.organisation) > 0
THEN LOWER(left(table_t.organisation, strpos(table_t.organisation, '-')))
WHEN POSITION('/' IN table_t.organisation) > 0
THEN LOWER(left(table_t.organisation, strpos(table_t.organisation, '/') - 1))
WHEN POSITION(' /' IN table_t.organisation) > 0
THEN LOWER(left(table_t.organisation, strpos(table_t.organisation, '/')))
ElSE
LOWER(table_t.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN table_t.organisation) > 0
THEN LOWER(right(table_t.organisation, (LENGTH(table_t.organisation) - strpos(table_t.organisation, '-')-1)))
WHEN POSITION('-' IN table_t.organisation) > 0
THEN LOWER(right(table_t.organisation, (LENGTH(table_t.organisation) - strpos(table_t.organisation, '-'))))
WHEN POSITION('/' IN table_t.organisation) > 0
THEN LOWER(right(table_t.organisation, (LENGTH(table_t.organisation) - strpos(table_t.organisation, '/'))))
WHEN POSITION('/ ' IN table_t.organisation) > 0
THEN LOWER(right(table_t.organisation, (LENGTH(table_t.organisation) - strpos(table_t.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

table_t.contract_number AS sub_contract,
table_t.id_planting_site,
table_t.name_village,
table_t.name_owner,
table_t.remark,
TO_DATE(table_t.planting_date, 'YYYY-MM-DD') AS planting_date,
table_t.tree_number,
table_t.planting_distance,
table_t.only_location,
table_c.gps_points_combined

FROM akvo_tree_registration_locations_light_version table_t
cross join lateral (
       values (gps_corner_1), (gps_corner_2), (gps_corner_3), (gps_corner_4)
   ) as table_c(gps_points_combined);

ALTER TABLE superset_ecosia_tree_registration_light
ADD lat_y REAL;

ALTER TABLE superset_ecosia_tree_registration_light
ADD lon_x REAL;

UPDATE superset_ecosia_tree_registration_light
SET
lat_y = ST_Y(gps_points_combined::geometry),
lon_x = ST_X(gps_points_combined::geometry);

UPDATE superset_ecosia_tree_registration_light
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_tree_registration_light
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = '';

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_registration_light
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_registration_light
SET contract = TRUNC(sub_contract);'''

conn.commit()

create_a47 = '''CREATE TABLE superset_ecosia_tree_distribution_unregistered_farmers
AS SELECT

AKVO_tree_distribution_unregistered_farmers.identifier_akvo,
AKVO_tree_distribution_unregistered_farmers.display_name,
AKVO_tree_distribution_unregistered_farmers.device_id,
AKVO_tree_distribution_unregistered_farmers.instance,
AKVO_tree_distribution_unregistered_farmers.submission,
AKVO_tree_distribution_unregistered_farmers.akvo_form_version,
LOWER(AKVO_tree_distribution_unregistered_farmers.country) AS country,
AKVO_tree_distribution_unregistered_farmers.test,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN AKVO_tree_distribution_unregistered_farmers.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(AKVO_tree_distribution_unregistered_farmers.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(AKVO_tree_distribution_unregistered_farmers.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(AKVO_tree_distribution_unregistered_farmers.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)),
			LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)),
			LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)),
			LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(AKVO_tree_distribution_unregistered_farmers.organisation) AS organisation,

CASE
WHEN POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-') - 2))
WHEN POSITION(' -' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-')))
WHEN POSITION('/' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/') - 1))
WHEN POSITION(' /' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/')))
ElSE
LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-')-1)))
WHEN POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-'))))
WHEN POSITION('/' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/'))))
WHEN POSITION('/ ' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

AKVO_tree_distribution_unregistered_farmers.contract_number AS sub_contract,
AKVO_tree_distribution_unregistered_farmers.name_tree_receiver,
AKVO_tree_distribution_unregistered_farmers.gender_tree_receiver,
AKVO_tree_distribution_unregistered_farmers.check_ownership_trees,
AKVO_tree_distribution_unregistered_farmers.check_ownership_land,
AKVO_tree_distribution_unregistered_farmers.url_photo_receiver_trees,
AKVO_tree_distribution_unregistered_farmers.url_photo_id_card_tree_receiver,
AKVO_tree_distribution_unregistered_farmers.location_house_tree_receiver,
AKVO_tree_distribution_unregistered_farmers.name_site_id_tree_planting,
AKVO_tree_distribution_unregistered_farmers.confirm_planting_location,
AKVO_tree_distribution_unregistered_farmers.total_number_trees_received,
AKVO_tree_distribution_unregistered_farmers.url_signature_tree_receiver

FROM AKVO_tree_distribution_unregistered_farmers;

UPDATE superset_ecosia_tree_distribution_unregistered_farmers
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_tree_distribution_unregistered_farmers
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = '';

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_distribution_unregistered_farmers
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_distribution_unregistered_farmers
SET contract = TRUNC(sub_contract);'''

conn.commit()


create_a48 = '''CREATE TABLE superset_ecosia_site_registration_unregistered_farmers
AS SELECT

AKVO_site_registration_distributed_trees.identifier_akvo,
AKVO_site_registration_distributed_trees.displayname,
AKVO_site_registration_distributed_trees.device_id,
AKVO_site_registration_distributed_trees.instance,
AKVO_site_registration_distributed_trees.submitter,
AKVO_site_registration_distributed_trees.submission_date,
AKVO_site_registration_distributed_trees.form_version,
LOWER(AKVO_tree_distribution_unregistered_farmers.country) AS country,
AKVO_site_registration_distributed_trees.test,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(AKVO_tree_distribution_unregistered_farmers.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(AKVO_tree_distribution_unregistered_farmers.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(AKVO_tree_distribution_unregistered_farmers.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)),
			LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)),
			LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)),
			LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(AKVO_tree_distribution_unregistered_farmers.organisation) AS organisation,

CASE
WHEN POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-') - 2))
WHEN POSITION(' -' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-')))
WHEN POSITION('/' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/') - 1))
WHEN POSITION(' /' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(left(AKVO_tree_distribution_unregistered_farmers.organisation, strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/')))
ElSE
LOWER(AKVO_tree_distribution_unregistered_farmers.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-')-1)))
WHEN POSITION('-' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '-'))))
WHEN POSITION('/' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/'))))
WHEN POSITION('/ ' IN AKVO_tree_distribution_unregistered_farmers.organisation) > 0
THEN LOWER(right(AKVO_tree_distribution_unregistered_farmers.organisation, (LENGTH(AKVO_tree_distribution_unregistered_farmers.organisation) - strpos(AKVO_tree_distribution_unregistered_farmers.organisation, '/')-1)))
ElSE
''
END AS sub_partner,


AKVO_tree_distribution_unregistered_farmers.contract_number AS sub_contract,
AKVO_site_registration_distributed_trees.name_region_village_planting_site,
AKVO_site_registration_distributed_trees.name_owner_planting_site,
AKVO_site_registration_distributed_trees.gender_owner_planting_site,
AKVO_site_registration_distributed_trees.photo_owner_planting_site,
AKVO_site_registration_distributed_trees.nr_trees_received,
AKVO_site_registration_distributed_trees.confirm_plant_location_own_land,
AKVO_site_registration_distributed_trees.one_multiple_planting_sites,
AKVO_site_registration_distributed_trees.nr_trees_given_away,
AKVO_site_registration_distributed_trees.more_less_200_trees,
AKVO_site_registration_distributed_trees.date_tree_planting,
AKVO_site_registration_distributed_trees.centroid_coord,
AKVO_site_registration_distributed_trees.polygon,
AKVO_site_registration_distributed_trees.number_coord_pol,
AKVO_site_registration_distributed_trees.area_ha,
AKVO_site_registration_distributed_trees.avg_tree_distance,
AKVO_site_registration_distributed_trees.estimated_area,
AKVO_site_registration_distributed_trees.unit_estimated_area,
AKVO_site_registration_distributed_trees.estimated_tree_number_planted,
AKVO_site_registration_distributed_trees.confirm_additional_photos,
AKVO_site_registration_distributed_trees.comment_enumerator

FROM AKVO_site_registration_distributed_trees
JOIN AKVO_tree_distribution_unregistered_farmers
ON AKVO_tree_distribution_unregistered_farmers.identifier_akvo
= AKVO_site_registration_distributed_trees.identifier_akvo;

UPDATE superset_ecosia_site_registration_unregistered_farmers
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_site_registration_unregistered_farmers
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = '';

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_site_registration_unregistered_farmers
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_site_registration_unregistered_farmers
SET contract = TRUNC(sub_contract);'''

conn.commit()

create_a49 = '''CREATE TABLE superset_ecosia_contract_overview
AS SELECT

CALC_TAB_tree_submissions_per_contract.name_country,
CALC_TAB_tree_submissions_per_contract.organisation,

CASE
WHEN POSITION('-' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(left(CALC_TAB_tree_submissions_per_contract.organisation, strpos(CALC_TAB_tree_submissions_per_contract.organisation, '-') - 1))
WHEN POSITION(' -' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(left(CALC_TAB_tree_submissions_per_contract.organisation, strpos(CALC_TAB_tree_submissions_per_contract.organisation, '-')))
WHEN POSITION('/' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(left(CALC_TAB_tree_submissions_per_contract.organisation, strpos(CALC_TAB_tree_submissions_per_contract.organisation, '/') - 1))
WHEN POSITION(' /' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(left(CALC_TAB_tree_submissions_per_contract.organisation, strpos(CALC_TAB_tree_submissions_per_contract.organisation, '/')))
ElSE
LOWER(CALC_TAB_tree_submissions_per_contract.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(right(CALC_TAB_tree_submissions_per_contract.organisation, (LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - strpos(CALC_TAB_tree_submissions_per_contract.organisation, '-')-1)))
WHEN POSITION('-' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(right(CALC_TAB_tree_submissions_per_contract.organisation, (LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - strpos(CALC_TAB_tree_submissions_per_contract.organisation, '-'))))
WHEN POSITION('/' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(right(CALC_TAB_tree_submissions_per_contract.organisation, (LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - strpos(CALC_TAB_tree_submissions_per_contract.organisation, '/'))))
WHEN POSITION('/ ' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN LOWER(right(CALC_TAB_tree_submissions_per_contract.organisation, (LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - strpos(CALC_TAB_tree_submissions_per_contract.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(CALC_TAB_tree_submissions_per_contract.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(CALC_TAB_tree_submissions_per_contract.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(CALC_TAB_tree_submissions_per_contract.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN CALC_TAB_tree_submissions_per_contract.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(CALC_TAB_tree_submissions_per_contract.organisation)),
			LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - POSITION('-' IN CALC_TAB_tree_submissions_per_contract.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(CALC_TAB_tree_submissions_per_contract.organisation)),
			LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - POSITION('-' IN CALC_TAB_tree_submissions_per_contract.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(CALC_TAB_tree_submissions_per_contract.organisation)),
			LENGTH(CALC_TAB_tree_submissions_per_contract.organisation) - POSITION('-' IN CALC_TAB_tree_submissions_per_contract.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

CALC_TAB_tree_submissions_per_contract."Contract number" AS sub_contract,
CALC_TAB_tree_submissions_per_contract."Total number of planting sites registered at t=0",
CALC_TAB_tree_submissions_per_contract."Total number of trees registered at t=0",
CALC_TAB_tree_submissions_per_contract."Latest submitted registration at t=0",
CALC_TAB_tree_submissions_per_contract."number of sites monitored/audited in t=1",
CALC_TAB_tree_submissions_per_contract."percentage of sites monitored/audited in t=1",
CALC_TAB_tree_submissions_per_contract."percentage of trees monitored/audited in t=1",
CALC_TAB_tree_submissions_per_contract."weighted avg perc tree_survival in t=1",
CALC_TAB_tree_submissions_per_contract."total tree number in t=1",
CALC_TAB_tree_submissions_per_contract."weighted avg tree_height in t1",
CALC_TAB_tree_submissions_per_contract."number of sites monitored/audited in t=2",
CALC_TAB_tree_submissions_per_contract."percentage of sites monitored/audited in t=2",
CALC_TAB_tree_submissions_per_contract."percentage of trees monitored/audited in t=2",
CALC_TAB_tree_submissions_per_contract."weighted avg perc tree_survival in t=2",
CALC_TAB_tree_submissions_per_contract."total tree number in t=2",
CALC_TAB_tree_submissions_per_contract."weighted avg tree_height in t=2",
CALC_TAB_tree_submissions_per_contract."number of sites monitored/audited in t=>3",
CALC_TAB_tree_submissions_per_contract."percentage of sites monitored/audited in t=>3",
CALC_TAB_tree_submissions_per_contract."percentage of trees monitored/audited in t=>3",
CALC_TAB_tree_submissions_per_contract."weighted avg perc tree_survival in t=>3",
CALC_TAB_tree_submissions_per_contract."total tree number in t=>3",
CALC_TAB_tree_submissions_per_contract."weighted avg tree_height in t=>3",
CALC_TAB_tree_submissions_per_contract."Total number of sites monitored/audited at least 1 time",
CALC_TAB_tree_submissions_per_contract."Number of tree species registered"

FROM CALC_TAB_tree_submissions_per_contract;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_contract_overview
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_contract_overview
SET contract = TRUNC(sub_contract);'''

conn.commit()

create_a50 = ('''
CREATE TABLE superset_ecosia_global_tree_species_distribution
AS SELECT
b.centroid_coord,
a.identifier_akvo,
'tree registration' AS source_species,

CASE
WHEN POSITION('_' IN a.lat_name_species) > 0
THEN LEFT(a.lat_name_species, LENGTH(a.lat_name_species)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN a.lat_name_species) > 0
THEN RIGHT(a.lat_name_species, 3)
END AS IUCN_redlist,

a.local_name_species
FROM akvo_tree_registration_species a
JOIN akvo_tree_registration_areas_updated b
ON a.identifier_akvo = b.identifier_akvo

UNION

SELECT
d.centroid_coord,
c.identifier_akvo,
'tree monitoring COUNTS' AS source_species,

CASE
WHEN POSITION('_' IN c.name_species) > 0
THEN LEFT(c.name_species, LENGTH(c.name_species)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN c.name_species) > 0
THEN RIGHT(c.name_species, 3)
END AS IUCN_redlist,

c.loc_name_spec AS local_name_species
FROM akvo_tree_monitoring_counts c
JOIN akvo_tree_registration_areas_updated d
ON c.identifier_akvo = d.identifier_akvo

UNION ALL

SELECT
e.pcq_location AS centroid_coord,
e.identifier_akvo,
'tree monitoring PCQ' AS source_species,
--e.q1_spec AS lat_name_species,
CASE
WHEN POSITION('_' IN e.q1_spec) > 0
THEN LEFT(e.q1_spec, LENGTH(e.q1_spec)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN e.q1_spec) > 0
THEN RIGHT(e.q1_spec, 3)
END AS IUCN_redlist,

'' AS local_name_species
FROM akvo_tree_monitoring_pcq e
WHERE NOT e.q1_spec = ''
AND NOT e.q1_spec = 'other'

UNION ALL

SELECT
f.pcq_location AS centroid_coord,
f.identifier_akvo,
'tree monitoring PCQ' AS source_species,
--f.q2_spec AS lat_name_species,

CASE
WHEN POSITION('_' IN f.q2_spec) > 0
THEN LEFT(f.q2_spec, LENGTH(f.q2_spec)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN f.q2_spec) > 0
THEN RIGHT(f.q2_spec, 3)
END AS IUCN_redlist,

'' AS local_name_species
FROM akvo_tree_monitoring_pcq f
WHERE NOT f.q2_spec = ''
AND NOT f.q2_spec = 'other'

UNION ALL

SELECT
g.pcq_location AS centroid_coord,
g.identifier_akvo,
'tree monitoring PCQ' AS source_species,
--g.q3_spec AS lat_name_species,

CASE
WHEN POSITION('_' IN g.q3_spec) > 0
THEN LEFT(g.q3_spec, LENGTH(g.q3_spec)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN g.q3_spec) > 0
THEN RIGHT(g.q3_spec, 3)
END AS IUCN_redlist,

'' AS local_name_species
FROM akvo_tree_monitoring_pcq g
WHERE NOT g.q3_spec = ''
AND NOT g.q3_spec = 'other'

UNION ALL

SELECT
h.pcq_location AS centroid_coord,
h.identifier_akvo,
'tree monitoring PCQ' AS source_species,
--h.q4_spec AS lat_name_species,

CASE
WHEN POSITION('_' IN h.q4_spec) > 0
THEN LEFT(h.q4_spec, LENGTH(h.q4_spec)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN h.q4_spec) > 0
THEN RIGHT(h.q4_spec, 3)
END AS IUCN_redlist,

'' AS local_name_species
FROM akvo_tree_monitoring_pcq h
WHERE NOT h.q4_spec = ''
AND NOT h.q4_spec = 'other'

UNION ALL

SELECT
j.gps_corner_1 AS centroid_coord,
i.identifier_akvo,
'tree registration LIGHT VERSION' AS source_species,

CASE
WHEN POSITION('|' IN i.lat_name_species) > 0
THEN SUBSTRING(i.lat_name_species FROM POSITION('|' IN i.lat_name_species)+1 FOR
LENGTH(i.lat_name_species) - POSITION(':' IN reverse(i.lat_name_species))- POSITION('|' IN i.lat_name_species))
END AS lat_name_species,

'nothing yet' as IUCN_redlist,

CASE
WHEN POSITION('(' IN i.lat_name_species) > 0
THEN reverse(SUBSTRING(reverse(i.lat_name_species) FROM POSITION(')' IN reverse(i.lat_name_species))+1 FOR
POSITION('(' IN reverse(i.lat_name_species))- POSITION(')' IN reverse(i.lat_name_species))-1))
END AS local_name_species

FROM akvo_tree_registration_species_light_version i
JOIN akvo_tree_registration_locations_light_version j
ON i.identifier_akvo = j.identifier_akvo

UNION ALL

SELECT
l.centroid_coord,
l.identifier_akvo,
'tree registration DISTRIBUTED TREES' AS source_species,
--k.species_lat AS lat_name_species,

CASE
WHEN POSITION('_' IN k.species_lat) > 0
THEN LEFT(k.species_lat, LENGTH(k.species_lat)-4)
END AS lat_name_species,

CASE
WHEN POSITION('_' IN k.species_lat) > 0
THEN RIGHT(k.species_lat, 3)
END AS IUCN_redlist,

k.species_local AS local_name_species
FROM akvo_site_registration_distributed_trees_species k
JOIN akvo_site_registration_distributed_trees l
ON k.identifier_akvo = l.identifier_akvo;

ALTER TABLE superset_ecosia_global_tree_species_distribution
ADD COLUMN geojson TEXT;

ALTER TABLE superset_ecosia_global_tree_species_distribution
ADD COLUMN id SERIAL PRIMARY KEY;

WITH geojson_table AS (SELECT
superset_ecosia_global_tree_species_distribution.id,
jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'tree species location',
        'geometry',   ST_AsGeoJSON(superset_ecosia_global_tree_species_distribution.centroid_coord)::json)))::text
					   AS geojson
FROM superset_ecosia_global_tree_species_distribution
GROUP BY superset_ecosia_global_tree_species_distribution.id)

UPDATE superset_ecosia_global_tree_species_distribution
SET geojson = geojson_table.geojson
FROM geojson_table
WHERE superset_ecosia_global_tree_species_distribution.id = geojson_table.id;''')

conn.commit()


# Here we create a seperate table for KANOP analysis. We first going to detect where self-intersections are located
# Then we are going to correct the self-intersected polygons with an ST-buffer(0.0). In the geometric error detection script
# Then we run the overlap analysis again, but with the corrected self-intersected polygons. In the geometric error detection script
# With this, we can filter out ALL overlapping polygons, including the ones that have a self-intersection (and have an overlap)
create_a51 = ('''
CREATE TABLE akvo_tree_registration_areas_updated_KANOP
AS (SELECT * FROM akvo_tree_registration_areas_updated);

ALTER TABLE akvo_tree_registration_areas_updated_KANOP
DROP COLUMN self_intersection,
DROP COLUMN overlap,
DROP COLUMN needle_shape,
DROP COLUMN check_duplicate_polygons;

ALTER TABLE akvo_tree_registration_areas_updated_KANOP
ADD polygon_corr_self_intersections geography (polygon, 4326),
ADD self_intersection_before_corr_pol BOOLEAN,
ADD self_intersection_after_corr_pol BOOLEAN,
ADD overlap_before_self_intersection_corrections BOOLEAN,
ADD overlap_after_self_intersection_corrections BOOLEAN,
ADD needle_shape_before_self_interection_corrections BOOLEAN,
ADD needle_shape_after_self_interection_corrections BOOLEAN,
ADD check_duplicate_polygons_after_self_corrections TEXT,
ADD check_duplicate_polygons_before_self_corrections TEXT;''')

conn.commit()


create_a20_ecosia_superset = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ecosia_superset;

--GRANT ALL ON SCHEMA conservation TO anne;
--GRANT SELECT ON ALL TABLES IN SCHEMA conservation TO anne;
--GRANT ALL PRIVILEGES ON conservation.conservation_areas TO anne;
--GRANT UPDATE ON conservation.conservation_areas_gid_seq TO anne;


GRANT USAGE ON SCHEMA PUBLIC TO ecosia_superset;
GRANT USAGE ON SCHEMA HEROKU_EXT TO ecosia_superset;

GRANT SELECT ON TABLE superset_ecosia_nursery_registration TO ecosia_superset;
GRANT UPDATE ON TABLE superset_ecosia_nursery_registration TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_monitoring TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_s4g_site_health TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_monitoring TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_monitoring_species TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_registration_photos TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_nursery_monitoring_photos TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration_photos TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration_species TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_s4g_fires TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_s4g_deforestation TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_geolocations TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_registration_light TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_monitoring_photos TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_tree_distribution_unregistered_farmers TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_contract_overview TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_new_devices TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_firms_historic_fires TO ecosia_superset;

DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_registration;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_monitoring;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_s4g_site_health;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_monitoring;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_monitoring_species;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_registration_photos;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_nursery_monitoring_photos;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration_photos;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration_species;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_s4g_fires;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_s4g_deforestation;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_geolocations;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_registration_light;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_monitoring_photos;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_tree_distribution_unregistered_farmers;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_contract_overview;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_new_devices;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_firms_historic_fires;

ALTER TABLE superset_ecosia_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_s4g_site_health enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_monitoring_species enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_registration_photos enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_nursery_monitoring_photos enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration_photos enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration_species enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_s4g_fires enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_s4g_deforestation enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_geolocations enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_registration_light enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_monitoring_photos enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_tree_distribution_unregistered_farmers enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_contract_overview enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_new_devices enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_firms_historic_fires enable ROW LEVEL SECURITY;

CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_registration TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_monitoring TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_s4g_site_health TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_monitoring TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_monitoring_species TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_registration_photos TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_nursery_monitoring_photos TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration_photos TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration_species TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_s4g_fires TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_s4g_deforestation TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_geolocations TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_registration_light TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_monitoring_photos TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_tree_distribution_unregistered_farmers TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_contract_overview TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_new_devices TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_firms_historic_fires TO ecosia_superset USING (true);'''

conn.commit()


# Execute drop tables
cur.execute(drop_tables)

conn.commit()

# Execute create tables
cur.execute(create_a1)
cur.execute(create_a2)
cur.execute(create_a3)
cur.execute(create_a6)
cur.execute(create_a14)
cur.execute(create_a15)
cur.execute(create_a16)
cur.execute(create_a17)
cur.execute(create_a31)
cur.execute(create_a32)
cur.execute(create_a33)
cur.execute(create_a36)
cur.execute(create_a37)
cur.execute(create_a38)
cur.execute(create_a39)
cur.execute(create_a40)
cur.execute(create_a41)
cur.execute(create_a44)
cur.execute(create_a45)
cur.execute(create_a46)
cur.execute(create_a47)
cur.execute(create_a49)
cur.execute(create_a51)

cur.execute(create_a20_ecosia_superset)

conn.commit()

# cur.execute('''ALTER TABLE CALC_GEOM_locations_registration_versus_externalaudits ADD COLUMN distance_to_registration_m INTEGER;''')
# conn.commit()
#
# cur.execute('''SELECT * FROM CALC_GEOM_locations_registration_versus_externalaudits;''')
# conn.commit()

# rows = cur.fetchall()
#
# for row in rows:
#     instance = row[2]
#     location_registration = (row[6:8])
#     location_audit = (row[8:10])
#     distance_m = (geodesic(location_registration, location_audit).m)
#
#     cur.execute('''UPDATE CALC_GEOM_locations_registration_versus_externalaudits SET distance_to_registration_m = %s WHERE instance = %s;''', (distance_m, instance))
#     conn.commit()

cur.close()

print('all queries are processed')
