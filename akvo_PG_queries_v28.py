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
DROP TABLE IF EXISTS CALC_TAB_Error_check_on_site_registration;
DROP TABLE IF EXISTS CALC_TAB_linear_regression_results;
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
DROP TABLE IF EXISTS superset_ecosia_nursery_registration_photos;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_pictures;
DROP TABLE IF EXISTS superset_ecosia_nursery_monitoring_photos;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_pictures;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_photos;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_species;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_species;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_pictures;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_photos;
DROP TABLE IF EXISTS superset_ecosia_s4g_fires;
DROP TABLE IF EXISTS superset_ecosia_s4g_deforestation;
DROP TABLE IF EXISTS superset_ecosia_tree_registration;
DROP TABLE IF EXISTS superset_ecosia_geolocations;
DROP TABLE IF EXISTS superset_ecosia_tree_monitoring_photos;
DROP TABLE IF EXISTS superset_ecosia_tree_registration_light;
DROP TABLE IF EXISTS superset_ecosia_tree_distribution_unregistered_farmers;
DROP TABLE IF EXISTS superset_ecosia_site_registration_unregistered_farmers;
DROP TABLE IF EXISTS superset_ecosia_contract_overview;'''
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
AND akvo_tree_monitoring_remapped_areas.polygon_remapped NOTNULL;

UPDATE akvo_tree_registration_areas_updated
SET
calc_area = akvo_tree_monitoring_remapped_areas.calc_area_remapped
FROM akvo_tree_monitoring_remapped_areas
WHERE akvo_tree_registration_areas_updated.identifier_akvo
= akvo_tree_monitoring_remapped_areas.identifier_akvo
AND akvo_tree_monitoring_remapped_areas.polygon_remapped NOTNULL;

UPDATE akvo_tree_registration_areas_updated
SET
number_coord_polygon = akvo_tree_monitoring_remapped_areas.number_coord_polygon_remapped
FROM akvo_tree_monitoring_remapped_areas
WHERE akvo_tree_registration_areas_updated.identifier_akvo
= akvo_tree_monitoring_remapped_areas.identifier_akvo
AND akvo_tree_monitoring_remapped_areas.polygon_remapped NOTNULL;

UPDATE akvo_tree_registration_areas_updated
SET calc_area = 0.2
WHERE polygon ISNULL;'''

conn.commit()


# Works well
create_a2 = '''CREATE TABLE CALC_TAB_monitoring_calculations_per_site AS

--- Plot dates of all MAIN TAB monitorings (COUNTS AND PCQ's)
WITH plot_dates_monitorings AS (SELECT
akvo_tree_monitoring_areas.identifier_akvo,
akvo_tree_monitoring_areas.instance,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_monitoring_areas.submission AS monitoring_submission,

-- Classify the options of method selection (as in the audit table below) is NOT needed here because
-- there is only one option to select PCQ or Tree count rows. No grouping problem here(?)
AKVO_Tree_monitoring_areas.method_selection,

'monitoring_data' AS procedure,
TO_DATE(akvo_tree_registration_areas_updated.planting_date, 'YYYY-MM-DD') AS planting_date
FROM akvo_tree_monitoring_areas
LEFT JOIN akvo_tree_registration_areas_updated
ON akvo_tree_monitoring_areas.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo),

-- Plot dates of all MAIN TAB audits (COUNTS AND PCQ's)
plot_dates_audits AS (SELECT
akvo_tree_external_audits_areas.identifier_akvo,
akvo_tree_external_audits_areas.instance,
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

-- Calculate time differences between planting date and audits/monitorings and give them a label strata (instance level)
table_label_strata AS (SELECT
combine_monitorings_audits.identifier_akvo,
combine_monitorings_audits.instance,
combine_monitorings_audits.method_selection,
combine_monitorings_audits.procedure,
combine_monitorings_audits.planting_date,
combine_monitorings_audits.monitoring_submission AS submission,
combine_monitorings_audits.monitoring_submission - planting_date AS difference_days_reg_monitoring,
CAST((combine_monitorings_audits.monitoring_submission - planting_date)*1.0/365 * 1.0 AS DECIMAL(7,1)) AS difference_years_reg_monitoring,
CEILING((combine_monitorings_audits.monitoring_submission - planting_date)*1.0/180)*180 AS label_strata
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

--List the enumerators. Some monitoring submissions were done at the same time by different submittors.
--We need to bundle them into 1 column so they don't appear as seperate submissions in the result table
submittors_monitoring AS (SELECT identifier_akvo, STRING_AGG(akvo_tree_monitoring_areas.submitter,' | ') AS submitter
FROM akvo_tree_monitoring_areas
GROUP BY identifier_akvo),

--List the enumerators. Some monitoring submissions were done at the same time by different submittors.
--We need to bundle them into 1 column so they don't appear as seperate submissions in the result table
submittors_audit AS (SELECT identifier_akvo, STRING_AGG(akvo_tree_external_audits_areas.submitter,' | ') AS submitter
FROM akvo_tree_external_audits_areas
GROUP BY identifier_akvo),

-- Sub CTE table to calculate PCQ MONITORING results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_pcq_monitoring AS (
SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.display_name,
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
THEN ROUND(((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * Akvo_tree_registration_areas_updated.calc_area)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,0)
ELSE ROUND(((SUM(AKVO_Tree_monitoring_areas.number_living_trees)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0))*100),0)
END AS perc_trees_survived,

CASE
WHEN AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
THEN ROUND(pcq_results_merged_monitoring_hgt,2)
ElSE AVG(akvo_tree_monitoring_areas.avg_tree_height)
END AS avg_tree_height

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

where AKVO_Tree_monitoring_areas.method_selection = 'Number of living trees is unknown. Go to PCQ method.'
AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
akvo_tree_registration_areas_updated.organisation,
submittors_monitoring.submitter,
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
-- a complex issue of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_pcq_audit AS (SELECT
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.display_name,
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
END AS avg_tree_height

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

WHERE table_label_strata.method_selection = 'PCQ' AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_external_audits_areas.identifier_akvo,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
count_pcq_samples.number_pcq_samples,
Akvo_tree_registration_areas_updated.planting_date,
table_label_strata.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_audit.submitter,
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
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_monitoring.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Monitoring' AS procedure,
'Tree count' AS data_collection_method,

0 AS avg_monitored_tree_distance,
0 AS avg_audit_tree_density,

CASE
WHEN
--AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
SUM(AKVO_Tree_monitoring_counts.number_species) NOTNULL
THEN ROUND(SUM(AKVO_Tree_monitoring_counts.number_species),0)
ELSE ROUND(AKVO_Tree_monitoring_areas.number_living_trees,0)
END AS nr_trees_monitored,

0 AS nr_samples_pcq_monitoring,
Akvo_tree_registration_areas_updated.planting_date AS planting_date,
MAX(table_label_strata.submission) AS latest_monitoring_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

CASE
WHEN
--AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
SUM(AKVO_Tree_monitoring_counts.number_species) NOTNULL
THEN ROUND(SUM(AKVO_Tree_monitoring_counts.number_species*1.0)/NULLIF(akvo_tree_registration_areas_updated.tree_number*1.0,0)*100,0)
ELSE ROUND(SUM(AKVO_Tree_monitoring_areas.number_living_trees)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,0)
END AS perc_trees_survived,

ROUND(AVG(akvo_tree_monitoring_areas.avg_tree_height)::numeric,2) AS avg_tree_height

FROM AKVO_Tree_monitoring_areas
LEFT JOIN AKVO_Tree_monitoring_counts
ON AKVO_Tree_monitoring_areas.instance = AKVO_Tree_monitoring_counts.instance
LEFT JOIN Akvo_tree_registration_areas_updated
ON AKVO_Tree_monitoring_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON AKVO_Tree_monitoring_areas.instance = table_label_strata.instance
LEFT JOIN submittors_monitoring
ON submittors_monitoring.identifier_akvo = AKVO_Tree_monitoring_areas.identifier_akvo

WHERE AKVO_Tree_monitoring_areas.method_selection = 'The trees were counted'
AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
AKVO_Tree_monitoring_areas.identifier_akvo,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
Akvo_tree_registration_areas_updated.planting_date,
AKVO_Tree_monitoring_areas.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_monitoring.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
AKVO_Tree_monitoring_areas.display_name,
AKVO_Tree_monitoring_areas.number_living_trees),


-- Sub CTE table to calculate COUNTS of AUDIT results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_audit AS (SELECT
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
LOWER(akvo_tree_registration_areas_updated.organisation) AS organisation,
submittors_audit.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number AS registered_tree_number,
'Audit' AS procedure,
'Tree count' AS data_collection_method,

0 AS avg_audit_tree_distance,
0 AS avg_audit_tree_density,

CASE
WHEN table_label_strata.method_selection = 'Tree count' AND SUM(AKVO_Tree_external_audits_counts.number_species) NOTNULL
THEN ROUND(SUM(AKVO_Tree_external_audits_counts.number_species),0)
ELSE ROUND(AKVO_Tree_external_audits_areas.audit_reported_trees,0)
END AS nr_trees_monitored,

0 AS nr_samples_pcq_audit,
Akvo_tree_registration_areas_updated.planting_date AS planting_date,
MAX(table_label_strata.submission) AS latest_audit_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_audit,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_audit,
table_label_strata.label_strata,

CASE
WHEN table_label_strata.method_selection = 'Tree count' AND SUM(AKVO_Tree_external_audits_counts.number_species) NOTNULL
THEN ROUND(SUM(AKVO_Tree_external_audits_counts.number_species*1.0)/NULLIF(akvo_tree_registration_areas_updated.tree_number*1.0,0)*100,0)
ELSE ROUND(SUM(AKVO_Tree_external_audits_areas.audit_reported_trees)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,0)
END AS perc_trees_survived,

ROUND(AVG(akvo_tree_external_audits_areas.audit_reported_tree_height),2) AS avg_tree_height_m

FROM akvo_tree_external_audits_areas
LEFT JOIN AKVO_Tree_external_audits_counts
ON akvo_tree_external_audits_areas.instance = AKVO_Tree_external_audits_counts.instance
LEFT JOIN Akvo_tree_registration_areas_updated
ON akvo_tree_external_audits_areas.identifier_akvo = Akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON akvo_tree_external_audits_areas.instance = table_label_strata.instance
LEFT JOIN submittors_audit
ON submittors_audit.identifier_akvo = akvo_tree_external_audits_areas.identifier_akvo

WHERE table_label_strata.method_selection = 'Tree count' AND Akvo_tree_registration_areas_updated.identifier_akvo NOTNULL

GROUP BY
table_label_strata.label_strata,
akvo_tree_external_audits_areas.identifier_akvo,
Akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
Akvo_tree_registration_areas_updated.planting_date,
table_label_strata.method_selection,
akvo_tree_registration_areas_updated.organisation,
submittors_audit.submitter,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.country,
AKVO_Tree_external_audits_areas.display_name,
AKVO_Tree_external_audits_areas.audit_reported_trees),

-- Add the POLYGON results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial tree number). Only for polygons
registration_results_polygon AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
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
0 AS "Average tree height (m)"

FROM akvo_tree_registration_areas_updated
WHERE polygon NOTNULL),


-- Add the NON-polygon results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial tree number). Only for NON-polygons
registration_results_non_polygon AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.display_name,
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
0 AS "Average tree height (m)"

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
WHEN photo_owner LIKE '%/%'
THEN RIGHT(photo_owner, strpos(reverse(photo_owner),'/')-1)
ELSE photo_owner
END;

UPDATE akvo_tree_registration_areas_updated
SET photo_owner = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_owner)
WHERE photo_owner NOTNULL;

UPDATE akvo_tree_registration_photos
SET photo_url = CASE
WHEN photo_url LIKE '%/%'
THEN RIGHT(photo_url, strpos(reverse(photo_url),'/')-1)
ELSE photo_url
END;

UPDATE akvo_tree_registration_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_url);

UPDATE AKVO_Tree_external_audits_photos
SET url_photo = CASE
WHEN url_photo LIKE '%/%'
THEN RIGHT(url_photo, strpos(reverse(url_photo),'/')-1)
ELSE url_photo
END;

UPDATE AKVO_Tree_external_audits_photos
SET url_photo = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',url_photo);

UPDATE akvo_tree_monitoring_photos
SET photo_url = CASE
WHEN photo_url LIKE '%/%'
THEN RIGHT(photo_url, strpos(reverse(photo_url),'/')-1)
ELSE photo_url
END;

UPDATE akvo_tree_monitoring_photos
SET photo_url = CONCAT('https://akvoflow-201.s3.amazonaws.com/images/',photo_url);'''

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
CREATE TABLE CALC_TAB_linear_regression_results AS WITH linear_regression_field_data AS (SELECT
h.identifier_akvo,
h.id_planting_site,
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
table_e.avg_tree_height
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
COUNT(calc_tab_monitoring_calculations_per_site.label_strata) AS "number of label_strata (monitoring periods)",
linear_regression_field_data.maximum_label_strata AS "latest monitoring (max label_strata)",
linear_regression_field_data.perc_trees_survived AS "latest monitored tree number (at max label_strata)",
linear_regression_field_data.avg_tree_height AS "latest monitored tree height (at max label_strata)",

ROUND(linear_regression_field_data.slope_survival_perc,6) AS site_linear_regression_slope_survival_perc,
ROUND(linear_regression_field_data.intercept_survival_perc,2) AS site_intercept_value_linear_regression_survival_perc,
ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) AS site_estimated_survival_perc_linear_regression_t3,
ROUND(linear_regression_field_data.slope_avg_tree_height::DECIMAL,6) AS site_linear_regression_slope_avg_tree_height,
ROUND(linear_regression_field_data.intercept_avg_tree_height::DECIMAL,2) AS site_intercept_value_linear_regression_avg_tree_height,
ROUND(((1080 * linear_regression_field_data.slope_avg_tree_height) + linear_regression_field_data.intercept_avg_tree_height)::DECIMAL,2) AS site_estimated_avg_tree_height_linear_regression_t3,

CASE
WHEN ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) > 100
AND ROUND(((1080 * linear_regression_field_data.slope_avg_tree_height) + linear_regression_field_data.intercept_avg_tree_height)::DECIMAL,2) > 2
AND linear_regression_field_data.slope_avg_tree_height > 0
THEN 'tree development very likely positive (survival% >100%, tree height > 2m and showing positive trend at t=3)'


WHEN ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) BETWEEN 100 AND 80
AND ROUND(((1080 * linear_regression_field_data.slope_avg_tree_height) + linear_regression_field_data.intercept_avg_tree_height)::DECIMAL,2) > 1
AND linear_regression_field_data.slope_avg_tree_height > 0
THEN 'tree development likely positive (survival between 100% and 80%, tree height > 1m or showing positive trend at t=3)'

WHEN ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) BETWEEN 79.99 AND 60
THEN 'tree development unsure (survival between 80% and 60%, no height criteria at t=3)'

WHEN ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) BETWEEN 59.9 AND 25
AND linear_regression_field_data.slope_avg_tree_height <= 0
THEN 'tree development likely negative (survival between 60% and 25% and negative height trend at t=3)'

WHEN ROUND(((1080 * linear_regression_field_data.slope_survival_perc) + linear_regression_field_data.intercept_survival_perc),2) BETWEEN 24.9 AND 0
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
linear_regression_field_data.maximum_label_strata,
linear_regression_field_data.slope_survival_perc,
linear_regression_field_data.intercept_survival_perc,
site_estimated_survival_perc_linear_regression_t3,
site_linear_regression_slope_avg_tree_height,
site_intercept_value_linear_regression_avg_tree_height,
site_estimated_avg_tree_height_linear_regression_t3,
linear_regression_field_data.slope_avg_tree_height,
linear_regression_field_data.perc_trees_survived,
linear_regression_field_data.avg_tree_height;'''

conn.commit()


create_a7 = '''CREATE TABLE CALC_GEOM_Trees_counted_per_site_by_external_audit
AS SELECT AKVO_Tree_registration_areas.centroid_coord,
AKVO_Tree_registration_areas.identifier_akvo,
LOWER(AKVO_Tree_registration_areas.organisation) AS "Name organisation",
AKVO_Tree_registration_areas.submitter AS "Name submitter registration data",
AKVO_Tree_external_audits_areas.submitter AS "Name auditor",
AKVO_Tree_registration_areas.id_planting_site AS "ID planting site",
AKVO_Tree_registration_areas.contract_number AS "Contract number",
AKVO_Tree_registration_areas.tree_number AS "Nr. trees registered",
AKVO_Tree_external_audits_areas.audit_reported_trees AS "Nr. trees counted by auditor"
FROM AKVO_Tree_registration_areas JOIN AKVO_Tree_external_audits_areas
ON AKVO_Tree_registration_areas.identifier_akvo = AKVO_Tree_external_audits_areas.identifier_akvo
WHERE AKVO_Tree_external_audits_areas.audit_reported_trees NOTNULL
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
ROUND(SUM(perc_trees_survived * registered_tree_number)/SUM(registered_tree_number),1) AS weighted_avg_perc_tree_survival_t1,
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
label_strata > 0 AND label_strata <= 360
GROUP BY identifier_akvo) table_c
ON table_a.identifier_akvo = table_c.identifier_akvo
AND table_a.label_strata = table_c.max_label_strata_t1
AND table_a.rank_monitoring_audit_method = table_c.max_method_ranking_t1
GROUP BY table_a.contract_number, table_b.total_nr_trees_registered_per_contract,
table_b.total_nr_planting_sites_per_contract),

-- This table calculates results on contract for T=2
CTE_contract_level_monitoring_audit_results_t2 AS (SELECT
table_a.contract_number,
ROUND(SUM(perc_trees_survived * registered_tree_number)/SUM(registered_tree_number),1) AS weighted_avg_perc_tree_survival_t2,
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
label_strata > 360 AND label_strata <= 720
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
label_strata > 720
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

from akvo_nursery_registration
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
akvo_nursery_registration.nursery_type,
akvo_nursery_registration.nursery_name,
akvo_nursery_registration.newly_established,
akvo_nursery_registration.full_tree_capacity,
akvo_nursery_registration.lat_y,
akvo_nursery_registration.lon_x,
CALC_TAB_Error_partner_report_on_nursery_registration."species currently produced in nursery",
CALC_TAB_Error_partner_report_on_nursery_registration."nr of photos taken during registration",
CALC_TAB_Error_partner_report_on_nursery_registration."Check nr of photos taken during registration of the nursery"

FROM akvo_nursery_registration
LEFT JOIN CALC_TAB_Error_partner_report_on_nursery_registration
ON CALC_TAB_Error_partner_report_on_nursery_registration.identifier_akvo = akvo_nursery_registration.identifier_akvo;

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

SELECT identifier_akvo FROM AKVO_Tree_registration_photos
UNION ALL
SELECT identifier_akvo FROM AKVO_Tree_monitoring_photos
UNION ALL
SELECT identifier_akvo FROM AKVO_Tree_external_audits_photos
UNION ALL
SELECT identifier_akvo FROM AKVO_tree_registration_areas_updated
WHERE AKVO_tree_registration_areas_updated.photo_owner NOTNULL),

count_total_number_photos_per_site AS (SELECT identifier_akvo, COUNT(identifier_akvo) AS total_nr_photos
FROM COUNT_Total_number_of_photos_taken
GROUP BY identifier_akvo),

count_number_tree_species_registered AS (SELECT identifier_akvo, COUNT(*) AS nr_species_registered
FROM AKVO_Tree_registration_species
GROUP BY AKVO_Tree_registration_species.identifier_akvo)


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
t.contract_number,
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
t.lat_y,
t.lon_x,
t.number_coord_polygon AS nr_points_in_polygon,
--s4g_ecosia_data_quality.nr_photos_taken AS "number of tree photos taken",

CASE
WHEN count_total_number_photos_per_site.total_nr_photos NOTNULL
THEN count_total_number_photos_per_site.total_nr_photos
WHEN count_total_number_photos_per_site.total_nr_photos ISNULL
THEN 0
END AS "number of tree photos taken",

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
count_number_tree_species_registered.nr_species_registered,
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
ON t.identifier_akvo = S4G_API_health_indicators.identifier_akvo
LEFT JOIN count_total_number_photos_per_site
ON count_total_number_photos_per_site.identifier_akvo = t.identifier_akvo
LEFT JOIN count_number_tree_species_registered
ON count_number_tree_species_registered.identifier_akvo = t.identifier_akvo;

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

CALC_TAB_monitoring_calculations_per_site.submitter,
CALC_TAB_monitoring_calculations_per_site.contract_number,
CALC_TAB_monitoring_calculations_per_site.id_planting_site,
CALC_TAB_monitoring_calculations_per_site.calc_area,
CALC_TAB_monitoring_calculations_per_site.registered_tree_number,
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
CALC_TAB_monitoring_calculations_per_site.avg_tree_height

FROM CALC_TAB_monitoring_calculations_per_site
WHERE CALC_TAB_monitoring_calculations_per_site.organisation NOTNULL;'''

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
--akvo_nursery_registration.identifier_akvo,
--akvo_nursery_registration.instance,
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
akvo_tree_registration_areas_updated.contract_number,
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
akvo_tree_registration_areas_updated.contract_number,
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
AND NOT akvo_tree_registration_areas_updated.photo_owner ~ '^\s*$'; -- This last row is needed as not all empty rows are captured by the NOTNULL (strange)

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
WHERE photo_url NOTNULL;'''

conn.commit()


create_a41 = '''CREATE TABLE superset_ecosia_tree_registration_species
AS SELECT
akvo_tree_registration_areas.display_name,
LOWER(akvo_tree_registration_areas.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_areas.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_areas.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas.organisation)),
			LENGTH(akvo_tree_registration_areas.organisation) - POSITION('-' IN akvo_tree_registration_areas.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas.organisation)),
			LENGTH(akvo_tree_registration_areas.organisation) - POSITION('-' IN akvo_tree_registration_areas.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_areas.organisation)),
			LENGTH(akvo_tree_registration_areas.organisation) - POSITION('-' IN akvo_tree_registration_areas.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,


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
akvo_tree_registration_areas_updated.contract_number,
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
akvo_tree_registration_areas_updated.contract_number,
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
t.contract_number,
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
GROUP BY t.identifier_akvo, t.organisation, t.contract_number, t.display_name),

-- Here we convert the centroid-point locations from WKT format to geojson string format that can be read by superset
buffer_around_200_trees_centroids AS (SELECT
identifier_akvo,

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
t.contract_number,
t.display_name,
'tree registration' AS procedure,
ST_Buffer(t.centroid_coord,25) as buffer
FROM akvo_tree_registration_areas_updated AS t
WHERE t.polygon ISNULL),

-- Here we convert the buffer polygon areas (WKT format) to geojson string format that can be read by superset
wkt_buffer_200_trees_areas_to_geojson AS (
SELECT
t.identifier_akvo,

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
t.contract_number,
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
group by t.identifier_akvo, t.organisation, t.contract_number, t.display_name),

-- Here we convert the PCQ MONITORING sample point locations from WKT format to geojson string format that can be read by superset
wkt_pcq_samples_monitoring_to_geojson AS
(SELECT
pcq_samples_monitorings.identifier_akvo,

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
akvo_tree_registration_areas_updated.contract_number,
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
GROUP BY pcq_samples_monitorings.identifier_akvo,
akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name),

-- Here we convert the photo (GEOTAG) locations (TREE REGISTRATION) from WKT format to geojson string format that can be read by superset
wkt_photo_registration_geotag_to_geojson AS
(SELECT
tree_registration_photos_geotag.identifier_akvo,

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
akvo_tree_registration_areas_updated.contract_number,
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
WHERE tree_registration_photos_geotag.photo_geotag_location NOTNULL
GROUP BY tree_registration_photos_geotag.identifier_akvo,
akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name),

-- Here we convert the photo (GPS) locations (TREE REGISTRATION) from WKT format to geojson string format that can be read by superset
wkt_photo_registration_gps_to_geojson AS
(SELECT
tree_registration_photos_gps.identifier_akvo,

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
akvo_tree_registration_areas_updated.contract_number,
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
WHERE tree_registration_photos_gps.photo_geotag_location ISNULL
GROUP BY tree_registration_photos_gps.identifier_akvo,
akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name),

-- Here we convert the PCQ sample point AUDIT locations from WKT format to geojson string format that can be read by superset
wkt_pcq_samples_audit_to_geojson AS
(SELECT
pcq_samples_audits.identifier_akvo,

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
akvo_tree_registration_areas_updated.contract_number,
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
GROUP BY pcq_samples_audits.identifier_akvo,
akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name),


-- Here we convert the COUNT sample MONITORING locations from WKT format to geojson string format that can be read by superset
wkt_count_samples_monitoring_to_geojson AS
(SELECT
count_samples_monitoring.identifier_akvo,

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
akvo_tree_registration_areas_updated.contract_number,
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
akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name),


-- Here we convert the COUNT sample AUDIT locations from WKT format to geojson string format that can be read by superset
wkt_count_samples_audit_to_geojson AS
(SELECT
count_samples_audit.identifier_akvo,

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
akvo_tree_registration_areas_updated.contract_number,
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
GROUP BY count_samples_audit.identifier_akvo,
akvo_tree_registration_areas_updated.organisation, akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.display_name)


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
SELECT * FROM wkt_count_samples_audit_to_geojson;'''

#wkt_pcq_photo_monitoring_to_geojson
#wkt_pcq_photo_audit_to_geojson


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
akvo_tree_registration_areas_updated.contract_number,
'Monitoring' as procedure,
akvo_tree_monitoring_photos.identifier_akvo,
akvo_tree_monitoring_photos.instance,
akvo_tree_monitoring_photos.photo_url,
akvo_tree_monitoring_photos.photo_location

FROM akvo_tree_monitoring_photos
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_monitoring_photos.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo

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
akvo_tree_registration_areas_updated.contract_number,
'Audit' as procedure,

AKVO_Tree_external_audits_photos.identifier_akvo,
AKVO_Tree_external_audits_photos.instance,
AKVO_Tree_external_audits_photos.url_photo AS photo_url,
AKVO_Tree_external_audits_photos.location_photo AS photo_location

FROM AKVO_Tree_external_audits_photos
JOIN akvo_tree_registration_areas_updated
ON AKVO_Tree_external_audits_photos.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo;

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
WHERE photo_url NOTNULL;'''

conn.commit()

create_a46 = '''CREATE TABLE superset_ecosia_tree_registration_light
AS SELECT

akvo_tree_registration_locations_light_version.identifier_akvo,
akvo_tree_registration_locations_light_version.display_name,
akvo_tree_registration_locations_light_version.device_id,
akvo_tree_registration_locations_light_version.instance,
akvo_tree_registration_locations_light_version.submission,
akvo_tree_registration_locations_light_version.akvo_form_version,
LOWER(akvo_tree_registration_locations_light_version.country) AS country,
akvo_tree_registration_locations_light_version.test,

-- Create a unique code for filtering in superset, based on main organisation name
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_locations_light_version.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_tree_registration_locations_light_version.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_tree_registration_locations_light_version.organisation),3)),4))) AS NUMERIC) AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN akvo_tree_registration_locations_light_version.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_locations_light_version.organisation)),
			LENGTH(akvo_tree_registration_locations_light_version.organisation) - POSITION('-' IN akvo_tree_registration_locations_light_version.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_locations_light_version.organisation)),
			LENGTH(akvo_tree_registration_locations_light_version.organisation) - POSITION('-' IN akvo_tree_registration_locations_light_version.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_tree_registration_locations_light_version.organisation)),
			LENGTH(akvo_tree_registration_locations_light_version.organisation) - POSITION('-' IN akvo_tree_registration_locations_light_version.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(akvo_tree_registration_locations_light_version.organisation) AS organisation,
akvo_tree_registration_locations_light_version.contract_number,
akvo_tree_registration_locations_light_version.id_planting_site,
akvo_tree_registration_locations_light_version.name_village,
akvo_tree_registration_locations_light_version.name_owner,
akvo_tree_registration_locations_light_version.remark,
TO_DATE(akvo_tree_registration_locations_light_version.planting_date, 'YYYY-MM-DD') AS planting_date,
akvo_tree_registration_locations_light_version.tree_number,
akvo_tree_registration_locations_light_version.planting_distance,
akvo_tree_registration_locations_light_version.only_location,
akvo_tree_registration_locations_light_version.gps_corner_1,
akvo_tree_registration_locations_light_version.gps_corner_2,
akvo_tree_registration_locations_light_version.gps_corner_3,
akvo_tree_registration_locations_light_version.gps_corner_4

FROM akvo_tree_registration_locations_light_version
WHERE akvo_tree_registration_locations_light_version.organisation NOTNULL;

ALTER TABLE superset_ecosia_tree_registration_light
ADD lat_y REAL;

ALTER TABLE superset_ecosia_tree_registration_light
ADD lon_x REAL;

UPDATE superset_ecosia_tree_registration_light
SET
lat_y = ST_Y(gps_corner_1::geometry),
lon_x = ST_X(gps_corner_1::geometry);

UPDATE superset_ecosia_tree_registration_light
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_tree_registration_light
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = '';'''

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
AKVO_tree_distribution_unregistered_farmers.contract_number,
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
WHERE test = ''
OR test = 'This is real, valid data';'''

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
AKVO_tree_distribution_unregistered_farmers.contract_number,
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
WHERE test = ''
OR test = 'This is real, valid data';'''

conn.commit()

create_a49 = '''CREATE TABLE superset_ecosia_contract_overview
AS SELECT

CALC_TAB_tree_submissions_per_contract.name_country,
CALC_TAB_tree_submissions_per_contract.organisation,

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

CALC_TAB_tree_submissions_per_contract."Contract number" AS contract_number,
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

FROM CALC_TAB_tree_submissions_per_contract;'''

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

CREATE POLICY mkec_policy ON akvo_ecosia_tree_area_registration TO kenya_mkec USING (organisation = 'mount kenya environmental conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_tree_area_monitoring TO kenya_mkec USING (EXISTS (SELECT * FROM akvo_ecosia_tree_area_registration
WHERE akvo_ecosia_tree_area_registration.organisation = 'mount kenya environmental conservation'
AND akvo_ecosia_tree_area_monitoring.identifier_akvo = akvo_ecosia_tree_area_registration.identifier_akvo));
CREATE POLICY mkec_policy ON akvo_ecosia_nursery_registration TO kenya_mkec USING (organisation = 'mount kenya environmental conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_nursery_monitoring TO kenya_mkec USING (EXISTS (SELECT * FROM akvo_ecosia_nursery_registration
WHERE akvo_ecosia_nursery_registration.organisation = 'mount kenya environmental conservation'
AND akvo_ecosia_nursery_monitoring.identifier_akvo = akvo_ecosia_nursery_registration.identifier_akvo));
CREATE POLICY mkec_policy ON error_partner_report_on_site_registration TO kenya_mkec USING (error_partner_report_on_site_registration.organisation = 'mount kenya environmental conservation');
CREATE POLICY mkec_policy ON error_partner_report_on_nursery_registration TO kenya_mkec USING (error_partner_report_on_nursery_registration.organisation = 'mount kenya environmental conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_contract_overview TO kenya_mkec USING (akvo_ecosia_contract_overview.organisation = 'mount kenya environmental conservation');
CREATE POLICY mkec_policy ON akvo_ecosia_tree_photo_registration TO kenya_mkec USING (akvo_ecosia_tree_photo_registration.organisation = 'mount kenya environmental conservation');'''


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

CREATE POLICY fdia_policy ON akvo_ecosia_tree_area_registration TO nicaragua_fdia USING (organisation = 'fundacion dia');
CREATE POLICY fdia_policy ON akvo_ecosia_tree_area_monitoring TO nicaragua_fdia USING (EXISTS (SELECT * FROM akvo_ecosia_tree_area_registration
WHERE akvo_ecosia_tree_area_registration.organisation = 'fundacion dia'
AND akvo_ecosia_tree_area_monitoring.identifier_akvo = akvo_ecosia_tree_area_registration.identifier_akvo));
CREATE POLICY fdia_policy ON akvo_ecosia_nursery_registration TO nicaragua_fdia USING (organisation = 'fundacion dia');
CREATE POLICY fdia_policy ON akvo_ecosia_nursery_monitoring TO nicaragua_fdia USING (EXISTS (SELECT * FROM akvo_ecosia_nursery_registration
WHERE akvo_ecosia_nursery_registration.organisation = 'fundacion dia'
AND akvo_ecosia_nursery_monitoring.identifier_akvo = akvo_ecosia_nursery_registration.identifier_akvo));
CREATE POLICY fdia_policy ON error_partner_report_on_site_registration TO nicaragua_fdia USING (error_partner_report_on_site_registration.organisation = 'fundacion dia');
CREATE POLICY fdia_policy ON error_partner_report_on_nursery_registration TO nicaragua_fdia USING (error_partner_report_on_nursery_registration.organisation = 'fundacion dia');
CREATE POLICY fdia_policy ON akvo_ecosia_contract_overview TO nicaragua_fdia USING (akvo_ecosia_contract_overview.organisation = 'fundacion dia');
CREATE POLICY fdia_policy ON akvo_ecosia_tree_photo_registration TO nicaragua_fdia USING (akvo_ecosia_tree_photo_registration.organisation = 'fundacion dia');'''

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

CREATE POLICY haf_policy ON akvo_ecosia_tree_area_registration TO morocco_haf USING (organisation = 'high atlas foundation');
CREATE POLICY haf_policy ON akvo_ecosia_tree_area_monitoring TO morocco_haf USING (EXISTS (SELECT * FROM akvo_ecosia_tree_area_registration
WHERE akvo_ecosia_tree_area_registration.organisation = 'high atlas foundation'
AND akvo_ecosia_tree_area_monitoring.identifier_akvo = akvo_ecosia_tree_area_registration.identifier_akvo));
CREATE POLICY haf_policy ON akvo_ecosia_nursery_registration TO morocco_haf USING (organisation = 'high atlas foundation');
CREATE POLICY haf_policy ON akvo_ecosia_nursery_monitoring TO morocco_haf USING (EXISTS (SELECT * FROM akvo_ecosia_nursery_registration
WHERE akvo_ecosia_nursery_registration.organisation = 'high atlas foundation'
AND akvo_ecosia_nursery_monitoring.identifier_akvo = akvo_ecosia_nursery_registration.identifier_akvo));
CREATE POLICY haf_policy ON error_partner_report_on_site_registration TO morocco_haf USING (error_partner_report_on_site_registration.organisation = 'high atlas foundation');
CREATE POLICY haf_policy ON error_partner_report_on_nursery_registration TO morocco_haf USING (error_partner_report_on_nursery_registration.organisation = 'high atlas foundation');
CREATE POLICY haf_policy ON akvo_ecosia_contract_overview TO morocco_haf USING (akvo_ecosia_contract_overview.organisation = 'high atlas foundation');
CREATE POLICY haf_policy ON akvo_ecosia_tree_photo_registration TO morocco_haf USING (akvo_ecosia_tree_photo_registration.organisation = 'high atlas foundation');'''

conn.commit()

create_a20_ecosia_superset = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ecosia_superset;

GRANT USAGE ON SCHEMA PUBLIC TO ecosia_superset;
GRANT USAGE ON SCHEMA HEROKU_EXT TO ecosia_superset;

GRANT SELECT ON TABLE superset_ecosia_nursery_registration TO ecosia_superset;
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
GRANT SELECT ON TABLE superset_ecosia_site_registration_unregistered_farmers TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_contract_overview TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_new_devices TO ecosia_superset;

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
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_site_registration_unregistered_farmers;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_contract_overview;
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_new_devices;

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
ALTER TABLE superset_ecosia_site_registration_unregistered_farmers enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_contract_overview enable ROW LEVEL SECURITY;
ALTER TABLE superset_ecosia_new_devices enable ROW LEVEL SECURITY;

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
CREATE POLICY ecosia_superset_policy ON superset_ecosia_site_registration_unregistered_farmers TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_contract_overview TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_new_devices TO ecosia_superset USING (true);'''

conn.commit()

create_a21_s4g = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM netherlands_s4g;

GRANT USAGE ON SCHEMA PUBLIC TO netherlands_s4g;
GRANT USAGE ON SCHEMA HEROKU_EXT TO netherlands_s4g;

GRANT SELECT ON TABLE akvo_tree_registration_areas_updated TO netherlands_s4g;
GRANT SELECT ON TABLE CALC_TAB_monitoring_calculations_per_site TO netherlands_s4g;

DROP POLICY IF EXISTS s4g_ecosia_policy ON akvo_tree_registration_areas_updated;
DROP POLICY IF EXISTS s4g_ecosia_policy ON CALC_TAB_monitoring_calculations_per_site;

ALTER TABLE akvo_tree_registration_areas_updated enable ROW LEVEL SECURITY;
ALTER TABLE CALC_TAB_monitoring_calculations_per_site enable ROW LEVEL SECURITY;

CREATE POLICY s4g_ecosia_policy ON akvo_tree_registration_areas_updated TO netherlands_s4g USING (true);
CREATE POLICY s4g_ecosia_policy ON CALC_TAB_monitoring_calculations_per_site TO netherlands_s4g USING (true);'''

conn.commit()

create_a22_ecosia_viewing = '''
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ecosia;

GRANT USAGE ON SCHEMA PUBLIC TO ecosia;
GRANT USAGE ON SCHEMA HEROKU_EXT TO ecosia;

GRANT SELECT ON TABLE akvo_tree_registration_areas_updated TO ecosia;
GRANT SELECT ON TABLE akvo_tree_monitoring_areas TO ecosia;
GRANT SELECT ON TABLE akvo_nursery_registration TO ecosia;
GRANT SELECT ON TABLE akvo_nursery_monitoring TO ecosia;
GRANT SELECT ON TABLE akvo_tree_registration_photos TO ecosia;

DROP POLICY IF EXISTS ecosia_policy ON akvo_tree_registration_areas_updated;
DROP POLICY IF EXISTS ecosia_policy ON akvo_tree_monitoring_areas;
DROP POLICY IF EXISTS ecosia_policy ON akvo_nursery_registration;
DROP POLICY IF EXISTS ecosia_policy ON akvo_nursery_monitoring;
DROP POLICY IF EXISTS ecosia_policy ON akvo_tree_registration_photos;

ALTER TABLE akvo_tree_registration_areas_updated enable ROW LEVEL SECURITY;
ALTER TABLE akvo_tree_monitoring_areas enable ROW LEVEL SECURITY;
ALTER TABLE akvo_nursery_registration enable ROW LEVEL SECURITY;
ALTER TABLE akvo_nursery_monitoring enable ROW LEVEL SECURITY;
ALTER TABLE akvo_tree_registration_photos enable ROW LEVEL SECURITY;

CREATE POLICY ecosia_policy ON akvo_tree_registration_areas_updated TO ecosia USING (true);
CREATE POLICY ecosia_policy ON akvo_tree_monitoring_areas TO ecosia USING (true);
CREATE POLICY ecosia_policy ON akvo_nursery_registration TO ecosia USING (true);
CREATE POLICY ecosia_policy ON akvo_nursery_monitoring TO ecosia USING (true);
CREATE POLICY ecosia_policy ON akvo_tree_registration_photos TO ecosia USING (true);'''

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
#cur.execute(create_a7)
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
cur.execute(create_a35)
cur.execute(create_a36)
cur.execute(create_a37)
cur.execute(create_a38)
cur.execute(create_a39)
cur.execute(create_a40)
cur.execute(create_a41)
cur.execute(create_a42)
cur.execute(create_a43)
cur.execute(create_a44)
cur.execute(create_a45)
cur.execute(create_a46)
cur.execute(create_a47)
cur.execute(create_a48)
cur.execute(create_a49)

cur.execute(create_a17_mkec)
cur.execute(create_a18_fdia)
cur.execute(create_a20_ecosia_superset)
cur.execute(create_a21_s4g)
cur.execute(create_a22_ecosia_viewing)

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

print('all queries are processed')
