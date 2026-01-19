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
DROP TABLE IF EXISTS AKVO_tree_registration_areas_integrated;
DROP TABLE IF EXISTS calc_tab_monitoring_calculations_per_site_merged_by_partner;
DROP TABLE IF EXISTS calc_tab_monitoring_calculations_per_site_merged_akvo;
DROP TABLE IF EXISTS calc_tab_monitoring_calculations_per_site_merged_odk;
DROP TABLE IF EXISTS calc_tab_monitoring_calculations_per_site_merged;
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
--DROP TABLE IF EXISTS akvo_tree_registration_areas_updated_remotesensing;
DROP TABLE IF EXISTS superset_ecosia_kanop_chloris_results;'''

conn.commit()

create_a1_integrated = '''
CREATE TABLE akvo_tree_registration_areas_integrated
AS

-- THIS QUERY IS A JOIN of the ODK registration table, the AKVO registration table and the AKVO + ODK tree distribution table (of unregistered farmers). Include all columns for the registration update table.

-- Here we prepare the AKVO tree registration of unregistered farmers. A join is needed for this query

WITH CTE_join_tree_distribution_and_registration_AKVO AS (SELECT
a.identifier_akvo,
a.displayname,
a.device_id,
a.instance,
a.submission_date,
a.submission_date AS updated_at,
a.submitter,
a.form_version,
'AKVO' AS source_data,
'unregistered_farmers' AS data_form,
b.country,
LOWER(b.organisation) AS organisation,
b.contract_number,

CASE
WHEN a.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN a.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN a.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN a.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN a.test = ''
THEN 'This is real, valid data'
WHEN a.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN a.test = 'valid_data'
THEN 'This is real, valid data'
WHEN a.test = 'Valid data'
THEN 'This is real, valid data'
WHEN a.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

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
LEFT JOIN akvo_tree_distribution_unregistered_farmers AS b
ON a.identifier_akvo = b.identifier_akvo),

-- Make a CTE table to do a count for unregistered farmers with the ODK form
CTE_odk_tree_count_unregistered_farmers AS (SELECT submissionid_odk, SUM(tree_species_number_registered) total_registered_tree_number
FROM ODK_unregistered_farmers_tree_registration_species
GROUP BY ODK_unregistered_farmers_tree_registration_species.submissionid_odk),

-- Here we prepare the ODK tree registration of unregistered farmers. A join is needed for this query
CTE_join_tree_distribution_and_registration_ODK AS (SELECT
a.submissionid_odk,
b.device_id,
b.submission_date,

CASE
WHEN b.updated_at NOTNULL
THEN b.updated_at
WHEN b.updated_at ISNULL
THEN b.submission_date
END AS updated_at,

b.field_date,
'ODK' AS source_data,
'unregistered_farmers' AS data_form,

CASE
WHEN b.planting_date NOTNULL
THEN b.planting_date
ELSE b.field_date::TEXT
END AS planting_date,

b.submitter,
b.odk_form_version::varchar(10),

CASE
WHEN b.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = ''
THEN 'This is real, valid data'
WHEN b.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN b.test = 'valid_data'
THEN 'This is real, valid data'
WHEN b.test = 'Valid data'
THEN 'This is real, valid data'
WHEN b.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(a.organisation) AS organisation,
a.contract_number,
a.planting_site_id,
a.name_location_tree_planting,
b.registration_multiple_locations,
a.recipient_full_name,
a.recipient_photo,
a.recipient_gender,
a.total_tree_nr_handed_out,
b.comment_planting_site,
b.planting_system_used,

CASE
WHEN b.polygon NOTNULL
THEN NULL
WHEN b.line NOTNULL
THEN NULL
WHEN b.point NOTNULL
THEN b.point
END AS geometry,

c.total_registered_tree_number

FROM odk_unregistered_farmers_tree_registration_main b
JOIN odk_unregistered_farmers_tree_handout_main a
ON a.ecosia_site_id_dist = b.ecosia_site_id_dist
JOIN CTE_odk_tree_count_unregistered_farmers c
ON b.submissionid_odk = c.submissionid_odk),

-- Below we integrate a few specific registrations that have no link with neither the AKVO nor the ODK tree distribution form. AS such, we still integrate them to the dashboard since the number of trees and location are in this dataset. But no farmer name, no submissionid/identifier_akvo or no id_planting_site is there....
CTE_join_tree_distribution_and_registration_empty_no_links AS (SELECT
b.submissionid_odk,
b.device_id,
b.submission_date,

CASE
WHEN b.updated_at NOTNULL
THEN b.updated_at
WHEN b.updated_at ISNULL
THEN b.submission_date
END AS updated_at,

'' AS instance,
b.field_date,
'ODK' AS source_data,
'unregistered_farmers' AS data_form,

CASE
WHEN b.planting_date NOTNULL
THEN b.planting_date
ELSE b.field_date::TEXT
END AS planting_date,

b.submitter,
b.odk_form_version::varchar(10),

CASE
WHEN b.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = ''
THEN 'This is real, valid data'
WHEN b.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN b.test = 'valid_data'
THEN 'This is real, valid data'
WHEN b.test = 'Valid data'
THEN 'This is real, valid data'
WHEN b.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

'symagine - ssdc' AS organisation,
179.20 AS contract_number,
'unknown' AS planting_site_id,
'unknown' AS name_location_tree_planting,
'unknown' AS registration_multiple_locations,
'unknown' AS recipient_full_name,
'unknown' AS recipient_photo,
'unknown' AS recipient_gender,
0 AS total_tree_nr_handed_out,
b.comment_planting_site,
b.planting_system_used,

CASE
WHEN b.polygon NOTNULL
THEN NULL
WHEN b.line NOTNULL
THEN NULL
WHEN b.point NOTNULL
THEN b.point
END AS geometry,

c.total_registered_tree_number

FROM odk_unregistered_farmers_tree_registration_main b
JOIN CTE_odk_tree_count_unregistered_farmers c
ON b.submissionid_odk = c.submissionid_odk
where odk_form_version IN ('2025-09-03_v1','2025-08-29_v14', '2025-08-29_v13', '2025-08-29_v7', '2025-08-29_v5', '2025-08-29_v2')
AND name_owner ISNULL
AND b.ecosia_site_id_dist = ''),

--------------

-- Here we prepare the AKVO-ODK tree registration of unregistered farmers.
-- Instances of listed farmers were once collected with the AKVO form and (could) later on having been registered with the ODK form (ODK registration of unregistered farmers)
-- A join is needed for this query.

CTE_join_tree_distribution_and_registration_AKVO_vs_ODK AS (SELECT
a.identifier_akvo,
b.device_id,
b.submission_date,

CASE
WHEN b.updated_at NOTNULL
THEN b.updated_at
WHEN b.updated_at ISNULL
THEN b.submission_date
END AS updated_at,

a.instance,
b.field_date,
'AKVO-ODK' AS source_data,
'unregistered_farmers' AS data_form,

CASE
WHEN b.planting_date NOTNULL
THEN b.planting_date
ELSE b.field_date::TEXT
END AS planting_date,

b.submitter,
b.odk_form_version::varchar(10),

CASE
WHEN b.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN b.test = ''
THEN 'This is real, valid data'
WHEN b.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN b.test = 'valid_data'
THEN 'This is real, valid data'
WHEN b.test = 'Valid data'
THEN 'This is real, valid data'
WHEN b.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(a.organisation) AS organisation,
a.contract_number,
a.name_site_id_tree_planting AS planting_site_id,
a.location_house_tree_receiver AS name_location_tree_planting,
a.confirm_planting_location AS registration_multiple_locations,
a.name_tree_receiver AS recipient_full_name,
a.url_photo_receiver_trees AS recipient_photo,
a.gender_tree_receiver AS recipient_gender,
a.total_number_trees_received AS total_tree_nr_handed_out,
b.comment_planting_site,
b.planting_system_used,

CASE
WHEN b.polygon NOTNULL
THEN NULL
WHEN b.line NOTNULL
THEN NULL
WHEN b.point NOTNULL
THEN b.point
END AS geometry,

c.total_registered_tree_number

FROM odk_unregistered_farmers_tree_registration_main b
-- Only here a left join so that registrations that -for all unknown reasons- do not have a key linkage with the farmers list (AKVO nor ODK), will still enter in the table with site registrations. Only here a left join and not in the other tables because then we get duplicates of these 'no linkage' site registrations.
LEFT JOIN akvo_tree_distribution_unregistered_farmers a
ON a.identifier_akvo = b.ecosia_site_id_dist
JOIN CTE_odk_tree_count_unregistered_farmers c
ON b.submissionid_odk = c.submissionid_odk),


---
--- UNION of all data together into 1 table (ODK tree registration (normal) + AKVO tree registration (normal) + ODK unregistered farmers + AKVO unregistered farmers)
---

union_tree_registration_tree_registration_unreg_farmers AS (
SELECT
c.identifier_akvo,
c.display_name,
c.device_id,
c.instance,
c.submission,
c.submission_year,
c.submissiontime,

CASE
WHEN c.modifiedat IS NOT NULL
THEN c.modifiedat::timestamp AT TIME ZONE 'Europe/Amsterdam'
WHEN c.modifiedat IS NULL
THEN c.submission::timestamp AT TIME ZONE 'Europe/Amsterdam'
END AS updated_at,

c.submitter,
c.modifiedat,
c.akvo_form_version::varchar(10),
'AKVO' AS data_source,
'normal tree registration' AS form_source,
c.country,

CASE
WHEN c.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN c.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN c.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN c.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN c.test = ''
THEN 'This is real, valid data'
WHEN c.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN c.test = 'valid_data'
THEN 'This is real, valid data'
WHEN c.test = 'Valid data'
THEN 'This is real, valid data'
WHEN c.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(c.organisation) AS organisation,
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

--- UNION with data from AKVO tree registrations of unregistered farmers

SELECT
d.identifier_akvo,
d.displayname,
d.device_id,
d.instance,
d.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,

CASE
WHEN d.updated_at NOTNULL
THEN d.updated_at
WHEN d.updated_at ISNULL
THEN d.submission_date
END AS updated_at,

d.submitter,
'n/a' AS modifiedat,
d.form_version::varchar(10),
'AKVO' AS data_source,
'unregistered_farmers' AS data_form,
d.country,

CASE
WHEN d.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN d.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN d.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN d.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN d.test = ''
THEN 'This is real, valid data'
WHEN d.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN d.test = 'valid_data'
THEN 'This is real, valid data'
WHEN d.test = 'Valid data'
THEN 'This is real, valid data'
WHEN d.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,


LOWER(d.organisation) AS organisation,
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

FROM CTE_join_tree_distribution_and_registration_AKVO AS d

UNION ALL

--- UNION with data from ODK tree registrations

SELECT
h.submissionid_odk as identifier_akvo,
CONCAT(h.contract_number,' - ', h.id_planting_site, ' - ', h.name_owner) AS displayname,
h.device_id,
0 AS instance,
h.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,

CASE
WHEN h.updated_at NOTNULL
THEN h.updated_at
WHEN h.updated_at ISNULL
THEN h.submission_date
END AS updated_at,

h.submitter,
'n/a' AS modifiedat,
h.odk_form_version AS form_version,
'ODK' AS data_source,
'normal tree registration' AS form_source,
h.country,

CASE
WHEN h.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN h.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN h.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN h.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN h.test = ''
THEN 'This is real, valid data'
WHEN h.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN h.test = 'valid_data'
THEN 'This is real, valid data'
WHEN h.test = 'Valid data'
THEN 'This is real, valid data'
WHEN h.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(h.organisation) AS organisation,
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

h.landscape_element_type,  ----AS more_less_200_trees,

h.planting_date AS planting_date,
h.tree_number AS tree_number,
h.calc_area AS estimated_area,
h.calc_area AS area_ha,
NULL AS lat_y,
NULL AS lon_x,
6 AS number_coord_pol,
h.centroid_coord,
h.polygon,
NULL AS multipoint,
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

FROM odk_tree_registration_main AS h

UNION ALL


--- UNION with data from ODK tree registrations of unregistered farmers

SELECT
i.submissionid_odk AS identifier_akvo,
CONCAT(i.contract_number,' - ', i.planting_site_id, ' - ', i.recipient_full_name) AS displayname,

i.device_id,
0 AS instance,
i.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,

CASE
WHEN i.updated_at NOTNULL
THEN i.updated_at
WHEN i.updated_at ISNULL
THEN i.submission_date
END AS updated_at,

i.submitter,
'n/a' AS modifiedat,
i.odk_form_version::varchar(10),
'ODK' AS data_source,
'unregistered_farmers' AS form_source,
'' AS country,

CASE
WHEN i.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN i.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN i.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN i.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN i.test = ''
THEN 'This is real, valid data'
WHEN i.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN i.test = 'valid_data'
THEN 'This is real, valid data'
WHEN i.test = 'Valid data'
THEN 'This is real, valid data'
WHEN i.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(i.organisation) AS organisation,
i.contract_number,
i.planting_site_id AS name_site_id_tree_planting,
'n/a' AS check_ownership_land,
'n/a' AS name_region_village_planting_site,
'n/a' AS name_region,
i.recipient_full_name AS name_owner_planting_site,
i.recipient_photo AS photo_owner_planting_site,
i.recipient_gender AS gender_owner_planting_site,
'n/a' AS objective_site,
'n/a' AS site_preparation,
i.planting_system_used AS planting_technique,
'n/a' AS planting_system,
i.comment_planting_site AS comment_enumerator,
i.planting_system_used AS landscape_element_type,
i.planting_date AS planting_date,
i.total_registered_tree_number AS tree_number,
NULL AS estimated_area,
NULL AS area_ha,
NULL AS lat_y,
NULL AS lon_x,
0 AS number_coord_pol,
i.geometry AS centroid_coord,
NULL AS polygon,
NULL AS multipoint,
i.registration_multiple_locations AS confirm_plant_location_own_land,
'n/a' AS one_multiple_planting_sites,
0 AS nr_trees_given_away,
i.total_tree_nr_handed_out AS nr_trees_received,
'n/a' AS url_photo_receiver_trees,
'n/a' AS location_house_tree_receiver,
'n/a' AS confirm_planting_location,
'n/a' AS url_signature_tree_receiver,
i.total_tree_nr_handed_out AS total_number_trees_received,
'n/a' AS check_ownership_trees,
'n/a' AS gender_tree_receiver,
'n/a' AS name_tree_receiver

FROM CTE_join_tree_distribution_and_registration_ODK AS i


UNION ALL


--- UNION with data from ODK tree registrations of unregistered farmers that have empty id's, farmer names and planting site ID's

SELECT
p.submissionid_odk AS identifier_akvo,
CONCAT(p.contract_number,' - ', p.planting_site_id, ' - ', p.recipient_full_name) AS displayname,

p.device_id,
0 AS instance,
p.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,

CASE
WHEN p.updated_at NOTNULL
THEN p.updated_at
WHEN p.updated_at ISNULL
THEN p.submission_date
END AS updated_at,

p.submitter,
'n/a' AS modifiedat,
p.odk_form_version::varchar(10),
'ODK' AS data_source,
'unregistered_farmers' AS form_source,
'' AS country,

CASE
WHEN p.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN p.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN p.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN p.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN p.test = ''
THEN 'This is real, valid data'
WHEN p.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN p.test = 'valid_data'
THEN 'This is real, valid data'
WHEN p.test = 'Valid data'
THEN 'This is real, valid data'
WHEN p.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(p.organisation) AS organisation,
p.contract_number,
p.planting_site_id AS name_site_id_tree_planting,
'n/a' AS check_ownership_land,
'n/a' AS name_region_village_planting_site,
'n/a' AS name_region,
p.recipient_full_name AS name_owner_planting_site,
p.recipient_photo AS photo_owner_planting_site,
p.recipient_gender AS gender_owner_planting_site,
'n/a' AS objective_site,
'n/a' AS site_preparation,
p.planting_system_used AS planting_technique,
'n/a' AS planting_system,
p.comment_planting_site AS comment_enumerator,
p.planting_system_used AS landscape_element_type,
p.planting_date AS planting_date,
p.total_registered_tree_number AS tree_number,
NULL AS estimated_area,
NULL AS area_ha,
NULL AS lat_y,
NULL AS lon_x,
0 AS number_coord_pol,
p.geometry AS centroid_coord,
NULL AS polygon,
NULL AS multipoint,
p.registration_multiple_locations AS confirm_plant_location_own_land,
'n/a' AS one_multiple_planting_sites,
0 AS nr_trees_given_away,
p.total_tree_nr_handed_out AS nr_trees_received,
'n/a' AS url_photo_receiver_trees,
'n/a' AS location_house_tree_receiver,
'n/a' AS confirm_planting_location,
'n/a' AS url_signature_tree_receiver,
p.total_tree_nr_handed_out AS total_number_trees_received,
'n/a' AS check_ownership_trees,
'n/a' AS gender_tree_receiver,
'n/a' AS name_tree_receiver

FROM CTE_join_tree_distribution_and_registration_empty_no_links AS p


UNION ALL

--- UNION with data from the AKVO listing of unregistered farmers and the area registration of those listed farmers with the ODK registration form

SELECT
j.identifier_akvo,
CONCAT(j.contract_number,' - ', j.planting_site_id, ' - ', j.recipient_full_name) AS displayname,

j.device_id,
j.instance,
j.submission_date,
NULL AS submission_year,
'n/a' AS submissiontime,

CASE
WHEN j.updated_at NOTNULL
THEN j.updated_at
WHEN j.updated_at ISNULL
THEN j.submission_date
END AS updated_at,


j.submitter,
'n/a' AS modifiedat,
j.odk_form_version::varchar(10),
'AKVO-ODK' AS data_source,
'unregistered_farmers' AS form_source,
'' AS country,

CASE
WHEN j.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN j.test = 'This is a test, this record can be deleted.'
THEN 'This is a test, this record can be deleted.'
WHEN j.test = 'This is a test, this record can be deleted'
THEN 'This is a test, this record can be deleted.'
WHEN j.test = 'xxxxx'
THEN 'This is a test, this record can be deleted.'
WHEN j.test = ''
THEN 'This is real, valid data'
WHEN j.test = 'This is real, valid data\r'
THEN 'This is real, valid data'
WHEN j.test = 'valid_data'
THEN 'This is real, valid data'
WHEN j.test = 'Valid data'
THEN 'This is real, valid data'
WHEN j.test = 'This is real, valid data'
THEN 'This is real, valid data'
END AS test,

LOWER(j.organisation) AS organisation,
j.contract_number,
j.planting_site_id AS name_site_id_tree_planting,
'n/a' AS check_ownership_land,
'n/a' AS name_region_village_planting_site,
'n/a' AS name_region,
j.recipient_full_name AS name_owner_planting_site,
j.recipient_photo AS photo_owner_planting_site,
j.recipient_gender AS gender_owner_planting_site,
'n/a' AS objective_site,
'n/a' AS site_preparation,
j.planting_system_used AS planting_technique,
'n/a' AS planting_system,
j.comment_planting_site AS comment_enumerator,
j.planting_system_used AS landscape_element_type,
j.planting_date AS planting_date,
j.total_registered_tree_number AS tree_number,
NULL AS estimated_area,
NULL AS area_ha,
NULL AS lat_y,
NULL AS lon_x,
0 AS number_coord_pol,
j.geometry AS centroid_coord,
NULL AS polygon,
NULL AS multipoint,
j.registration_multiple_locations AS confirm_plant_location_own_land,
'n/a' AS one_multiple_planting_sites,
0 AS nr_trees_given_away,
j.total_tree_nr_handed_out AS nr_trees_received,
'n/a' AS url_photo_receiver_trees,
'n/a' AS location_house_tree_receiver,
'n/a' AS confirm_planting_location,
'n/a' AS url_signature_tree_receiver,
j.total_tree_nr_handed_out AS total_number_trees_received,
'n/a' AS check_ownership_trees,
'n/a' AS gender_tree_receiver,
'n/a' AS name_tree_receiver

FROM CTE_join_tree_distribution_and_registration_AKVO_vs_ODK AS j)

SELECT * FROM union_tree_registration_tree_registration_unreg_farmers
WHERE organisation != '';

------------- END OF ALL UNIONS. In the last statement the UNION table called 'akvo_tree_registration_areas_integrated' is created by: SELECT * FROM union_tree_registration_tree_registration_unreg_farmers.


-- Below we integrate the registration of additional trees (from the ODK form) into the initial registration of (the number of) trees.
-- We need to group and SUM them first since there can be multiple 'added tree' submissions for 1 site.
WITH added_trees_per_site AS
(select ecosia_site_id, SUM(nr_added_trees) AS added_trees FROM odk_tree_monitoring_main
where test = 'valid_data'
and nr_added_trees NOTNULL
group by ecosia_site_id)

UPDATE akvo_tree_registration_areas_integrated a
SET tree_number = coalesce(a.tree_number,0) + added_trees_per_site.added_trees
FROM added_trees_per_site
WHERE a.identifier_akvo = added_trees_per_site.ecosia_site_id;


-- Below we update the planting date. When during a registration, no trees were registered, but only the site was mapped, no planting date is being generated.
-- In that case, the submission date is used in the ODK_registration download script.
-- That date needs to be updated with the planting date during a tree monitoring, using the option ADDING of trees.
-- Once a planting date is available (through the ADDING of trees option in monitoring), we use the EARLIEST date (using ASC instead of DESC).
-- This to prevent that the planting date is always updated with latest ADDING of trees. We want to know when the FIRST trees were planted (for RS purposes)
WITH planting_date_added_trees AS
(SELECT
ecosia_site_id,
planting_date_added,
ROW_NUMBER() OVER (PARTITION BY ecosia_site_id ORDER BY planting_date_added ASC) AS rn
FROM odk_tree_monitoring_main
WHERE planting_date_added NOTNULL)

UPDATE akvo_tree_registration_areas_integrated a
SET planting_date = p.planting_date_added
FROM planting_date_added_trees p
WHERE a.identifier_akvo = p.ecosia_site_id
AND p.rn = 1;


-- Add GEOMETRIC CHECK columns so that they can later be populated with the geometric correction script
ALTER TABLE akvo_tree_registration_areas_integrated
ADD species_latin text,
--The columns below are UPDATED with the geometric error script (AKVO_database_PG_queries_v1_sql_geometric_error_detection.py)
ADD self_intersection BOOLEAN,
ADD overlap TEXT,
ADD outside_country BOOLEAN,
ADD check_200_trees BOOLEAN,
ADD check_duplicate_polygons TEXT,
ADD needle_shape BOOLEAN,
ADD total_nr_geometric_errors INTEGER;

-- Add column to check whether a site was remapped by the partner or not. This is important for the edit function. Set default to 'no'
ALTER TABLE akvo_tree_registration_areas_integrated
ADD re_mapped_by_partner TEXT DEFAULT 'no';

-- Below we transpose all species from multiple rows into 1 row
WITH t as
(SELECT akvo_tree_registration_species.identifier_akvo,
	STRING_AGG(akvo_tree_registration_species.lat_name_species,' | ')
 species_list
	  FROM akvo_tree_registration_species
 JOIN akvo_tree_registration_areas_integrated
 ON akvo_tree_registration_species.identifier_akvo = akvo_tree_registration_areas_integrated.identifier_akvo
 GROUP BY akvo_tree_registration_species.identifier_akvo)

UPDATE akvo_tree_registration_areas_integrated
SET species_latin = t.species_list
FROM t
WHERE t.identifier_akvo = akvo_tree_registration_areas_integrated.identifier_akvo;

UPDATE akvo_tree_registration_areas_integrated
SET calc_area = 0.2
WHERE polygon ISNULL;'''

conn.commit()


# A copy is made from the INTEGRATED TABLE and named UPDATED table. This is done only once (see script "create_a1_insertion" below)!
create_a1_updated = '''CREATE TABLE IF NOT EXISTS akvo_tree_registration_areas_updated
AS (SELECT * FROM akvo_tree_registration_areas_integrated);

ALTER TABLE akvo_tree_registration_areas_updated
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS re_mapped_by_partner TEXT DEFAULT 'no',
ADD COLUMN IF NOT EXISTS edit_confirmation BOOLEAN DEFAULT False;'''

conn.commit()

# If an instance (identifier) already exists then the instance will be UPDATED for the polygon column (see script below).
# By doing so, polygon updates made on the ODK/AKVO platform (by the re-map option) will be processed
create_a1_updates_from_odk_akvo_server_side_updated = '''
-- Below we check if a new polygon (re-mapped polygon) was submitted by AKVO collect. If multiple submissions of new polygons were done, the latest submission will be selected
WITH updates_polygon_akvo AS (SELECT identifier_akvo, polygon_remapped, submission
FROM (SELECT
identifier_akvo,
polygon_remapped,
submission,
ROW_NUMBER() OVER (PARTITION BY identifier_akvo ORDER BY submission DESC) AS most_recent_akvo
FROM akvo_tree_monitoring_remapped_areas
WHERE polygon_remapped NOTNULL) sub
WHERE most_recent_akvo = 1)

-- No 'updated_at' criteria used here because AKVO does not have a column for that in their API...(?)
-- Updates from the AKVO collect (remapped sites with monitoring) are integrated in UPDATED TABLE
UPDATE akvo_tree_registration_areas_updated
SET
polygon = updates_polygon_akvo.polygon_remapped,
re_mapped_by_partner = 'yes'
FROM updates_polygon_akvo
WHERE akvo_tree_registration_areas_updated.identifier_akvo = updates_polygon_akvo.identifier_akvo
AND (akvo_tree_registration_areas_updated.edit_confirmation = False
OR akvo_tree_registration_areas_updated.edit_confirmation ISNULL);


-- Below we check if a new polygon was submitted by ODK collect. If multiple submissions of new polygons were done, the latest submission will be selected
WITH updates_polygon_odk AS (SELECT ecosia_site_id, remaped_polygon_planting_site, submission_date
FROM (
    SELECT
        ecosia_site_id,
        remaped_polygon_planting_site,
        submission_date,
        ROW_NUMBER() OVER (PARTITION BY ecosia_site_id ORDER BY submission_date DESC) as most_recent_odk
    FROM odk_tree_monitoring_main
    WHERE remaped_polygon_planting_site NOTNULL
) sub
WHERE most_recent_odk = 1)

-- Updates from the ODK collect (remapped sites with monitoring) are integrated in UPDATED TABLE
UPDATE akvo_tree_registration_areas_updated
SET
polygon = updates_polygon_odk.remaped_polygon_planting_site,
re_mapped_by_partner = 'yes',
updated_at = updates_polygon_odk.submission_date
FROM updates_polygon_odk
WHERE akvo_tree_registration_areas_updated.identifier_akvo = updates_polygon_odk.ecosia_site_id
AND (akvo_tree_registration_areas_updated.edit_confirmation = False
OR akvo_tree_registration_areas_updated.edit_confirmation ISNULL);'''

conn.commit()

# Update also the EDITS table with polygon corrections uploaded by the partner (with AKVO and ODK app)
create_a1_updates_from_odk_akvo_server_side_edits = '''
-- Below we check if a new polygon (re-mapped polygon) was submitted by AKVO collect. If multiple submissions of new polygons were done, the latest submission will be selected
WITH updates_polygon_akvo AS (SELECT identifier_akvo, polygon_remapped, submission
FROM (SELECT
identifier_akvo,
polygon_remapped,
submission,
ROW_NUMBER() OVER (PARTITION BY identifier_akvo ORDER BY submission DESC) AS most_recent_akvo
FROM akvo_tree_monitoring_remapped_areas
WHERE polygon_remapped NOTNULL) sub
WHERE most_recent_akvo = 1)

-- No 'updated_at' criteria used here because AKVO does not have a column for that in their API...(?)
-- Updates from the AKVO collect (remapped sites with monitoring) are integrated in EDITS TABLE
UPDATE akvo_tree_registration_areas_edits
SET
polygon = updates_polygon_akvo.polygon_remapped,
re_mapped_by_partner = 'yes'
FROM updates_polygon_akvo
WHERE akvo_tree_registration_areas_edits.identifier_akvo = updates_polygon_akvo.identifier_akvo
AND (akvo_tree_registration_areas_edits.edit_confirmation = False
OR akvo_tree_registration_areas_edits.edit_confirmation ISNULL);


-- Below we check if a new polygon was submitted by ODK collect. If multiple submissions of new polygons were done, the latest submission will be selected
WITH updates_polygon_odk AS (SELECT ecosia_site_id, remaped_polygon_planting_site, submission_date
FROM (
    SELECT
        ecosia_site_id,
        remaped_polygon_planting_site,
        submission_date,
        ROW_NUMBER() OVER (PARTITION BY ecosia_site_id ORDER BY submission_date DESC) as most_recent_odk
    FROM odk_tree_monitoring_main
    WHERE remaped_polygon_planting_site NOTNULL
) sub
WHERE most_recent_odk = 1)

-- Updates from the ODK collect (remapped sites with monitoring) are integrated in EDITS TABLE
UPDATE akvo_tree_registration_areas_edits
SET
polygon = updates_polygon_odk.remaped_polygon_planting_site,
re_mapped_by_partner = 'yes',
updated_at = updates_polygon_odk.submission_date
FROM updates_polygon_odk
WHERE akvo_tree_registration_areas_edits.identifier_akvo = updates_polygon_odk.ecosia_site_id
AND (akvo_tree_registration_areas_edits.edit_confirmation = False
OR akvo_tree_registration_areas_edits.edit_confirmation ISNULL);'''

conn.commit()



# The UPDATED table is maintained (not deleted) and only updated with NEW intances (downloads) from the INTEGRATED table
create_a1_insertion = '''INSERT INTO akvo_tree_registration_areas_updated
(
identifier_akvo,
display_name,
device_id,
instance,
submission,
submission_year,
submissiontime,
updated_at,
submitter,
modifiedat,
akvo_form_version,
data_source,
form_source,
country,
test,
organisation,
contract_number,
id_planting_site,
land_title,
name_village,
name_region,
name_owner,
photo_owner,
gender_owner,
objective_site,
site_preparation,
planting_technique,
planting_system,
remark,
nr_trees_option,
planting_date,
tree_number,
estimated_area,
calc_area,
lat_y,
lon_x,
number_coord_polygon,
centroid_coord,
polygon,
re_mapped_by_partner,
multipoint,
confirm_plant_location_own_land,
one_multiple_planting_sites,
nr_trees_given_away,
nr_trees_received,
url_photo_receiver_trees,
location_house_tree_receiver,
confirm_planting_location,
url_signature_tree_receiver,
total_number_trees_received,
check_ownership_trees,
gender_tree_receiver,
name_tree_receiver,
species_latin,
self_intersection,
overlap,
outside_country,
check_200_trees,
check_duplicate_polygons,
needle_shape,
total_nr_geometric_errors,
edit_confirmation)


SELECT
identifier_akvo,
display_name,
device_id,
instance,
submission,
submission_year,
submissiontime,
updated_at,
submitter,
modifiedat,
akvo_form_version,
data_source,
form_source,
country,
test,
organisation,
contract_number,
id_planting_site,
land_title,
name_village,
name_region,
name_owner,
photo_owner,
gender_owner,
objective_site,
site_preparation,
planting_technique,
planting_system,
remark,
nr_trees_option,
planting_date,
tree_number,
estimated_area,
calc_area,
lat_y,
lon_x,
number_coord_polygon,
centroid_coord,
polygon,
re_mapped_by_partner,
multipoint,
confirm_plant_location_own_land,
one_multiple_planting_sites,
nr_trees_given_away,
nr_trees_received,
url_photo_receiver_trees,
location_house_tree_receiver,
confirm_planting_location,
url_signature_tree_receiver,
total_number_trees_received,
check_ownership_trees,
gender_tree_receiver,
name_tree_receiver,
species_latin,
self_intersection,
overlap,
outside_country,
check_200_trees,
check_duplicate_polygons,
needle_shape,
total_nr_geometric_errors,
'False' AS edit_confirmation

FROM akvo_tree_registration_areas_integrated
WHERE
akvo_tree_registration_areas_integrated.submission >= CURRENT_DATE - INTERVAL '2 day'  -- Only recent submissions
AND NOT EXISTS (
SELECT 1
FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_integrated.identifier_akvo
AND akvo_tree_registration_areas_updated.edit_confirmation = False);

-- Delete rows that were removed from the INTEGRATED table.
DELETE FROM akvo_tree_registration_areas_updated
WHERE NOT EXISTS (SELECT 1 FROM akvo_tree_registration_areas_integrated
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_integrated.identifier_akvo);'''



# Make a copy of the table UPDATES to create a seperate EDIT table to do the editing (only need to do this once!
# Once this is done, the table will only be updated)
# Add also some additional columns to this newly created table.
# This command creates and fills the table from UPDATED. So it basically creates a full copy
create_a1_edit = '''CREATE TABLE IF NOT EXISTS akvo_tree_registration_areas_edits AS (SELECT
identifier_akvo,
display_name,
device_id,
instance,
submission,
submission_year,
submissiontime,
updated_at,
submitter,
modifiedat,
akvo_form_version,
data_source,
form_source,
country,
test,
organisation,
contract_number,
id_planting_site,
land_title,
name_village,
name_region,
name_owner,
photo_owner,
gender_owner,
objective_site,
site_preparation,
planting_technique,
planting_system,
remark,
nr_trees_option,
planting_date,
tree_number,
estimated_area,
calc_area,
lat_y,
lon_x,
number_coord_polygon,
centroid_coord,
polygon,
re_mapped_by_partner,
multipoint,
confirm_plant_location_own_land,
one_multiple_planting_sites,
nr_trees_given_away,
nr_trees_received,
url_photo_receiver_trees,
location_house_tree_receiver,
confirm_planting_location,
url_signature_tree_receiver,
total_number_trees_received,
check_ownership_trees,
gender_tree_receiver,
name_tree_receiver,
species_latin,
self_intersection,
overlap,
outside_country,
check_200_trees,
check_duplicate_polygons,
needle_shape,
total_nr_geometric_errors,
edit_confirmation

FROM akvo_tree_registration_areas_updated);

ALTER TABLE akvo_tree_registration_areas_edits
ADD COLUMN IF NOT EXISTS fid SERIAL PRIMARY KEY, -- Needed in order to be able to edit in QGIS
ADD COLUMN IF NOT EXISTS edit_confirmation BOOLEAN, -- QGIS confirmation to process the edits
ADD COLUMN IF NOT EXISTS chloris_uploaded BOOLEAN, -- Confirmation that the polygons are uploaded to Chloris
ADD COLUMN IF NOT EXISTS kanop_uploaded BOOLEAN, -- Confirmation that the polygons are uploaded to Kanop
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ, -- Add colum if not exits (in case table was already created)
ADD COLUMN IF NOT EXISTS re_mapped_by_partner TEXT; -- Add colum if not exits (in case table was already created)'''

conn.commit()


#  Insert new rows from the UPDATE table into the EDIT table. We DO NOT want to delete the EDITS by a full refresh, so only insertions and updated (and deletes) are allowed.
create_a1_integrate_new_data = '''
INSERT INTO akvo_tree_registration_areas_edits (identifier_akvo, display_name, device_id, instance, submission, submission_year, submissiontime, updated_at, submitter, modifiedat, akvo_form_version, data_source, form_source, country, test, organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, photo_owner, gender_owner, objective_site, site_preparation, planting_technique, planting_system, remark, nr_trees_option, planting_date, tree_number, estimated_area, calc_area, lat_y, lon_x, number_coord_polygon, centroid_coord, polygon, re_mapped_by_partner, multipoint, confirm_plant_location_own_land, one_multiple_planting_sites, nr_trees_given_away, nr_trees_received, url_photo_receiver_trees, location_house_tree_receiver, confirm_planting_location, url_signature_tree_receiver, total_number_trees_received, check_ownership_trees, gender_tree_receiver, name_tree_receiver, species_latin, self_intersection, overlap, outside_country, check_200_trees, check_duplicate_polygons, needle_shape, total_nr_geometric_errors, edit_confirmation)

SELECT
identifier_akvo,
display_name,
device_id,
instance,
submission,
submission_year,
submissiontime,
updated_at,
submitter,
modifiedat,
akvo_form_version,
data_source,
form_source,
country,
test,
organisation,
contract_number,
id_planting_site,
land_title,
name_village,
name_region,
name_owner,
photo_owner,
gender_owner,
objective_site,
site_preparation,
planting_technique,
planting_system,
remark,
nr_trees_option,
planting_date,
tree_number,
estimated_area,
calc_area,
lat_y,
lon_x,
number_coord_polygon,
centroid_coord,
polygon,
re_mapped_by_partner,
multipoint,
confirm_plant_location_own_land,
one_multiple_planting_sites,
nr_trees_given_away,
nr_trees_received,
url_photo_receiver_trees,
location_house_tree_receiver,
confirm_planting_location,
url_signature_tree_receiver,
total_number_trees_received,
check_ownership_trees,
gender_tree_receiver,
name_tree_receiver,
species_latin,
self_intersection,
overlap,
outside_country,
check_200_trees,
check_duplicate_polygons,
needle_shape,
total_nr_geometric_errors,
'False' AS edit_confirmation

FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_updated.submission >= CURRENT_DATE - INTERVAL '2 day'  -- Only recent submissions
AND NOT EXISTS (SELECT identifier_akvo FROM akvo_tree_registration_areas_edits
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_edits.identifier_akvo
AND akvo_tree_registration_areas_updated.edit_confirmation = False);

-- Delete rows that were removed from the EDITS table.
DELETE FROM akvo_tree_registration_areas_edits
WHERE NOT EXISTS (SELECT 1 FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_edits.identifier_akvo);'''

conn.commit()


# Update EDITS table with geometric error detection results. This is done from the UPDATED table.
# The geometric analysis is done AFTER the run of the (this) dashboard script.
# Needs to be done aditional to previous update of edits table because that one updates only the NEW submissions.
# This table is updated after a second run of the DAshboard script: In the frist run, the resultss from EDITS are tranfered to the UPDATED table.
# Then the GEOMETRIC ERROR SCRIPT runs over the UPDATED table and analysis the results.
# Afther this run of the GEOMETRIC ERROR SCRIPT, a second run of the DASHBOARD script is needed to transfer the results of the GEOMETRIC ERROR SCRIPT from the UPDATED table into the EDIT table (see below)
create_a1_updates_from_updated_to_edits_geometric_corr = '''
--First set all the former values to NULL because -if not- the former values will remain.
UPDATE akvo_tree_registration_areas_edits
SET
self_intersection = NULL,
overlap = NULL,
outside_country = NULL,
check_200_trees = NULL,
check_duplicate_polygons = NULL,
needle_shape = NULL,
total_nr_geometric_errors = NULL;

UPDATE akvo_tree_registration_areas_edits
SET
self_intersection = akvo_tree_registration_areas_updated.self_intersection,
overlap = akvo_tree_registration_areas_updated.overlap,
outside_country = akvo_tree_registration_areas_updated.outside_country,
check_200_trees = akvo_tree_registration_areas_updated.check_200_trees,
check_duplicate_polygons = akvo_tree_registration_areas_updated.check_duplicate_polygons,
needle_shape = akvo_tree_registration_areas_updated.needle_shape,
total_nr_geometric_errors = akvo_tree_registration_areas_updated.total_nr_geometric_errors
FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_edits.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo;'''

conn.commit()


# Insert the edits back again in the UPDATE TABLE so that they become visible for the dashboard and also available for prosessing by KANOP AND CHLORIS
create_a1_edit_integration = '''UPDATE akvo_tree_registration_areas_updated
SET
contract_number = akvo_tree_registration_areas_edits.contract_number,
organisation = akvo_tree_registration_areas_edits.organisation,
polygon = akvo_tree_registration_areas_edits.polygon,
country = akvo_tree_registration_areas_edits.country,
test = akvo_tree_registration_areas_edits.test,
id_planting_site = akvo_tree_registration_areas_edits.id_planting_site,
land_title = akvo_tree_registration_areas_edits.land_title,
name_village = akvo_tree_registration_areas_edits.name_village,
name_region = akvo_tree_registration_areas_edits.name_region,
name_owner = akvo_tree_registration_areas_edits.name_owner,
gender_owner = akvo_tree_registration_areas_edits.gender_owner,
objective_site = akvo_tree_registration_areas_edits.objective_site,
site_preparation = akvo_tree_registration_areas_edits.site_preparation,
planting_technique = akvo_tree_registration_areas_edits.planting_technique,
planting_system = akvo_tree_registration_areas_edits.planting_system,
planting_date = akvo_tree_registration_areas_edits.planting_date,
tree_number = akvo_tree_registration_areas_edits.tree_number,
remark = akvo_tree_registration_areas_edits.remark,
nr_trees_option = akvo_tree_registration_areas_edits.nr_trees_option,
centroid_coord = akvo_tree_registration_areas_edits.centroid_coord,
confirm_plant_location_own_land = akvo_tree_registration_areas_edits.confirm_plant_location_own_land,
one_multiple_planting_sites = akvo_tree_registration_areas_edits.one_multiple_planting_sites,
nr_trees_given_away = akvo_tree_registration_areas_edits.nr_trees_given_away,
nr_trees_received = akvo_tree_registration_areas_edits.nr_trees_received,
url_photo_receiver_trees = akvo_tree_registration_areas_edits.url_photo_receiver_trees,
location_house_tree_receiver = akvo_tree_registration_areas_edits.location_house_tree_receiver,
confirm_planting_location = akvo_tree_registration_areas_edits.confirm_planting_location,
url_signature_tree_receiver = akvo_tree_registration_areas_edits.url_signature_tree_receiver,
total_number_trees_received = akvo_tree_registration_areas_edits.total_number_trees_received,
check_ownership_trees = akvo_tree_registration_areas_edits.check_ownership_trees,
gender_tree_receiver = akvo_tree_registration_areas_edits.gender_tree_receiver,
name_tree_receiver = akvo_tree_registration_areas_edits.name_tree_receiver,
species_latin = akvo_tree_registration_areas_edits.species_latin,
edit_confirmation = akvo_tree_registration_areas_edits.edit_confirmation

FROM akvo_tree_registration_areas_edits
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_edits.identifier_akvo
AND akvo_tree_registration_areas_edits.edit_confirmation = TRUE;
--(AND NOT chloris_uploaded = TRUE
--OR kanop_uploaded = TRUE);

-- Delete rows that were removed in the EDIT table also from the UPDATE table.
DELETE FROM akvo_tree_registration_areas_updated
WHERE NOT EXISTS (SELECT 1 FROM akvo_tree_registration_areas_edits
WHERE akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_areas_edits.identifier_akvo);'''

conn.commit()


create_a1_remote_sensing_results = '''CREATE TABLE superset_ecosia_kanop_chloris_results AS (SELECT
t1.identifier_akvo,
t1.organisation,
t1.id_planting_site,
t1.contract_number,
t1.year_of_analisis,
t1.chloris_above_ground_dry_biomass,
t2.kanop_above_ground_living_biomass AS kanop_above_ground_dry_biomass

FROM (SELECT
identifier_akvo,
organisation,
contract_number AS contract_number,
id_planting_site,
year_of_analisis AS year_of_analisis,
forest_agb_stock_per_year_mt AS chloris_above_ground_dry_biomass
FROM superset_ecosia_CHLORIS_polygon_results) t1

JOIN

(SELECT
identifier_akvo,
name_project AS contract_number,
'unknown' AS id_planting_site,
year_of_analisis AS year_of_analisis,
request_measurement_date AS year_analysis,
livingabovegroundbiomass_present AS kanop_above_ground_living_biomass
FROM superset_ecosia_kanop_polygon_level_1_moment) t2
ON t1.identifier_akvo = t2.identifier_akvo
and t1.year_of_analisis = t2.year_of_analisis);'''

conn.commit()

# Update the EDITS table with KANOP AND CHLORIS analysis so that we know which sites were already processed by KANOP AND CHLORIS (and don't need to be edited or uploaded again)
create_a1_remote_sensing_update = '''UPDATE akvo_tree_registration_areas_edits
SET
chloris_uploaded = True,
kanop_uploaded = True

FROM superset_ecosia_kanop_chloris_results
WHERE akvo_tree_registration_areas_edits.identifier_akvo = superset_ecosia_kanop_chloris_results.identifier_akvo;'''

conn.commit()


# Works well
create_a2_akvo = '''CREATE TABLE calc_tab_monitoring_calculations_per_site_merged_akvo AS

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
'AKVO' AS data_source,
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
'AKVO' AS data_source,
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
'AKVO' AS data_source,
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
'AKVO' AS data_source,
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
'AKVO' AS data_source,
'Audit' AS procedure,
'Tree count' AS data_collection_method,


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
'AKVO' AS data_source,
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
'AKVO' AS data_source,
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


# Works well
create_a2_odk = '''CREATE TABLE calc_tab_monitoring_calculations_per_site_merged_odk AS
WITH plot_dates_monitorings_odk AS (SELECT
odk_tree_monitoring_main.ecosia_site_id AS ecosia_site_id,
odk_tree_monitoring_main.submissionid_odk, -- This instance is now only to relate main monitorings with its monitoring repeats
odk_tree_monitoring_main.test AS test_monitoring,
odk_tree_registration_main.id_planting_site,
odk_tree_monitoring_main.submission_date AS monitoring_submission,

-- Classify the options is needed to prevent seperate groups
CASE
WHEN odk_tree_monitoring_main.monitoring_method = 'pcq_method'
THEN 'PCQ'
WHEN odk_tree_monitoring_main.monitoring_method = 'counting_method'
THEN 'Tree count'
WHEN odk_tree_monitoring_main.monitoring_method = 'own_method'
THEN 'Own method'
ELSE 'Unknown'
END AS method_selection,

'monitoring_data' AS procedure,

CASE
WHEN odk_tree_registration_main.planting_date NOTNULL
THEN TO_DATE(odk_tree_registration_main.planting_date, 'YYYY-MM-DD')
ELSE TO_DATE(akvo_tree_registration_areas_updated.planting_date, 'YYYY-MM-DD')
END AS planting_date

FROM odk_tree_monitoring_main
LEFT JOIN odk_tree_registration_main
ON odk_tree_monitoring_main.ecosia_site_id = odk_tree_registration_main.ecosia_site_id
LEFT JOIN akvo_tree_registration_areas_updated
ON akvo_tree_registration_areas_updated.identifier_akvo = odk_tree_monitoring_main.ecosia_site_id),


-- Calculate time differences between planting date and audits/monitorings and give them a label strata (on instance level)
table_label_strata AS (SELECT
plot_dates_monitorings_odk.ecosia_site_id,
plot_dates_monitorings_odk.submissionid_odk,
plot_dates_monitorings_odk.test_monitoring,
plot_dates_monitorings_odk.method_selection,
plot_dates_monitorings_odk.procedure,
plot_dates_monitorings_odk.planting_date,
plot_dates_monitorings_odk.monitoring_submission AS submission,
plot_dates_monitorings_odk.monitoring_submission - planting_date AS difference_days_reg_monitoring,
CAST((plot_dates_monitorings_odk.monitoring_submission - planting_date)*1.0/365 * 1.0 AS DECIMAL(7,1)) AS difference_years_reg_monitoring,

-- Calculate label-strata. Include rule that if a monitoring and a registration is carried out on the same day (= 0 days difference)
-- the label-strata for that monitoring instance receives a value 180 (and not 0)
CASE
WHEN CEILING((plot_dates_monitorings_odk.monitoring_submission - planting_date)*1.0/180)*180 > 0
THEN CEILING((plot_dates_monitorings_odk.monitoring_submission - planting_date)*1.0/180)*180
ELSE 180
END AS label_strata

FROM plot_dates_monitorings_odk
order by ecosia_site_id),


-- List number of PCQ samples of odk monitorings
list_pcq_samples AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk),


-- Calculate number of PCQ samples of odk monitorings
count_pcq_samples AS (SELECT
list_pcq_samples.submissionid_odk,
table_label_strata.label_strata,
COUNT(list_pcq_samples.submissionid_odk) AS number_pcq_samples
FROM list_pcq_samples
LEFT JOIN table_label_strata
ON table_label_strata.submissionid_odk = list_pcq_samples.submissionid_odk
GROUP BY
list_pcq_samples.submissionid_odk,
table_label_strata.label_strata),


-- Get all PCQ MONITORING DISTANCE values into 1 column in order to calculate the average
merge_pcq_monitoring_dist AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_distance_q1 AS pcq_results_merged_monitoring
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_distance_q2 AS pcq_results_merged_monitoring
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_distance_q3 AS pcq_results_merged_monitoring
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_distance_q4 AS pcq_results_merged_monitoring
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk),


-- Get all PCQ MONITORIG HEIGHT values into 1 column in order to calculate the average
merge_pcq_monitoring_hgt AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_height_q1 AS pcq_results_merged_monitoring_hgt
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_height_q2 AS pcq_results_merged_monitoring_hgt
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_height_q3 AS pcq_results_merged_monitoring_hgt
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_pcq.tree_height_q4 AS pcq_results_merged_monitoring_hgt
FROM odk_tree_monitoring_main
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk),


-- Calculate average DISTANCES from PCQ monitoring values
pcq_monitoring_avg_dist AS (SELECT
merge_pcq_monitoring_dist.ecosia_site_id,
table_label_strata.label_strata,
AVG(pcq_results_merged_monitoring) AS pcq_results_merged_monitoring
FROM merge_pcq_monitoring_dist
LEFT JOIN table_label_strata
ON merge_pcq_monitoring_dist.submissionid_odk = table_label_strata.submissionid_odk
WHERE merge_pcq_monitoring_dist.pcq_results_merged_monitoring > 0
AND merge_pcq_monitoring_dist.pcq_results_merged_monitoring < 200
GROUP BY merge_pcq_monitoring_dist.ecosia_site_id,
table_label_strata.label_strata),


-- Calculate average HEIGHT from PCQ monitoring values
pcq_monitoring_avg_hgt AS (SELECT
merge_pcq_monitoring_hgt.ecosia_site_id,
table_label_strata.label_strata,
AVG(pcq_results_merged_monitoring_hgt) AS pcq_results_merged_monitoring_hgt
FROM merge_pcq_monitoring_hgt
LEFT JOIN table_label_strata
ON merge_pcq_monitoring_hgt.submissionid_odk = table_label_strata.submissionid_odk
WHERE merge_pcq_monitoring_hgt.pcq_results_merged_monitoring_hgt > 0
AND merge_pcq_monitoring_hgt.pcq_results_merged_monitoring_hgt < 100
GROUP BY merge_pcq_monitoring_hgt.ecosia_site_id,
table_label_strata.label_strata),

-- Calculate AVERAGE tree count results for the MONITORING COUNTS OF ODK (in case multiple COUNTS are carried out in the same label_strata)
count_monitoring_avg_count AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
--odk_tree_monitoring_count_trees.submissionid_odk,
table_label_strata.label_strata,

SUM(odk_tree_monitoring_count_trees.count_species) AS nr_trees_monitored

FROM odk_tree_monitoring_count_trees
LEFT JOIN table_label_strata
ON odk_tree_monitoring_count_trees.submissionid_odk = table_label_strata.submissionid_odk
LEFT JOIN odk_tree_monitoring_main
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_count_trees.submissionid_odk
GROUP BY
--odk_tree_monitoring_count_trees.submissionid_odk,
odk_tree_monitoring_main.ecosia_site_id,
--odk_tree_monitoring_count_trees.count_species,
table_label_strata.label_strata),

-- Calculate AVERAGE tree count results for the MONITORING OWN METHODS OF ODK
count_monitoring_avg_own_method AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_own_method.submissionid_odk,
table_label_strata.label_strata,

SUM(odk_tree_monitoring_own_method.tree_number_own_method) AS nr_trees_monitored

FROM odk_tree_monitoring_own_method
LEFT JOIN table_label_strata
ON odk_tree_monitoring_own_method.submissionid_odk = table_label_strata.submissionid_odk
LEFT JOIN odk_tree_monitoring_main
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_own_method.submissionid_odk
GROUP BY
odk_tree_monitoring_own_method.submissionid_odk,
odk_tree_monitoring_main.ecosia_site_id,
table_label_strata.label_strata),

--List the enumerators. Some monitoring submissions were done at the same time by different submittors.
--We need to bundle them into 1 column so they don't appear as seperate submissions in the result table
submittors_monitoring AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
table_label_strata.label_strata,
STRING_AGG(odk_tree_monitoring_main.username,' | ') AS submitter
FROM odk_tree_monitoring_main
JOIN table_label_strata
ON odk_tree_monitoring_main.submissionid_odk = table_label_strata.submissionid_odk
GROUP BY odk_tree_monitoring_main.ecosia_site_id,
table_label_strata.label_strata),


-- Group the "site impressions" from ODK MONITORINGS on label_strata level + identifier. This enables
-- to add site impressions to the each label strata (for a specific monitoring period)
site_impressions_monitoring AS (SELECT
odk_tree_monitoring_main.ecosia_site_id,
table_label_strata.label_strata,
STRING_AGG(odk_tree_monitoring_main.overall_observation_site,' | ') AS site_impressions
FROM odk_tree_monitoring_main
JOIN table_label_strata
ON odk_tree_monitoring_main.submissionid_odk = table_label_strata.submissionid_odk
GROUP BY odk_tree_monitoring_main.ecosia_site_id,
table_label_strata.label_strata),


-- Sub CTE table to calculate PCQ MONITORING OF ODK results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this temporary CTE table is more easy.
calc_interm_results_tree_numbers_pcq_monitoring_odk AS (
SELECT
odk_tree_monitoring_main.ecosia_site_id AS identifier_akvo,

CASE
WHEN odk_tree_monitoring_main.contract_number_monitoring NOTNULL AND odk_tree_registration_main.id_planting_site NOTNULL AND odk_tree_registration_main.name_owner NOTNULL
THEN CONCAT(odk_tree_monitoring_main.contract_number_monitoring,' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.name_owner)
WHEN odk_tree_monitoring_main.contract_number_monitoring ISNULL OR odk_tree_registration_main.id_planting_site ISNULL OR odk_tree_registration_main.name_owner ISNULL
THEN CONCAT(akvo_tree_registration_areas_updated.contract_number,' - ', akvo_tree_registration_areas_updated.id_planting_site, ' - ', akvo_tree_registration_areas_updated.name_owner)
ELSE 'Display name cannot be generated because too many unknowns'
END AS displayname,

CASE
WHEN odk_tree_monitoring_main.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN odk_tree_monitoring_main.test = 'valid_data'
THEN 'This is real, valid data'
END AS test,

CASE
WHEN odk_tree_monitoring_main.country NOTNULL
THEN LOWER(odk_tree_monitoring_main.country)
WHEN akvo_tree_registration_areas_updated.country NOTNULL
THEN LOWER(akvo_tree_registration_areas_updated.country)
ELSE 'country unknown'
END AS country,

LOWER(odk_tree_monitoring_main.organisation) AS organisation,
odk_tree_monitoring_main.username,

CASE
WHEN odk_tree_registration_main.contract_number NOTNULL
THEN odk_tree_registration_main.contract_number
ELSE akvo_tree_registration_areas_updated.contract_number
END AS contract_number,

CASE
WHEN odk_tree_registration_main.id_planting_site NOTNULL
THEN odk_tree_registration_main.id_planting_site
ELSE akvo_tree_registration_areas_updated.id_planting_site
END AS id_planting_site,

CASE
WHEN odk_tree_registration_main.calc_area NOTNULL
THEN odk_tree_registration_main.calc_area
ELSE akvo_tree_registration_areas_updated.calc_area
END AS calc_area,

CASE
WHEN odk_tree_registration_main.tree_number NOTNULL
THEN odk_tree_registration_main.tree_number
ELSE akvo_tree_registration_areas_updated.tree_number
END AS registered_tree_number,

'ODK' AS data_source,
'Monitoring' AS procedure,
'PCQ' AS data_collection_method,

ROUND(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2) AS avg_tree_distance_m,

ROUND((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000),0) AS avg_tree_density,

CASE
WHEN odk_tree_registration_main.calc_area NOTNULL
THEN ROUND((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * odk_tree_registration_main.calc_area,0)
ELSE ROUND((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * akvo_tree_registration_areas_updated.calc_area,0)
END AS nr_trees_monitored,

count_pcq_samples.number_pcq_samples,

CASE
	WHEN odk_tree_registration_main.planting_date NOTNULL
	THEN odk_tree_registration_main.planting_date
	ELSE akvo_tree_registration_areas_updated.planting_date
	END AS planting_date,

MAX(table_label_strata.submission) AS latest_monitoring_submission,

MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

CASE
WHEN odk_tree_registration_main.calc_area NOTNULL AND odk_tree_registration_main.tree_number NOTNULL
THEN ROUND(((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * odk_tree_registration_main.calc_area)/NULLIF(odk_tree_registration_main.tree_number,0)*100,2)
WHEN odk_tree_registration_main.calc_area ISNULL OR odk_tree_registration_main.tree_number ISNULL
THEN ROUND(((1/(NULLIF(POWER(pcq_monitoring_avg_dist.pcq_results_merged_monitoring,2),0))*10000) * akvo_tree_registration_areas_updated.calc_area)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,2)
ELSE NULL
END AS perc_trees_survived,

ROUND(pcq_monitoring_avg_hgt.pcq_results_merged_monitoring_hgt,2) AS avg_tree_height,

site_impressions_monitoring.site_impressions

FROM odk_tree_monitoring_main
LEFT JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk
LEFT JOIN odk_tree_registration_main
ON odk_tree_registration_main.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
LEFT JOIN table_label_strata
ON odk_tree_monitoring_main.submissionid_odk = table_label_strata.submissionid_odk
LEFT JOIN akvo_tree_registration_areas_updated
ON odk_tree_monitoring_main.ecosia_site_id = akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN pcq_monitoring_avg_dist
ON pcq_monitoring_avg_dist.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND pcq_monitoring_avg_dist.label_strata = table_label_strata.label_strata
LEFT JOIN pcq_monitoring_avg_hgt
ON pcq_monitoring_avg_hgt.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND pcq_monitoring_avg_hgt.label_strata = table_label_strata.label_strata
--AND count_pcq_samples.label_strata = table_label_strata.label_strata
LEFT JOIN count_pcq_samples
ON count_pcq_samples.submissionid_odk = odk_tree_monitoring_main.submissionid_odk
LEFT JOIN submittors_monitoring
ON submittors_monitoring.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND submittors_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_monitoring
ON site_impressions_monitoring.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND site_impressions_monitoring.label_strata = table_label_strata.label_strata

WHERE odk_tree_monitoring_main.monitoring_method = 'pcq_method'

GROUP BY
table_label_strata.label_strata,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.country,
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_main.test,
odk_tree_registration_main.organisation,
	odk_tree_monitoring_main.organisation,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.name_owner,
site_impressions_monitoring.site_impressions,
pcq_monitoring_avg_hgt.pcq_results_merged_monitoring_hgt,
odk_tree_monitoring_main.contract_number_monitoring,
odk_tree_registration_main.contract_number,
odk_tree_registration_main.calc_area,
odk_tree_registration_main.tree_number,
count_pcq_samples.number_pcq_samples,
odk_tree_registration_main.planting_date,
odk_tree_registration_main.contract_number,
odk_tree_registration_main.id_planting_site,
odk_tree_registration_main.name_owner,
odk_tree_registration_main.country,
odk_tree_monitoring_main.country,
odk_tree_registration_main.submitter,
odk_tree_monitoring_main.username,
akvo_tree_registration_areas_updated.tree_number,
pcq_monitoring_avg_dist.pcq_results_merged_monitoring,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.planting_date),

-- Sub CTE table to calculate COUNTS of MONITORING results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_count_monitoring_odk AS (SELECT
odk_tree_monitoring_main.ecosia_site_id AS identifier_akvo,

CASE
WHEN odk_tree_monitoring_main.contract_number_monitoring NOTNULL AND odk_tree_registration_main.id_planting_site NOTNULL AND odk_tree_registration_main.name_owner NOTNULL
THEN CONCAT(odk_tree_monitoring_main.contract_number_monitoring,' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.name_owner)
WHEN odk_tree_monitoring_main.contract_number_monitoring ISNULL OR odk_tree_registration_main.id_planting_site ISNULL OR odk_tree_registration_main.name_owner ISNULL
THEN CONCAT(akvo_tree_registration_areas_updated.contract_number,' - ', akvo_tree_registration_areas_updated.id_planting_site, ' - ', akvo_tree_registration_areas_updated.name_owner)
ELSE 'Display name cannot be generated because too many unknowns'
END AS displayname,

CASE
WHEN odk_tree_monitoring_main.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN odk_tree_monitoring_main.test = 'valid_data'
THEN 'This is real, valid data'
END AS test,

CASE
WHEN odk_tree_monitoring_main.country NOTNULL
THEN LOWER(odk_tree_monitoring_main.country)
WHEN akvo_tree_registration_areas_updated.country NOTNULL
THEN LOWER(akvo_tree_registration_areas_updated.country)
ELSE 'country unknown'
END AS country,

LOWER(odk_tree_monitoring_main.organisation) AS organisation,
odk_tree_monitoring_main.username,
CASE
WHEN odk_tree_registration_main.contract_number NOTNULL
THEN odk_tree_registration_main.contract_number
ELSE akvo_tree_registration_areas_updated.contract_number
END AS contract_number,

CASE
WHEN odk_tree_registration_main.id_planting_site NOTNULL
THEN odk_tree_registration_main.id_planting_site
ELSE akvo_tree_registration_areas_updated.id_planting_site
END AS id_planting_site,

CASE
WHEN odk_tree_registration_main.calc_area NOTNULL
THEN odk_tree_registration_main.calc_area
ELSE akvo_tree_registration_areas_updated.calc_area
END AS calc_area,

CASE
WHEN odk_tree_registration_main.tree_number NOTNULL
THEN odk_tree_registration_main.tree_number
ELSE akvo_tree_registration_areas_updated.tree_number
END AS registered_tree_number,
'ODK' AS data_source,
'Monitoring' AS procedure,
'Tree count' AS data_collection_method,

CASE
WHEN count_monitoring_avg_count.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area NOTNULL
THEN ROUND(100/NULLIF(SQRT(count_monitoring_avg_count.nr_trees_monitored/NULLIF(odk_tree_registration_main.calc_area,0)),0),2)
WHEN count_monitoring_avg_count.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area ISNULL
THEN ROUND(100/NULLIF(SQRT(count_monitoring_avg_count.nr_trees_monitored/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2)
END AS avg_monitored_tree_distance,

CASE
WHEN count_monitoring_avg_count.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area NOTNULL
THEN ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(odk_tree_registration_main.calc_area,0),0)
WHEN count_monitoring_avg_count.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area ISNULL
THEN ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.calc_area,0),0)
END AS avg_monitored_tree_density,

CASE
WHEN count_monitoring_avg_count.nr_trees_monitored NOTNULL
THEN ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored),0)
ELSE 0
END AS nr_trees_monitored,

0 AS nr_samples_pcq_monitoring,

CASE
WHEN odk_tree_registration_main.planting_date NOTNULL
THEN odk_tree_registration_main.planting_date
ELSE akvo_tree_registration_areas_updated.planting_date
END AS planting_date,


MAX(table_label_strata.submission) AS latest_monitoring_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

CASE
WHEN odk_tree_registration_main.tree_number NOTNULL
THEN ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(odk_tree_registration_main.tree_number,0)*100,2)
ELSE ROUND(AVG(count_monitoring_avg_count.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,2)
END AS perc_trees_survived,

ROUND(AVG(odk_tree_monitoring_count_trees.avg_tree_height_species)::numeric,2) AS avg_tree_height,


site_impressions_monitoring.site_impressions

FROM odk_tree_monitoring_main
LEFT JOIN count_monitoring_avg_count
ON odk_tree_monitoring_main.ecosia_site_id = count_monitoring_avg_count.ecosia_site_id
LEFT JOIN odk_tree_registration_main
ON odk_tree_monitoring_main.ecosia_site_id = odk_tree_registration_main.ecosia_site_id
LEFT JOIN akvo_tree_registration_areas_updated
ON odk_tree_monitoring_main.ecosia_site_id = akvo_tree_registration_areas_updated.identifier_akvo
LEFT JOIN table_label_strata
ON odk_tree_monitoring_main.submissionid_odk = table_label_strata.submissionid_odk
LEFT JOIN odk_tree_monitoring_count_trees
ON odk_tree_monitoring_count_trees.submissionid_odk = odk_tree_monitoring_main.submissionid_odk
LEFT JOIN submittors_monitoring
ON submittors_monitoring.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND submittors_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_monitoring
ON site_impressions_monitoring.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND site_impressions_monitoring.label_strata = table_label_strata.label_strata


WHERE odk_tree_monitoring_main.monitoring_method = 'counting_method'


GROUP BY
table_label_strata.label_strata,
odk_tree_monitoring_main.contract_number_monitoring,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.name_owner,
odk_tree_monitoring_main.country,
akvo_tree_registration_areas_updated.country,
odk_tree_monitoring_main.username,
odk_tree_monitoring_main.organisation,
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_main.test,
akvo_tree_registration_areas_updated.planting_date,
odk_tree_registration_main.calc_area,
akvo_tree_registration_areas_updated.calc_area,
akvo_tree_registration_areas_updated.tree_number,
odk_tree_registration_main.tree_number,
odk_tree_registration_main.planting_date,
odk_tree_registration_main.organisation,
submittors_monitoring.submitter,
site_impressions_monitoring.site_impressions,
odk_tree_registration_main.contract_number,
odk_tree_registration_main.id_planting_site,
odk_tree_registration_main.country,
odk_tree_registration_main.name_owner,
display_name,
count_monitoring_avg_count.nr_trees_monitored),

-- ---------------

-- Sub CTE table to calculate OWN METHOD of MONITORING results with CASE more easy and transparent. If we would do this in a subquery it results in
-- a complex issues of multiple rows combined with grouping problems. This is why this intermediary table is more easy.
calc_interm_results_tree_numbers_own_method_monitoring_odk AS (SELECT
odk_tree_monitoring_main.ecosia_site_id AS identifier_akvo,

CASE
WHEN odk_tree_monitoring_main.contract_number_monitoring NOTNULL AND odk_tree_registration_main.id_planting_site NOTNULL AND odk_tree_registration_main.name_owner NOTNULL
THEN CONCAT(odk_tree_monitoring_main.contract_number_monitoring,' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.name_owner)
WHEN odk_tree_monitoring_main.contract_number_monitoring ISNULL OR odk_tree_registration_main.id_planting_site ISNULL OR odk_tree_registration_main.name_owner ISNULL
THEN CONCAT(akvo_tree_registration_areas_updated.contract_number,' - ', akvo_tree_registration_areas_updated.id_planting_site, ' - ', akvo_tree_registration_areas_updated.name_owner)
ELSE 'Display name cannot be generated because too many unknowns'
END AS displayname,

CASE
WHEN odk_tree_monitoring_main.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN odk_tree_monitoring_main.test = 'valid_data'
THEN 'This is real, valid data'
END AS test,

CASE
WHEN odk_tree_monitoring_main.country NOTNULL
THEN LOWER(odk_tree_monitoring_main.country)
WHEN akvo_tree_registration_areas_updated.country NOTNULL
THEN LOWER(akvo_tree_registration_areas_updated.country)
ELSE 'country unknown'
END AS country,

LOWER(odk_tree_monitoring_main.organisation) AS organisation,
odk_tree_monitoring_main.username,
CASE
WHEN odk_tree_registration_main.contract_number NOTNULL
THEN odk_tree_registration_main.contract_number
ELSE akvo_tree_registration_areas_updated.contract_number
END AS contract_number,

CASE
WHEN odk_tree_registration_main.id_planting_site NOTNULL
THEN odk_tree_registration_main.id_planting_site
ELSE akvo_tree_registration_areas_updated.id_planting_site
END AS id_planting_site,

CASE
WHEN odk_tree_registration_main.calc_area NOTNULL
THEN odk_tree_registration_main.calc_area
ELSE akvo_tree_registration_areas_updated.calc_area
END AS calc_area,

CASE
WHEN odk_tree_registration_main.tree_number NOTNULL
THEN odk_tree_registration_main.tree_number
ELSE akvo_tree_registration_areas_updated.tree_number
END AS registered_tree_number,

'ODK' AS data_source,
'Monitoring' AS procedure,
'Own method' AS data_collection_method,

CASE
WHEN count_monitoring_avg_own_method.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area NOTNULL
THEN ROUND(100/NULLIF(SQRT(count_monitoring_avg_own_method.nr_trees_monitored/NULLIF(odk_tree_registration_main.calc_area,0)),0),2)
WHEN count_monitoring_avg_own_method.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area ISNULL
THEN ROUND(100/NULLIF(SQRT(count_monitoring_avg_own_method.nr_trees_monitored/NULLIF(akvo_tree_registration_areas_updated.calc_area,0)),0),2)
END AS avg_monitored_tree_distance,

CASE
WHEN count_monitoring_avg_own_method.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area NOTNULL
THEN ROUND(AVG(count_monitoring_avg_own_method.nr_trees_monitored)/NULLIF(odk_tree_registration_main.calc_area,0),0)
WHEN count_monitoring_avg_own_method.nr_trees_monitored NOTNULL AND odk_tree_registration_main.calc_area ISNULL
THEN ROUND(AVG(count_monitoring_avg_own_method.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.calc_area,0),0)
END AS avg_monitored_tree_density,

CASE
WHEN count_monitoring_avg_own_method.nr_trees_monitored NOTNULL
THEN ROUND(AVG(count_monitoring_avg_own_method.nr_trees_monitored),0)
ELSE 0
END AS nr_trees_monitored,

0 AS nr_samples_pcq_monitoring,

CASE
WHEN odk_tree_registration_main.planting_date NOTNULL
THEN odk_tree_registration_main.planting_date
ELSE akvo_tree_registration_areas_updated.planting_date
END AS planting_date,

MAX(table_label_strata.submission) AS latest_monitoring_submission,
MAX(table_label_strata.difference_days_reg_monitoring) AS nr_days_registration_monitoring,
MAX(table_label_strata.difference_years_reg_monitoring) AS nr_years_registration_monitoring,
table_label_strata.label_strata,

CASE
WHEN odk_tree_registration_main.tree_number NOTNULL
THEN ROUND(AVG(count_monitoring_avg_own_method.nr_trees_monitored)/NULLIF(odk_tree_registration_main.tree_number,0)*100,2)
ELSE ROUND(AVG(count_monitoring_avg_own_method.nr_trees_monitored)/NULLIF(akvo_tree_registration_areas_updated.tree_number,0)*100,2)
END AS perc_trees_survived,

ROUND(AVG(odk_tree_monitoring_own_method.tree_height_own_method)::numeric,2) AS avg_tree_height,

site_impressions_monitoring.site_impressions

FROM odk_tree_monitoring_main
LEFT JOIN count_monitoring_avg_own_method
ON odk_tree_monitoring_main.submissionid_odk = count_monitoring_avg_own_method.submissionid_odk
LEFT JOIN odk_tree_registration_main
ON odk_tree_monitoring_main.ecosia_site_id = odk_tree_registration_main.ecosia_site_id
LEFT JOIN akvo_tree_registration_areas_updated
ON odk_tree_monitoring_main.ecosia_site_id = akvo_tree_registration_areas_updated.identifier_akvo
JOIN table_label_strata
ON odk_tree_monitoring_main.submissionid_odk = table_label_strata.submissionid_odk
LEFT JOIN submittors_monitoring
ON submittors_monitoring.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND submittors_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN site_impressions_monitoring
ON site_impressions_monitoring.ecosia_site_id = odk_tree_monitoring_main.ecosia_site_id
AND site_impressions_monitoring.label_strata = table_label_strata.label_strata
LEFT JOIN odk_tree_monitoring_own_method
ON odk_tree_monitoring_own_method.submissionid_odk = odk_tree_monitoring_main.submissionid_odk

WHERE odk_tree_monitoring_main.monitoring_method = 'own_method'

GROUP BY
table_label_strata.label_strata,
odk_tree_monitoring_main.ecosia_site_id,
akvo_tree_registration_areas_updated.planting_date,
--odk_tree_monitoring_own_method.tree_number_own_method,
odk_tree_monitoring_main.contract_number_monitoring,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.name_owner,
odk_tree_monitoring_main.country,
akvo_tree_registration_areas_updated.country,
odk_tree_monitoring_main.organisation,
odk_tree_monitoring_main.username,
akvo_tree_registration_areas_updated.calc_area,
odk_tree_monitoring_main.test,
akvo_tree_registration_areas_updated.tree_number,
odk_tree_registration_main.calc_area,
odk_tree_registration_main.tree_number,
odk_tree_registration_main.planting_date,
odk_tree_registration_main.organisation,
submittors_monitoring.submitter,
site_impressions_monitoring.site_impressions,
odk_tree_registration_main.contract_number,
odk_tree_registration_main.id_planting_site,
odk_tree_registration_main.country,
odk_tree_registration_main.name_owner,
display_name,
count_monitoring_avg_own_method.nr_trees_monitored),


-- Add the POLYGON results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial registered tree number). Only for polygons
registration_results_polygon_odk AS (SELECT
odk_tree_registration_main.ecosia_site_id AS identifier_akvo,
CONCAT(odk_tree_registration_main.contract_number,' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.name_owner) AS displayname,

CASE
WHEN odk_tree_registration_main.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN odk_tree_registration_main.test = 'valid_data'
THEN 'This is real, valid data'
END AS test,

LOWER(odk_tree_registration_main.country) AS country,
LOWER(odk_tree_registration_main.organisation) AS organisation,
odk_tree_registration_main.submitter,
odk_tree_registration_main.contract_number,
odk_tree_registration_main.id_planting_site,
odk_tree_registration_main.calc_area,
odk_tree_registration_main.tree_number AS registered_tree_number,
'ODK' AS data_source,
'Registration' AS procedure,
'tree registration' AS data_collection_method,
ROUND(100/NULLIF(SQRT(odk_tree_registration_main.tree_number/NULLIF(odk_tree_registration_main.calc_area,0)),0),2) AS avg_registered_tree_distance_m,
ROUND((odk_tree_registration_main.tree_number/NULLIF(odk_tree_registration_main.calc_area,0)),0) AS avg_registered_tree_density,
odk_tree_registration_main.tree_number AS nr_trees_monitored,
0 as nr_samples_pcq_registration,
odk_tree_registration_main.planting_date,
odk_tree_registration_main.submission_date AS latest_registration_submission,
0 AS nr_days_planting_date_registration,
0 AS nr_years_planting_date_registration,
0 AS label_strata,
100 AS "Percentage of trees survived",
0 AS "Average tree height (m)",
'No site impression yet because it is a first registration.' AS site_impressions

FROM odk_tree_registration_main
WHERE polygon NOTNULL),


-- Add the NON-polygon results from registrations to the upper table so that the initial registered tree numbers are integrated
-- including a '0' value for strata '0' (initial tree number). Only for NON-polygons
registration_results_non_polygon_odk AS (SELECT
odk_tree_registration_main.ecosia_site_id AS identifier_akvo,
CONCAT(odk_tree_registration_main.contract_number,' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.name_owner) AS displayname,

CASE
WHEN odk_tree_registration_main.test = 'test_data'
THEN 'This is a test, this record can be deleted.'
WHEN odk_tree_registration_main.test = 'valid_data'
THEN 'This is real, valid data'
END AS test,

LOWER(odk_tree_registration_main.country) AS country,
LOWER(odk_tree_registration_main.organisation) AS organisation,
odk_tree_registration_main.submitter,
odk_tree_registration_main.contract_number,
odk_tree_registration_main.id_planting_site,
odk_tree_registration_main.calc_area,
odk_tree_registration_main.tree_number AS registered_tree_number,
'ODK' AS data_source,
'Registration' AS procedure,
'tree registration' AS data_collection_method,
ROUND(100/NULLIF(SQRT(odk_tree_registration_main.tree_number/NULLIF(odk_tree_registration_main.calc_area,0)),0),2)
AS avg_registered_tree_distance,
ROUND((odk_tree_registration_main.tree_number/NULLIF(odk_tree_registration_main.calc_area,0)),0)
AS avg_registered_tree_density,
odk_tree_registration_main.tree_number AS nr_trees_monitored,
0 as nr_samples_pcq_registration,
odk_tree_registration_main.planting_date,
odk_tree_registration_main.submission_date AS latest_registration_submission,
0 AS nr_days_planting_date_registration,
0 AS nr_years_planting_date_registration,
0 AS label_strata,
100 AS "Percentage of trees survived",
0 AS "Average tree height (m)",
'No site impression yet because it is a first registration.' AS site_impressions

FROM odk_tree_registration_main
WHERE polygon ISNULL),

--UNION of the 3 tables (PCQ and COUNTS and OWN method) FROM MONITORING and registration data (polygon and non-polygon)
monitoring_tree_numbers_odk AS
(SELECT * FROM calc_interm_results_tree_numbers_pcq_monitoring_odk
UNION ALL
SELECT * FROM calc_interm_results_tree_numbers_count_monitoring_odk
UNION ALL
SELECT * FROM calc_interm_results_tree_numbers_own_method_monitoring_odk
UNION ALL
SELECT * FROM registration_results_polygon_odk
UNION ALL
SELECT * FROM registration_results_non_polygon_odk)

SELECT * FROM monitoring_tree_numbers_odk;'''

conn.commit()


create_a2_merge_akvo_odk = '''
--- MERGE tables from akvo monitoring results and odk monitoring results together

CREATE TABLE calc_tab_monitoring_calculations_per_site_merged
AS (

SELECT * FROM calc_tab_monitoring_calculations_per_site_merged_akvo

UNION All

SELECT * FROM calc_tab_monitoring_calculations_per_site_merged_odk
)'''

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
FROM calc_tab_monitoring_calculations_per_site_merged) s
GROUP BY s.identifier_akvo, s.id_planting_site) h

JOIN (SELECT
table_e.identifier_akvo,
table_e.label_strata AS maximum_label_strata,
table_e.perc_trees_survived,
table_e.avg_tree_height,
table_e.calc_area
FROM calc_tab_monitoring_calculations_per_site_merged table_e
JOIN (SELECT identifier_akvo, MAX(label_strata) AS max_label_strata FROM calc_tab_monitoring_calculations_per_site_merged
	 GROUP BY identifier_akvo) table_f
ON table_f.identifier_akvo = table_e.identifier_akvo
	 AND table_f.max_label_strata = table_e.label_strata) table_g
ON table_g.identifier_akvo = h.identifier_akvo)


-- Below we classify the site development by first calculating the prognosis on survival percentage and tree height in year 3, using the linear regression (y=mx+c)
-- The prognosis (survival perc in t=3 and tree height in t=3) are then used to classify the tree development on the sites
SELECT
linear_regression_field_data.identifier_akvo,
calc_tab_monitoring_calculations_per_site_merged.country,
calc_tab_monitoring_calculations_per_site_merged.organisation,
calc_tab_monitoring_calculations_per_site_merged.contract_number,
calc_tab_monitoring_calculations_per_site_merged.id_planting_site,
calc_tab_monitoring_calculations_per_site_merged.data_collection_method,
linear_regression_field_data.calc_area,
COUNT(calc_tab_monitoring_calculations_per_site_merged.label_strata) AS "number of label_strata (monitoring periods)",
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
JOIN calc_tab_monitoring_calculations_per_site_merged
ON linear_regression_field_data.identifier_akvo = calc_tab_monitoring_calculations_per_site_merged.identifier_akvo

WHERE calc_tab_monitoring_calculations_per_site_merged.label_strata > 0

GROUP BY
linear_regression_field_data.identifier_akvo,
calc_tab_monitoring_calculations_per_site_merged.country,
calc_tab_monitoring_calculations_per_site_merged.organisation,
calc_tab_monitoring_calculations_per_site_merged.contract_number,
calc_tab_monitoring_calculations_per_site_merged.id_planting_site,
calc_tab_monitoring_calculations_per_site_merged.data_collection_method,
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

-- This table is to plot the registered tree number, number of site registrations and registration dates on site level
AS WITH CTE_ranking_monitoring_audit_method AS (SELECT
calc_tab_monitoring_calculations_per_site_merged.*,
CASE
WHEN calc_tab_monitoring_calculations_per_site_merged.procedure = 'Audit' and data_collection_method = 'PCQ'
THEN 4
WHEN calc_tab_monitoring_calculations_per_site_merged.procedure = 'Audit' and data_collection_method = 'Tree count'
THEN 3
WHEN calc_tab_monitoring_calculations_per_site_merged.procedure = 'Monitoring' and data_collection_method = 'PCQ'
THEN 2
WHEN calc_tab_monitoring_calculations_per_site_merged.procedure = 'Monitoring' and data_collection_method = 'Tree count'
THEN 1
ELSE 0
END AS rank_monitoring_audit_method
FROM calc_tab_monitoring_calculations_per_site_merged),

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

-- Prepare the calculation of the percentages of native versus exotic species on contract level
CTE_percentage_exotic_native AS (
SELECT
b.contract_number,
'ODK' AS source_data,
'Registration' AS methodology,
a.species_name_latin,
a.native_exotic AS native_exotic,
a.nr_trees_per_species
FROM ODK_Tree_registration_tree_species a
JOIN odk_tree_registration_main b
ON a.submissionid_odk = b.submissionid_odk

UNION ALL

SELECT
d.contract_number AS contract_number,
'AKVO' AS source_data,
'Registration' AS methodology,
c.lat_name_species AS species_name_latin,
CASE
        WHEN SUBSTRING(c.lat_name_species, LENGTH(c.lat_name_species) - 2, 1) = 'E' THEN 'exotic'
        WHEN SUBSTRING(c.lat_name_species, LENGTH(c.lat_name_species) - 2, 1) = 'N' THEN 'native'
        ELSE 'unknown'
END AS native_exotic,

c.number_species AS nr_trees_per_species

FROM akvo_tree_registration_species c
JOIN akvo_tree_registration_areas d
ON c.identifier_akvo = d.identifier_akvo),

-- Calculate the percentages of native versus exotic species on contract level
total_trees AS (
    SELECT
        contract_number,
        SUM(nr_trees_per_species) AS total_nr_trees
    FROM CTE_percentage_exotic_native
    GROUP BY contract_number),

CTE_results_percentages_exotic_native AS (
    SELECT
        c.contract_number,
        ROUND(SUM(CASE WHEN native_exotic = 'native' THEN nr_trees_per_species ELSE 0 END) * 100.0 / NULLIF(t.total_nr_trees, 0), 1) AS percentage_native,
        ROUND(SUM(CASE WHEN native_exotic = 'exotic' THEN nr_trees_per_species ELSE 0 END) * 100.0 / NULLIF(t.total_nr_trees, 0), 1) AS percentage_exotic,
		ROUND(SUM(CASE WHEN native_exotic = 'unknown' THEN nr_trees_per_species ELSE 0 END) * 100.0 / NULLIF(t.total_nr_trees, 0), 1) AS percentage_unknown
    FROM CTE_percentage_exotic_native c
    JOIN total_trees t ON c.contract_number = t.contract_number
    GROUP BY c.contract_number,t.total_nr_trees),


-- This table unifies all submissions from audits and monitorings on instance level.
-- This is a preperatory table to label all instances with a label_strata
CTE_union_monitorings_audits AS (SELECT
AKVO_Tree_monitoring_areas.identifier_akvo,
AKVO_Tree_monitoring_areas.instance::TEXT,
AKVO_Tree_monitoring_areas.submission
FROM AKVO_Tree_monitoring_areas

UNION ALL

SELECT
AKVO_Tree_external_audits_areas.identifier_akvo,
AKVO_Tree_external_audits_areas.instance::TEXT,
AKVO_Tree_external_audits_areas.submission
FROM AKVO_Tree_external_audits_areas

UNION ALL

SELECT
odk_tree_monitoring_main.ecosia_site_id AS identifier_akvo,
odk_tree_monitoring_main.submissionid_odk AS instance,
odk_tree_monitoring_main.submission_date
FROM odk_tree_monitoring_main),


-- This table counts the total number of sites that were monitored or audited on contract level
CTE_tree_monitoring AS (
SELECT

--WHEN akvo_tree_registration_areas_updated.contract_number NOTNULL
--THEN akvo_tree_registration_areas_updated.contract_number
--ELSE odk_tree_registration_main.contract_number
--END AS contract_number,

akvo_tree_registration_areas_updated.contract_number,

COUNT(DISTINCT CTE_union_monitorings_audits.identifier_akvo) as nr_sites_monitored_audited,
COUNT(DISTINCT CTE_union_monitorings_audits.instance) as total_nr_monitorings_audits,
MAX(CTE_union_monitorings_audits.submission) AS "Latest submitted monitoring or audit"
FROM akvo_tree_registration_areas_updated
LEFT JOIN CTE_union_monitorings_audits
ON akvo_tree_registration_areas_updated.identifier_akvo = CTE_union_monitorings_audits.identifier_akvo
LEFT JOIN odk_tree_registration_main
ON akvo_tree_registration_areas_updated.identifier_akvo = odk_tree_registration_main.ecosia_site_id
GROUP BY akvo_tree_registration_areas_updated.contract_number),


-- This table lists all tree species on contract level that were reported during the registration
CTE_tree_species AS (
SELECT
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_species.lat_name_species AS "Number of tree species registered"
FROM akvo_tree_registration_areas_updated
LEFT JOIN akvo_tree_registration_species
ON akvo_tree_registration_areas_updated.identifier_akvo = akvo_tree_registration_species.identifier_akvo

UNION ALL

SELECT
odk_tree_registration_main.contract_number,
odk_tree_registration_main.tree_species AS "Number of tree species registered"
FROM odk_tree_registration_main),

CTE_count_species AS (SELECT
contract_number,
COUNT(DISTINCT "Number of tree species registered") AS "Number of tree species registered"
FROM CTE_tree_species
GROUP BY contract_number),


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
JOIN (
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
label_strata > 0 AND label_strata <= 540
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
label_strata > 540 AND label_strata <= 900
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
label_strata > 900
GROUP BY identifier_akvo) table_b
ON table_a.identifier_akvo = table_b.identifier_akvo
AND table_a.label_strata = table_b.max_label_strata_t3),

-- This table lists the monitoring results for sites that were been monitored in t=1. In case the site was monitored
-- the monitoring results are used. In a the site was NOT monitored, the weighted average survival percentage is used
-- to determine survived tree numbers in t=1
CTE_calculate_extrapolated_tree_number_site_level_t1 AS (SELECT
akvo_tree_registration_areas_updated.identifier_akvo,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.tree_number,

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
akvo_tree_registration_areas_updated.tree_number,

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
CTE_count_species."Number of tree species registered",

CTE_results_percentages_exotic_native.percentage_native,
CTE_results_percentages_exotic_native.percentage_exotic,
CTE_results_percentages_exotic_native.percentage_unknown


FROM CTE_total_tree_registrations
LEFT JOIN CTE_tree_monitoring
ON CTE_tree_monitoring.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_count_species
ON CTE_count_species.contract_number = CTE_total_tree_registrations.contract_number
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
ON CTE_calculate_extrapolated_tree_number_contract_level_t3.contract_number = CTE_total_tree_registrations.contract_number
LEFT JOIN CTE_results_percentages_exotic_native
ON CTE_results_percentages_exotic_native.contract_number = CTE_total_tree_registrations.contract_number;'''

conn.commit()

create_a31 = '''CREATE TABLE superset_ecosia_nursery_registration
AS WITH akvo_nursery_registrations AS (SELECT
akvo_nursery_registration.identifier_akvo,
akvo_nursery_registration.display_name,
akvo_nursery_registration.submitter,
akvo_nursery_registration.instance::TEXT,
akvo_nursery_registration.submission,
'AKVO' AS source_data,
LOWER(akvo_nursery_registration.country) AS country,
akvo_nursery_registration.test,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN akvo_nursery_registration.organisation != ''
THEN
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(akvo_nursery_registration.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

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
FROM akvo_nursery_registration),

-------

odk_nursery_registrations AS (SELECT
odk_nursery_registration_main.ecosia_nursery_id AS identifier_akvo,
CONCAT(odk_nursery_registration_main.organisation, ' - ', odk_nursery_registration_main.nursery_registration_name, ' - ', odk_nursery_registration_main.user_name) AS display_name,
odk_nursery_registration_main.user_name AS submitter,
odk_nursery_registration_main.submissionid_odk AS instance,
odk_nursery_registration_main.submission_date AS submission,
'ODK' AS source_data,
'' AS country,

CASE
WHEN odk_nursery_registration_main.test_data_yes_no = 'valid_data'
THEN 'This is real, valid data'
WHEN odk_nursery_registration_main.test_data_yes_no = 'test_data'
THEN 'This is a test, this record can be deleted.'
END AS test,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_nursery_registration_main.organisation != ''
THEN
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_nursery_registration_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_nursery_registration_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_nursery_registration_main.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_nursery_registration_main.organisation)),
			LENGTH(odk_nursery_registration_main.organisation) - POSITION('-' IN odk_nursery_registration_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_nursery_registration_main.organisation)),
			LENGTH(odk_nursery_registration_main.organisation) - POSITION('-' IN odk_nursery_registration_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_nursery_registration_main.organisation)),
			LENGTH(odk_nursery_registration_main.organisation) - POSITION('-' IN odk_nursery_registration_main.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(odk_nursery_registration_main.organisation) AS organisation,

CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/')))
ElSE LOWER(odk_nursery_registration_main.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/')-1)))
ElSE LOWER(odk_nursery_registration_main.organisation)
END AS sub_partner,

odk_nursery_registration_main.nursery_registration_type AS nursery_type,
odk_nursery_registration_main.nursery_registration_name AS nursery_name,
odk_nursery_registration_main.nursery_registration_establishment AS newly_established,
odk_nursery_registration_main.nursery_registration_full_production_capacity AS full_tree_capacity,
odk_nursery_registration_main.lat_y,
odk_nursery_registration_main.lon_x
FROM odk_nursery_registration_main)

SELECT * FROM akvo_nursery_registrations
UNION ALL
SELECT * FROM odk_nursery_registrations;

UPDATE superset_ecosia_nursery_registration
SET test = 'yes'
WHERE test = 'This is a test, this record can be deleted.'
OR test = 'xxxxx';

UPDATE superset_ecosia_nursery_registration
SET test = 'no'
WHERE test = 'This is real, valid data'
OR test = ''
OR test = 'This is no test data'
OR test = 'valid data';

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
SELECT identifier_akvo, photo_url_4 FROM akvo_site_registration_distributed_trees_photos
UNION ALL

SELECT odk_tree_registration_main.ecosia_site_id, photo_name_1 FROM odk_tree_registration_photos
LEFT JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk
UNION ALL
SELECT odk_tree_registration_main.ecosia_site_id, photo_name_2 FROM odk_tree_registration_photos
LEFT JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk
UNION ALL
SELECT odk_tree_registration_main.ecosia_site_id, photo_name_3 FROM odk_tree_registration_photos
LEFT JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk
UNION ALL
SELECT odk_tree_registration_main.ecosia_site_id, photo_name_4 FROM odk_tree_registration_photos
LEFT JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk),

count_total_number_photos_per_site AS (SELECT identifier_akvo, COUNT(identifier_akvo) AS total_nr_photos
FROM COUNT_Total_number_of_photos_taken
GROUP BY identifier_akvo),

seperate_tree_species_registration_odk AS (SELECT
ecosia_site_id AS identifier_akvo,
UNNEST(STRING_TO_ARRAY(tree_species, ' ')) AS species_registered_odk
FROM odk_tree_registration_main),

count_number_tree_species_registered AS (SELECT identifier_akvo, COUNT(*) AS nr_species_registered
FROM AKVO_Tree_registration_species
GROUP BY AKVO_Tree_registration_species.identifier_akvo

UNION ALL

SELECT
seperate_tree_species_registration_odk.identifier_akvo,
COUNT(*) AS nr_species_registered
FROM seperate_tree_species_registration_odk
GROUP BY seperate_tree_species_registration_odk.identifier_akvo),

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
t.akvo_form_version AS form_version,
t.country,
t.test,
t.data_source,

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
OR test = 'This is no test data'
OR test = 'This is real, valid data\r';

UPDATE superset_ecosia_tree_registration
SET organisation = LOWER(organisation);

UPDATE superset_ecosia_tree_registration
SET country = LOWER(country);'''

conn.commit()

create_a33 = '''CREATE TABLE superset_ecosia_tree_monitoring
AS SELECT

calc_tab_monitoring_calculations_per_site_merged.identifier_akvo,
calc_tab_monitoring_calculations_per_site_merged.display_name,
LOWER(calc_tab_monitoring_calculations_per_site_merged.country) AS country,

-- Create a unique code for filtering in superset, based on main organisation name

CASE
WHEN calc_tab_monitoring_calculations_per_site_merged.organisation != ''
THEN
CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,


-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN calc_tab_monitoring_calculations_per_site_merged.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation)),
			LENGTH(calc_tab_monitoring_calculations_per_site_merged.organisation) - POSITION('-' IN calc_tab_monitoring_calculations_per_site_merged.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation)),
			LENGTH(calc_tab_monitoring_calculations_per_site_merged.organisation) - POSITION('-' IN calc_tab_monitoring_calculations_per_site_merged.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation)),
			LENGTH(calc_tab_monitoring_calculations_per_site_merged.organisation) - POSITION('-' IN calc_tab_monitoring_calculations_per_site_merged.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation) AS organisation,

CASE
WHEN POSITION('-' IN calc_tab_monitoring_calculations_per_site_merged.organisation) > 0
THEN LOWER(left(calc_tab_monitoring_calculations_per_site_merged.organisation, strpos(calc_tab_monitoring_calculations_per_site_merged.organisation, '-') - 2))
WHEN POSITION('/' IN calc_tab_monitoring_calculations_per_site_merged.organisation) > 0
THEN LOWER(left(calc_tab_monitoring_calculations_per_site_merged.organisation, strpos(calc_tab_monitoring_calculations_per_site_merged.organisation, '/') - 1))
ElSE
LOWER(calc_tab_monitoring_calculations_per_site_merged.organisation)
END AS partner,

CASE
WHEN POSITION('-' IN calc_tab_monitoring_calculations_per_site_merged.organisation) > 0
THEN LOWER(right(calc_tab_monitoring_calculations_per_site_merged.organisation, (LENGTH(calc_tab_monitoring_calculations_per_site_merged.organisation) - strpos(calc_tab_monitoring_calculations_per_site_merged.organisation, '-')-1)))
WHEN POSITION('/' IN calc_tab_monitoring_calculations_per_site_merged.organisation) > 0
THEN LOWER(right(calc_tab_monitoring_calculations_per_site_merged.organisation, (LENGTH(calc_tab_monitoring_calculations_per_site_merged.organisation) - strpos(calc_tab_monitoring_calculations_per_site_merged.organisation, '/'))))
ElSE
''
END AS sub_partner,

calc_tab_monitoring_calculations_per_site_merged.submitter,
calc_tab_monitoring_calculations_per_site_merged.contract_number AS sub_contract,
calc_tab_monitoring_calculations_per_site_merged.id_planting_site,
calc_tab_monitoring_calculations_per_site_merged.calc_area,
calc_tab_monitoring_calculations_per_site_merged.registered_tree_number,
calc_tab_monitoring_calculations_per_site_merged.test,
calc_tab_monitoring_calculations_per_site_merged.procedure,
calc_tab_monitoring_calculations_per_site_merged.data_collection_method,
calc_tab_monitoring_calculations_per_site_merged.avg_tree_distance_m,
calc_tab_monitoring_calculations_per_site_merged.avg_tree_density,
calc_tab_monitoring_calculations_per_site_merged.nr_trees_monitored,
calc_tab_monitoring_calculations_per_site_merged.number_pcq_samples,
calc_tab_monitoring_calculations_per_site_merged.planting_date,
calc_tab_monitoring_calculations_per_site_merged.latest_monitoring_submission,
calc_tab_monitoring_calculations_per_site_merged.nr_days_registration_monitoring,
calc_tab_monitoring_calculations_per_site_merged.nr_years_registration_monitoring,
calc_tab_monitoring_calculations_per_site_merged.label_strata,
calc_tab_monitoring_calculations_per_site_merged.perc_trees_survived,
calc_tab_monitoring_calculations_per_site_merged.avg_tree_height,
calc_tab_monitoring_calculations_per_site_merged.site_impressions

FROM calc_tab_monitoring_calculations_per_site_merged;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_monitoring
ADD contract NUMERIC(20,0);

UPDATE superset_ecosia_tree_monitoring
SET contract = TRUNC(sub_contract);'''

conn.commit()


create_a36 = '''CREATE TABLE superset_ecosia_nursery_monitoring
AS WITH akvo_ecosia_nursery_monitoring AS (SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,
'AKVO' AS source_data_monitoring,

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
akvo_nursery_monitoring.nr_working_personel,
akvo_nursery_monitoring.month_planting_stock,
akvo_nursery_monitoring.number_trees_produced_currently,
akvo_nursery_monitoring.challenges_nursery,
akvo_nursery_monitoring.gender_nursery_manager,
akvo_nursery_monitoring.test,
akvo_nursery_monitoring.name_nursery_manager,
akvo_nursery_monitoring.submitter,
akvo_nursery_monitoring.submission_date,
akvo_nursery_monitoring.identifier_akvo,

akvo_nursery_registration.lat_y,
akvo_nursery_registration.lon_x

FROM akvo_nursery_monitoring
JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = akvo_nursery_monitoring.identifier_akvo),


------------------


odk_ecosia_nursery_monitoring AS (SELECT

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CONCAT(odk_nursery_registration_main.organisation, ' - ', odk_nursery_registration_main.nursery_registration_name, ' - ', odk_nursery_registration_main.user_name)
WHEN akvo_nursery_registration.organisation NOTNULL
THEN CONCAT(akvo_nursery_registration.organisation, ' - ', akvo_nursery_registration.nursery_name, ' - ', akvo_nursery_registration.submitter)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data_monitoring,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),3)),4))) AS NUMERIC)
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
--ELSE 'no code generated'
END
END AS partnercode_main,


-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN odk_nursery_monitoring_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 3)),2)) AS NUMERIC)
END
WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END
END AS partnercode_sub,

CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN LOWER(odk_nursery_monitoring_main.organisation)
WHEN akvo_nursery_registration.organisation NOTNULL
THEN akvo_nursery_registration.organisation
ELSE 'organisation name unknown'
END AS organisation,

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/')))
ElSE
LOWER(odk_nursery_registration_main.organisation)
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
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
END
END AS partner,

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/')-1)))
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
END
ElSE LOWER(akvo_nursery_registration.organisation)
END AS sub_partner,

CASE
WHEN odk_nursery_registration_main.nursery_registration_name NOTNULL
THEN odk_nursery_registration_main.nursery_registration_name
WHEN akvo_nursery_registration.nursery_name NOTNULL
THEN akvo_nursery_registration.nursery_name
ELSE 'no name'
END AS nursery_name,

odk_nursery_monitoring_main.nursery_monitoring_nr_people AS nr_working_personel,
odk_nursery_monitoring_main.nursery_monitoring_planting_months AS month_planting_stock,
odk_nursery_monitoring_main.nursery_monitoring_tree_capacity AS number_trees_produced_currently,
odk_nursery_monitoring_main.nursery_monitoring_challenges AS challenges_nursery,
odk_nursery_monitoring_main.nursery_monitoring_gender_manager AS gender_nursery_manager,
odk_nursery_monitoring_main.test_data_yes_no AS test,
odk_nursery_monitoring_main.nursery_monitoring_manager AS name_nursery_manager,
odk_nursery_monitoring_main.user_name AS submitter,
odk_nursery_monitoring_main.field_date AS submission_date,
odk_nursery_monitoring_main.ecosia_nursery_id AS identifier_akvo,

CASE
WHEN odk_nursery_registration_main.lat_y NOTNULL
THEN odk_nursery_registration_main.lat_y
WHEN akvo_nursery_registration.lat_y NOTNULL
THEN akvo_nursery_registration.lat_y
END AS lat_y,

CASE
WHEN odk_nursery_registration_main.lon_x NOTNULL
THEN odk_nursery_registration_main.lon_x
WHEN akvo_nursery_registration.lon_x NOTNULL
THEN akvo_nursery_registration.lon_x
END AS lon_x

FROM odk_nursery_monitoring_main
LEFT JOIN odk_nursery_registration_main
ON odk_nursery_registration_main.ecosia_nursery_id = odk_nursery_monitoring_main.ecosia_nursery_id
--ON odk_nursery_registration_main.submissionid_odk = odk_nursery_monitoring_main.submissionid_odk
LEFT JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = odk_nursery_monitoring_main.ecosia_nursery_id)


SELECT * FROM akvo_ecosia_nursery_monitoring
UNION ALL
SELECT * FROM odk_ecosia_nursery_monitoring;


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
AS WITH akvo_ecosia_nursery_monitoring_species AS (SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,
'AKVO' AS source_data,

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
akvo_nursery_monitoring_tree_species.identifier_akvo,
akvo_nursery_monitoring_tree_species.tree_species_latin


FROM akvo_nursery_monitoring_tree_species
JOIN akvo_nursery_registration
ON akvo_nursery_monitoring_tree_species.identifier_akvo = akvo_nursery_registration.identifier_akvo
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring.instance = akvo_nursery_monitoring_tree_species.instance
WHERE akvo_nursery_registration.organisation NOTNULL),


------------------


seperate_tree_species_monitoring_nursery_odk AS (SELECT

CASE
WHEN odk_nursery_registration_main.ecosia_nursery_id NOTNULL
THEN odk_nursery_registration_main.ecosia_nursery_id
WHEN akvo_nursery_registration.identifier_akvo NOTNULL
THEN akvo_nursery_registration.identifier_akvo
ELSE ''
END AS identifier,

odk_nursery_monitoring_main.submissionid_odk,


UNNEST(STRING_TO_ARRAY(nursery_monitoring_tree_species, ' ')) AS nursery_species_monitored
FROM odk_nursery_monitoring_main
LEFT JOIN odk_nursery_registration_main
ON odk_nursery_registration_main.ecosia_nursery_id = odk_nursery_monitoring_main.ecosia_nursery_id
LEFT JOIN akvo_nursery_registration
ON odk_nursery_monitoring_main.ecosia_nursery_id = akvo_nursery_registration.identifier_akvo),


odk_ecosia_nursery_monitoring_species AS (SELECT

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CONCAT(odk_nursery_registration_main.organisation, ' - ', odk_nursery_registration_main.nursery_registration_name, ' - ', odk_nursery_registration_main.user_name)
WHEN akvo_nursery_registration.organisation NOTNULL
THEN CONCAT(akvo_nursery_registration.organisation, ' - ', akvo_nursery_registration.nursery_name, ' - ', akvo_nursery_registration.submitter)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),3)),4))) AS NUMERIC)
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
--ELSE 'no code generated'
END
END AS partnercode_main,


-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN odk_nursery_monitoring_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 3)),2)) AS NUMERIC)
END
WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END
END AS partnercode_sub,

CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN LOWER(odk_nursery_monitoring_main.organisation)
WHEN akvo_nursery_registration.organisation NOTNULL
THEN akvo_nursery_registration.organisation
ELSE 'organisation name unknown'
END AS organisation,


CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/')))
ElSE
LOWER(odk_nursery_registration_main.organisation)
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
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
END
END AS partner,

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/')-1)))
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
END
ElSE LOWER(akvo_nursery_registration.organisation)
END AS sub_partner,

CASE
WHEN odk_nursery_registration_main.nursery_registration_name NOTNULL
THEN odk_nursery_registration_main.nursery_registration_name
WHEN akvo_nursery_registration.nursery_name NOTNULL
THEN akvo_nursery_registration.nursery_name
END AS nursery_registration_name,

odk_nursery_monitoring_main.submission_date,

odk_nursery_monitoring_main.ecosia_nursery_id AS identifier_akvo,
seperate_tree_species_monitoring_nursery_odk.nursery_species_monitored


FROM odk_nursery_monitoring_main
LEFT JOIN odk_nursery_registration_main
ON odk_nursery_monitoring_main.ecosia_nursery_id = odk_nursery_registration_main.ecosia_nursery_id
LEFT JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = odk_nursery_monitoring_main.ecosia_nursery_id
LEFT JOIN seperate_tree_species_monitoring_nursery_odk
ON seperate_tree_species_monitoring_nursery_odk.submissionid_odk = odk_nursery_monitoring_main.submissionid_odk)


SELECT * FROM akvo_ecosia_nursery_monitoring_species
UNION ALL
SELECT * FROM odk_ecosia_nursery_monitoring_species;'''

conn.commit()

create_a38 = '''CREATE TABLE superset_ecosia_nursery_registration_photos
AS WITH nursery_registration_photos_akvo AS (SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,
'AKVO' AS source_data_monitoring,

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
ElSE ''
END AS sub_partner,

akvo_nursery_registration.nursery_name,
akvo_nursery_registration.submission AS submission_date,
akvo_nursery_registration_photos.photo_url,
akvo_nursery_registration_photos.centroid_coord,
akvo_nursery_registration_photos.identifier_akvo


FROM akvo_nursery_registration_photos
JOIN akvo_nursery_registration
ON akvo_nursery_registration_photos.identifier_akvo =  akvo_nursery_registration.identifier_akvo),


-------------------------

list_photos_registration_nursery_odk AS (
SELECT ecosia_nursery_id, nursery_registration_gps, photo_nursery_bed_1 AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, photo_nursery_bed_2 AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, photo_nursery_bed_3 AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, photo_nursery_bed_4 AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, nursery_registration_photo_north AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, nursery_registration_photo_east AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, nursery_registration_photo_south AS odk_photo_registration FROM odk_nursery_registration_main
UNION ALL
SELECT ecosia_nursery_id, nursery_registration_gps, nursery_registration_photo_west AS odk_photo_registration FROM odk_nursery_registration_main),


nursery_registration_photos_odk AS (SELECT

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CONCAT(odk_nursery_registration_main.organisation, ' - ', odk_nursery_registration_main.nursery_registration_name, ' - ', odk_nursery_registration_main.user_name)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data_monitoring,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_nursery_registration_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_nursery_registration_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_nursery_registration_main.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,


-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_nursery_registration_main.organisation)),
			LENGTH(odk_nursery_registration_main.organisation) - POSITION('-' IN odk_nursery_registration_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_nursery_registration_main.organisation)),
			LENGTH(odk_nursery_registration_main.organisation) - POSITION('-' IN odk_nursery_registration_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_nursery_registration_main.organisation)),
			LENGTH(odk_nursery_registration_main.organisation) - POSITION('-' IN odk_nursery_registration_main.organisation) - 3)),2)) AS NUMERIC)
ELSE 0
END AS partnercode_sub,

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN LOWER(odk_nursery_registration_main.organisation)
ELSE 'organisation unknown'
END AS organisation,

CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/')))
ElSE 'partner unknown'
END AS partner,

CASE
WHEN POSITION('- ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/')-1)))
ElSE 'subpartner unknown or not relevant'
END AS sub_partner,

CASE
WHEN odk_nursery_registration_main.nursery_registration_name NOTNULL
THEN odk_nursery_registration_main.nursery_registration_name
ELSE 'name unknown'
END AS nursery_name,

odk_nursery_registration_main.submission_date AS submission,

list_photos_registration_nursery_odk.odk_photo_registration AS photo_url,
list_photos_registration_nursery_odk.nursery_registration_gps AS centroid_coord,
list_photos_registration_nursery_odk.ecosia_nursery_id AS identifier_akvo

FROM odk_nursery_registration_main
LEFT JOIN list_photos_registration_nursery_odk
ON odk_nursery_registration_main.ecosia_nursery_id =  list_photos_registration_nursery_odk.ecosia_nursery_id
WHERE list_photos_registration_nursery_odk.odk_photo_registration NOTNULL)


SELECT * FROM nursery_registration_photos_akvo
UNION ALL
SELECT * FROM nursery_registration_photos_odk;


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
AS WITH nursery_monitoring_photos_akvo AS (SELECT
akvo_nursery_registration.display_name,
LOWER(akvo_nursery_registration.country) AS country,
'AKVO' AS source_data_monitoring,

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
akvo_nursery_monitoring_photos.photo_url,
akvo_nursery_monitoring_photos.centroid_coord,
akvo_nursery_monitoring_photos.identifier_akvo

FROM akvo_nursery_monitoring_photos
JOIN akvo_nursery_monitoring
ON akvo_nursery_monitoring.instance = akvo_nursery_monitoring_photos.instance
JOIN akvo_nursery_registration
ON akvo_nursery_monitoring_photos.identifier_akvo =  akvo_nursery_registration.identifier_akvo),


----------------------------

list_photos_monitoring_nursery_odk AS (
SELECT submissionid_odk, ecosia_nursery_id, nursery_bed_gps, nursery_monitoring_bed_1 AS odk_photo_monitoring_nursery FROM odk_nursery_monitoring_main
UNION ALL
SELECT submissionid_odk, ecosia_nursery_id, nursery_bed_gps, nursery_monitoring_bed_2 AS odk_photo_monitoring_nursery FROM odk_nursery_monitoring_main
UNION ALL
SELECT submissionid_odk, ecosia_nursery_id, nursery_bed_gps, nursery_monitoring_bed_3 AS odk_photo_monitoring_nursery FROM odk_nursery_monitoring_main
UNION ALL
SELECT submissionid_odk, ecosia_nursery_id, nursery_bed_gps, nursery_monitoring_bed_4 AS odk_photo_monitoring_nursery FROM odk_nursery_monitoring_main),


nursery_monitoring_photos_odk AS (SELECT

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CONCAT(odk_nursery_registration_main.organisation, ' - ', odk_nursery_registration_main.nursery_registration_name, ' - ', odk_nursery_registration_main.user_name)
WHEN akvo_nursery_registration.organisation NOTNULL
THEN CONCAT(akvo_nursery_registration.organisation, ' - ', akvo_nursery_registration.nursery_name, ' - ', akvo_nursery_registration.submitter)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_nursery_monitoring_main.organisation),3)),4))) AS NUMERIC)
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
--ELSE 'no code generated'
END
END AS partnercode_main,


-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN odk_nursery_monitoring_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_nursery_monitoring_main.organisation)),
			LENGTH(odk_nursery_monitoring_main.organisation) - POSITION('-' IN odk_nursery_monitoring_main.organisation) - 3)),2)) AS NUMERIC)
END
WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(akvo_nursery_registration.organisation)),
			LENGTH(akvo_nursery_registration.organisation) - POSITION('-' IN akvo_nursery_registration.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END
END AS partnercode_sub,

CASE
WHEN odk_nursery_monitoring_main.organisation NOTNULL
THEN LOWER(odk_nursery_monitoring_main.organisation)
WHEN akvo_nursery_registration.organisation NOTNULL
THEN akvo_nursery_registration.organisation
ELSE 'organisation name unknown'
END AS organisation,


CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(left(odk_nursery_registration_main.organisation, strpos(odk_nursery_registration_main.organisation, '/')))
ElSE
LOWER(odk_nursery_registration_main.organisation)
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
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
END
END AS partner,

CASE
WHEN odk_nursery_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_nursery_registration_main.organisation) > 0
THEN LOWER(right(odk_nursery_registration_main.organisation, (LENGTH(odk_nursery_registration_main.organisation) - strpos(odk_nursery_registration_main.organisation, '/')-1)))
END

WHEN akvo_nursery_registration.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-')-1)))
WHEN POSITION('-' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '-'))))
WHEN POSITION('/' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/'))))
WHEN POSITION('/ ' IN akvo_nursery_registration.organisation) > 0
THEN LOWER(right(akvo_nursery_registration.organisation, (LENGTH(akvo_nursery_registration.organisation) - strpos(akvo_nursery_registration.organisation, '/')-1)))
END
ElSE ''
END AS sub_partner,

CASE
WHEN odk_nursery_registration_main.nursery_registration_name NOTNULL
THEN odk_nursery_registration_main.nursery_registration_name
WHEN akvo_nursery_registration.nursery_name NOTNULL
THEN akvo_nursery_registration.nursery_name
END AS nursery_registration_name,

odk_nursery_monitoring_main.submission_date,

list_photos_monitoring_nursery_odk.odk_photo_monitoring_nursery AS photo_url,
list_photos_monitoring_nursery_odk.nursery_bed_gps AS centroid_coord,
list_photos_monitoring_nursery_odk.ecosia_nursery_id AS identifier_akvo

FROM odk_nursery_monitoring_main
LEFT JOIN odk_nursery_registration_main
ON odk_nursery_monitoring_main.ecosia_nursery_id = odk_nursery_registration_main.ecosia_nursery_id
LEFT JOIN akvo_nursery_registration
ON akvo_nursery_registration.identifier_akvo = odk_nursery_monitoring_main.ecosia_nursery_id
LEFT JOIN list_photos_monitoring_nursery_odk
ON list_photos_monitoring_nursery_odk.submissionid_odk = odk_nursery_monitoring_main.submissionid_odk
WHERE list_photos_monitoring_nursery_odk.odk_photo_monitoring_nursery NOTNULL)


SELECT * FROM nursery_monitoring_photos_akvo
UNION ALL
SELECT * FROM nursery_monitoring_photos_odk;


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
AS WITH tree_registration_photos_akvo AS (SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
'AKVO' AS source_data,

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
ELSE 0
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
ElSE LOWER(akvo_tree_registration_areas_updated.organisation)
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
ElSE LOWER(akvo_tree_registration_areas_updated.organisation)
END AS sub_partner,

akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Tree registration' AS procedure,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,
akvo_tree_registration_photos.identifier_akvo,
akvo_tree_registration_photos.instance::TEXT AS instance,
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
'AKVO' AS source_data,

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
akvo_tree_registration_areas_updated.instance::TEXT AS instance,

akvo_tree_registration_areas_updated.photo_owner AS photo_url,
NULL AS photo_geotag_location,
akvo_tree_registration_areas_updated.centroid_coord AS photo_gps_location

FROM akvo_tree_registration_areas_updated
WHERE akvo_tree_registration_areas_updated.photo_owner NOTNULL
AND NOT akvo_tree_registration_areas_updated.photo_owner ~ '^\s*$'),    -- This last row is needed as not all empty rows are captured by the NOTNULL (strange)


-----------------------


list_photos_tree_registration_odk AS (SELECT
odk_tree_registration_photos.submissionid_odk, odk_tree_registration_main.ecosia_site_id,
odk_tree_registration_photos.photo_gps_location, odk_tree_registration_photos.photo_name_1 AS photo_url
FROM odk_tree_registration_photos
JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk

UNION ALL

SELECT odk_tree_registration_photos.submissionid_odk, odk_tree_registration_main.ecosia_site_id,
odk_tree_registration_photos.photo_gps_location, odk_tree_registration_photos.photo_name_2 AS photo_url
FROM odk_tree_registration_photos
JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk

UNION ALL

SELECT odk_tree_registration_photos.submissionid_odk, odk_tree_registration_main.ecosia_site_id,
odk_tree_registration_photos.photo_gps_location, odk_tree_registration_photos.photo_name_3 AS photo_url
FROM odk_tree_registration_photos
JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk

UNION ALL

SELECT odk_tree_registration_photos.submissionid_odk, odk_tree_registration_main.ecosia_site_id,
odk_tree_registration_photos.photo_gps_location, odk_tree_registration_photos.photo_name_4 AS photo_url
FROM odk_tree_registration_photos
JOIN odk_tree_registration_main
ON odk_tree_registration_main.submissionid_odk = odk_tree_registration_photos.submissionid_odk),


tree_registration_photos_odk AS (SELECT

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CONCAT(odk_tree_registration_main.organisation, ' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.submitter)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 3)),2)) AS NUMERIC)
ELSE 0
END AS partnercode_sub,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN LOWER(odk_tree_registration_main.organisation)
ELSE 'organisation name unknown'
END AS organisation,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN
CASE
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '/')))
ElSE LOWER(odk_tree_registration_main.organisation)
END
END AS partner,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '/')-1)))
ElSE LOWER(odk_tree_registration_main.organisation)
END
END AS sub_partner,

-- The registered sites with ODK are already merged in the 'akvo_tree_registration_areas_updated' table. As such, best is to use this table to join data as all sites (ODK and AKVO) are in this table.
akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Tree registration' AS procedure,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,

list_photos_tree_registration_odk.ecosia_site_id AS identifier_akvo,
list_photos_tree_registration_odk.submissionid_odk AS instance,
list_photos_tree_registration_odk.photo_url AS photo_url,
list_photos_tree_registration_odk.photo_gps_location AS photo_geotag_location,
list_photos_tree_registration_odk.photo_gps_location AS photo_gps_location

FROM
list_photos_tree_registration_odk
JOIN akvo_tree_registration_areas_updated
ON akvo_tree_registration_areas_updated.identifier_akvo = list_photos_tree_registration_odk.ecosia_site_id
JOIN odk_tree_registration_main
ON odk_tree_registration_main.ecosia_site_id = list_photos_tree_registration_odk.ecosia_site_id

-- With this UNION below we collect the photo urls of farmers taken during registration. These photos are stored
-- in the tree registration areas table instead of the tree registration photos table

UNION ALL

SELECT
CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CONCAT(odk_tree_registration_main.organisation, ' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.submitter)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 3)),2)) AS NUMERIC)
ELSE 0
END AS partnercode_sub,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN LOWER(odk_tree_registration_main.organisation)
ELSE 'organisation name unknown'
END AS organisation,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN
CASE
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '/')))
ElSE LOWER(odk_tree_registration_main.organisation)
END
END AS partner,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '/')-1)))
ElSE LOWER(odk_tree_registration_main.organisation)
END
END AS sub_partner,

-- The registered sites with ODK are already merged in the 'akvo_tree_registration_areas_updated' table. As such, best is to use this table to join data as all sites (ODK and AKVO) are in this table.
akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Tree registration' AS procedure,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,

odk_tree_registration_main.ecosia_site_id AS identifier_akvo,
odk_tree_registration_main.submissionid_odk AS instance,
odk_tree_registration_main.photo_owner AS photo_url,
odk_tree_registration_main.centroid_coord AS photo_geotag_location,
odk_tree_registration_main.centroid_coord AS photo_gps_location

FROM odk_tree_registration_main
JOIN akvo_tree_registration_areas_updated
ON odk_tree_registration_main.ecosia_site_id = akvo_tree_registration_areas_updated.identifier_akvo
WHERE odk_tree_registration_main.photo_owner NOTNULL
AND NOT akvo_tree_registration_areas_updated.photo_owner ~ '^\s*$')


SELECT * FROM tree_registration_photos_akvo
UNION ALL
SELECT * FROM tree_registration_photos_odk;


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
AS WITH tree_registration_species_akvo AS (SELECT
akvo_tree_registration_areas_updated.display_name,
LOWER(akvo_tree_registration_areas_updated.country) AS country,
'AKVO' AS source_data,

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
AKVO_tree_registration_species.identifier_akvo,
AKVO_tree_registration_species.instance,
AKVO_tree_registration_species.lat_name_species,
AKVO_tree_registration_species.local_name_species,
AKVO_tree_registration_species.number_species

FROM AKVO_tree_registration_species
JOIN akvo_tree_registration_areas_updated
ON AKVO_tree_registration_species.identifier_akvo = akvo_tree_registration_areas_updated.identifier_akvo),


----------------------

seperate_tree_species_registration_odk AS (SELECT
submissionid_odk, ecosia_site_id,
UNNEST(STRING_TO_ARRAY(tree_species, ' ')) AS species_registered_odk
FROM odk_tree_registration_main),

tree_registration_species_odk AS (SELECT

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CONCAT(odk_tree_registration_main.organisation, ' - ', odk_tree_registration_main.id_planting_site, ' - ', odk_tree_registration_main.submitter)
ELSE 'No display name could be created'
END AS display_name,

'' AS country,
'ODK' AS source_data,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_tree_registration_main.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_tree_registration_main.organisation)),
			LENGTH(odk_tree_registration_main.organisation) - POSITION('-' IN odk_tree_registration_main.organisation) - 3)),2)) AS NUMERIC)
ELSE 0
END AS partnercode_sub,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN LOWER(odk_tree_registration_main.organisation)
ELSE 'organisation name unknown'
END AS organisation,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN
CASE
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '-')))
WHEN POSITION('/' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(left(odk_tree_registration_main.organisation, strpos(odk_tree_registration_main.organisation, '/')))
ElSE LOWER(odk_tree_registration_main.organisation)
END
END AS partner,

CASE
WHEN odk_tree_registration_main.organisation NOTNULL
THEN CASE
WHEN POSITION('- ' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '-'))))
WHEN POSITION('/' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_tree_registration_main.organisation) > 0
THEN LOWER(right(odk_tree_registration_main.organisation, (LENGTH(odk_tree_registration_main.organisation) - strpos(odk_tree_registration_main.organisation, '/')-1)))
ElSE LOWER(odk_tree_registration_main.organisation)
END
END AS sub_partner,

-- The registered sites with ODK are already merged in the 'akvo_tree_registration_areas_updated' table. As such, best is to use this table to join data as all sites (ODK and AKVO) are in this table.
akvo_tree_registration_areas_updated.contract_number AS sub_contract,
'Tree registration' AS procedure,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.submission AS submission_date,

odk_tree_registration_main.contract_number AS sub_contract,
odk_tree_registration_main.id_planting_site,
odk_tree_registration_main.submission_date,
odk_tree_registration_main.ecosia_site_id AS identifier_akvo,
odk_tree_registration_main.submissionid_odk AS instance,
seperate_tree_species_registration_odk.species_registered_odk AS lat_name_species,
'' AS local_name_species,
odk_tree_registration_main.tree_number AS number_species -- This should not be total tree numbers but the nr of trees for each species. However, currently the ODK form does not capture this: No number per species are asked


FROM odk_tree_registration_main
JOIN akvo_tree_registration_areas_updated
ON odk_tree_registration_main.ecosia_site_id = akvo_tree_registration_areas_updated.identifier_akvo
JOIN seperate_tree_species_registration_odk
ON odk_tree_registration_main.ecosia_site_id = seperate_tree_species_registration_odk.ecosia_site_id)

SELECT * FROM tree_registration_species_akvo
UNION ALL
SELECT * FROM tree_registration_species_akvo;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_tree_registration_species
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_tree_registration_species
SET contract = TRUNC(sub_contract);'''

conn.commit()


create_a44 = '''CREATE TABLE superset_ecosia_geolocations
-- Here we convert the polygon areas from WKT format to geojson string format that can be read by superset
AS WITH
wkt_polygons_to_geojson AS (
SELECT
t.identifier_akvo,
t.instance::TEXT,
t.submission,

CASE
WHEN t.test = 'test_data'
THEN 'yes'
WHEN t.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN t.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN t.test = 'xxxxx'
THEN 'yes'
WHEN t.test = ''
THEN 'no'
WHEN t.test = 'This is real, valid data\r'
THEN 'no'
WHEN t.test = 'valid_data'
THEN 'no'
WHEN t.test = 'Valid data'
THEN 'no'
WHEN t.test = 'This is real, valid data'
THEN 'no'
WHEN t.test ISNULL
THEN 'no'
END AS test,


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
t.id_planting_site,
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
GROUP BY t.identifier_akvo, t.instance, t.test, t.organisation, t.contract_number, t.display_name, t.id_planting_site,
t.submission, t.country),


-- Here we convert the centroid-point locations from WKT format to geojson string format that can be read by superset
-- THIS IS A CTE TABLE THAT DOES NOT NEED TO BE IN THE FINAL UNION BECAUSE THE FOLLOWING CTE TABLE (wkt_buffer_200_trees_areas_to_geojson) WILL BE.
buffer_around_200_trees_centroids AS (SELECT
identifier_akvo,
t.instance::TEXT,
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
t.id_planting_site,
t.display_name,
'tree registration' AS procedure,
ST_Buffer(t.centroid_coord,25) AS buffer


FROM akvo_tree_registration_areas_updated AS t
WHERE t.polygon ISNULL),



-- Here we convert the buffer polygon areas (WKT format) to geojson string format that can be read by superset
wkt_buffer_200_trees_areas_to_geojson AS (
SELECT
t.identifier_akvo,
t.instance::TEXT,
t.submission,

CASE
WHEN t.test = 'test_data'
THEN 'yes'
WHEN t.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN t.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN t.test = 'xxxxx'
THEN 'yes'
WHEN t.test = ''
THEN 'no'
WHEN t.test = 'This is real, valid data\r'
THEN 'no'
WHEN t.test = 'valid_data'
THEN 'no'
WHEN t.test = 'Valid data'
THEN 'no'
WHEN t.test = 'This is real, valid data'
THEN 'no'
WHEN t.test ISNULL
THEN 'no'
END AS test,


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
t.id_planting_site,
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
group by t.identifier_akvo, t.instance, t.test, t.organisation, t.sub_contract, t.id_planting_site, t.display_name,
t.submission, t.country),


-- Here we convert the PCQ MONITORING sample point locations from WKT format to geojson string format that can be read by superset.
-- HERE THIS IS DONE FOR THE AKVO DATA SET.
wkt_pcq_samples_akvo_monitoring_to_geojson AS
(SELECT
pcq_samples_monitorings.identifier_akvo,
akvo_tree_monitoring_areas.instance::TEXT,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN akvo_tree_monitoring_areas.test = 'test_data'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'xxxxx'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = ''
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data\r'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'valid_data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'Valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test ISNULL
THEN 'no'
END AS test,


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
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),

-- Here we convert the photo (GEOTAG) locations (TREE REGISTRATION) from WKT format to geojson string format that can be read by superset
wkt_photo_registration_geotag_to_geojson AS
(SELECT
tree_registration_photos_geotag.identifier_akvo,
akvo_tree_monitoring_areas.instance::TEXT,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN akvo_tree_monitoring_areas.test = 'test_data'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'xxxxx'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = ''
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data\r'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'valid_data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'Valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test ISNULL
THEN 'no'
END AS test,


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
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),



-- Here we convert the PCQ MONITORING sample point locations from WKT format to geojson string format that can be read by superset.
-- HERE THIS IS DONE FOR THE AKVO-ODK DATA SET. So this is the data what was registered with AKVO/ODK (all registrations from AKVO and ODK are in the updated table) but monitored with ODK.
wkt_pcq_samples_akvo_odk_monitoring_to_geojson AS
(SELECT
odk_tree_monitoring_main.ecosia_site_id AS identifier_akvo,
odk_tree_monitoring_pcq.submissionid_odk AS instance,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN odk_tree_monitoring_main.test = 'test_data'
THEN 'yes'
WHEN odk_tree_monitoring_main.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN odk_tree_monitoring_main.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN odk_tree_monitoring_main.test = 'xxxxx'
THEN 'yes'
WHEN odk_tree_monitoring_main.test = ''
THEN 'no'
WHEN odk_tree_monitoring_main.test = 'This is real, valid data\r'
THEN 'no'
WHEN odk_tree_monitoring_main.test = 'valid_data'
THEN 'no'
WHEN odk_tree_monitoring_main.test = 'Valid data'
THEN 'no'
WHEN odk_tree_monitoring_main.test = 'This is real, valid data'
THEN 'no'
WHEN odk_tree_monitoring_main.test ISNULL
THEN 'no'
END AS test,

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
akvo_tree_registration_areas_updated.display_name,
'PCQ sample location monitoring' AS procedure,

'PCQ sample location' AS geolocation_type,

jsonb_build_object(
    'type',       'FeatureCollection',
    'features',   json_agg(json_build_object(
        'type',       'Feature',
		'properties', 'PCQ sample locations monitoring',
        'geometry',   ST_AsGeoJSON(odk_tree_monitoring_pcq.gps_pcq_sample)::json)

    ))::text AS superset_geojson

from akvo_tree_registration_areas_updated
JOIN odk_tree_monitoring_main
ON odk_tree_monitoring_main.ecosia_site_id = akvo_tree_registration_areas_updated.identifier_akvo
JOIN odk_tree_monitoring_pcq
ON odk_tree_monitoring_main.submissionid_odk = odk_tree_monitoring_pcq.submissionid_odk

GROUP BY odk_tree_monitoring_pcq.submissionid_odk,
odk_tree_monitoring_main.ecosia_site_id,
odk_tree_monitoring_main.test,
akvo_tree_registration_areas_updated.organisation,
akvo_tree_registration_areas_updated.contract_number,
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),


-- Here we convert the photo (GPS) locations (TREE REGISTRATION) from WKT format to geojson string format that can be read by superset
wkt_photo_registration_gps_to_geojson AS
(SELECT
tree_registration_photos_gps.identifier_akvo,
akvo_tree_monitoring_areas.instance::TEXT,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN akvo_tree_monitoring_areas.test = 'test_data'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'xxxxx'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = ''
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data\r'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'valid_data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'Valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test ISNULL
THEN 'no'
END AS test,

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
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),

-- Here we convert the PCQ sample point AUDIT locations from WKT format to geojson string format that can be read by superset
wkt_pcq_samples_audit_to_geojson AS
(SELECT
pcq_samples_audits.identifier_akvo,
akvo_tree_monitoring_areas.instance::TEXT,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN akvo_tree_monitoring_areas.test = 'test_data'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'xxxxx'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = ''
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data\r'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'valid_data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'Valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test ISNULL
THEN 'no'
END AS test,


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
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),


-- Here we convert the COUNT sample MONITORING locations from WKT format to geojson string format that can be read by superset
wkt_count_samples_monitoring_to_geojson AS
(SELECT
count_samples_monitoring.identifier_akvo,
count_samples_monitoring.instance::TEXT,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN count_samples_monitoring.test = 'test_data'
THEN 'yes'
WHEN count_samples_monitoring.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN count_samples_monitoring.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN count_samples_monitoring.test = 'xxxxx'
THEN 'yes'
WHEN count_samples_monitoring.test = ''
THEN 'no'
WHEN count_samples_monitoring.test = 'This is real, valid data\r'
THEN 'no'
WHEN count_samples_monitoring.test = 'valid_data'
THEN 'no'
WHEN count_samples_monitoring.test = 'Valid data'
THEN 'no'
WHEN count_samples_monitoring.test = 'This is real, valid data'
THEN 'no'
WHEN count_samples_monitoring.test ISNULL
THEN 'no'
END AS test,

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
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country),


-- Here we convert the COUNT sample AUDIT locations from WKT format to geojson string format that can be read by superset
wkt_count_samples_audit_to_geojson AS
(SELECT
count_samples_audit.identifier_akvo,
akvo_tree_monitoring_areas.instance::TEXT,
akvo_tree_registration_areas_updated.submission,

CASE
WHEN akvo_tree_monitoring_areas.test = 'test_data'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted.'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'This is a test, this record can be deleted'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = 'xxxxx'
THEN 'yes'
WHEN akvo_tree_monitoring_areas.test = ''
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data\r'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'valid_data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'Valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test = 'This is real, valid data'
THEN 'no'
WHEN akvo_tree_monitoring_areas.test ISNULL
THEN 'no'
END AS test,

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
akvo_tree_registration_areas_updated.id_planting_site,
akvo_tree_registration_areas_updated.display_name,
akvo_tree_registration_areas_updated.submission,
akvo_tree_registration_areas_updated.country)


SELECT * FROM wkt_polygons_to_geojson
UNION ALL
SELECT * FROM wkt_buffer_200_trees_areas_to_geojson
UNION ALL
SELECT * FROM wkt_pcq_samples_akvo_monitoring_to_geojson
UNION ALL
SELECT * FROM wkt_pcq_samples_akvo_odk_monitoring_to_geojson
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
AS (SELECT

AKVO_tree_distribution_unregistered_farmers.identifier_akvo,
AKVO_tree_distribution_unregistered_farmers.display_name,
AKVO_tree_distribution_unregistered_farmers.device_id,
AKVO_tree_distribution_unregistered_farmers.instance,
AKVO_tree_distribution_unregistered_farmers.submission,
AKVO_tree_distribution_unregistered_farmers.akvo_form_version::TEXT,
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

FROM AKVO_tree_distribution_unregistered_farmers

UNION ALL

SELECT

odk_unregistered_farmers_tree_handout_main.submissionid_odk AS identifier_akvo,
CONCAT(odk_unregistered_farmers_tree_handout_main.organisation, ' - ', odk_unregistered_farmers_tree_handout_main.contract_number, ' - ', odk_unregistered_farmers_tree_handout_main.name_location_tree_planting, ' - ', odk_unregistered_farmers_tree_handout_main.planting_site_id) AS display_name,
odk_unregistered_farmers_tree_handout_main.device_id,
0 AS instance,
odk_unregistered_farmers_tree_handout_main.submission_date AS submission,
odk_unregistered_farmers_tree_handout_main.odk_form_version,
'' AS country,
odk_unregistered_farmers_tree_handout_main.test,

-- Create a unique code for filtering in superset, based on main organisation name
CASE
WHEN odk_unregistered_farmers_tree_handout_main.organisation NOTNULL
THEN CAST(CONCAT(
	POWER(ASCII(LEFT(LOWER(odk_unregistered_farmers_tree_handout_main.organisation),1)),3),
	POWER(ASCII(LEFT(LOWER(odk_unregistered_farmers_tree_handout_main.organisation),2)),2),
	SQRT(POWER(ASCII(LEFT(LOWER(odk_unregistered_farmers_tree_handout_main.organisation),3)),4))) AS NUMERIC)
ELSE 0
END AS partnercode_main,

-- Create a unique code for filtering in superset, based on main sub-organisation name
CASE
WHEN POSITION('-' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN CAST(CONCAT(POWER(ASCII(RIGHT((LOWER(odk_unregistered_farmers_tree_handout_main.organisation)),
			LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - POSITION('-' IN odk_unregistered_farmers_tree_handout_main.organisation) - 1)),3),
		    POWER(ASCII(RIGHT((LOWER(odk_unregistered_farmers_tree_handout_main.organisation)),
			LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - POSITION('-' IN odk_unregistered_farmers_tree_handout_main.organisation) - 2)),2),
		   	POWER(ASCII(RIGHT((LOWER(odk_unregistered_farmers_tree_handout_main.organisation)),
			LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - POSITION('-' IN odk_unregistered_farmers_tree_handout_main.organisation) - 3)),2)) AS NUMERIC)
ELSE
0
END AS partnercode_sub,

LOWER(odk_unregistered_farmers_tree_handout_main.organisation) AS organisation,

CASE
WHEN POSITION('-' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(left(odk_unregistered_farmers_tree_handout_main.organisation, strpos(odk_unregistered_farmers_tree_handout_main.organisation, '-') - 2))
WHEN POSITION(' -' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(left(odk_unregistered_farmers_tree_handout_main.organisation, strpos(odk_unregistered_farmers_tree_handout_main.organisation, '-')))
WHEN POSITION('/' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(left(odk_unregistered_farmers_tree_handout_main.organisation, strpos(odk_unregistered_farmers_tree_handout_main.organisation, '/') - 1))
WHEN POSITION(' /' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(left(odk_unregistered_farmers_tree_handout_main.organisation, strpos(odk_unregistered_farmers_tree_handout_main.organisation, '/')))
ElSE
LOWER(odk_unregistered_farmers_tree_handout_main.organisation)
END AS partner,

CASE
WHEN POSITION('- ' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(right(odk_unregistered_farmers_tree_handout_main.organisation, (LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - strpos(odk_unregistered_farmers_tree_handout_main.organisation, '-')-1)))
WHEN POSITION('-' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(right(odk_unregistered_farmers_tree_handout_main.organisation, (LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - strpos(odk_unregistered_farmers_tree_handout_main.organisation, '-'))))
WHEN POSITION('/' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(right(odk_unregistered_farmers_tree_handout_main.organisation, (LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - strpos(odk_unregistered_farmers_tree_handout_main.organisation, '/'))))
WHEN POSITION('/ ' IN odk_unregistered_farmers_tree_handout_main.organisation) > 0
THEN LOWER(right(odk_unregistered_farmers_tree_handout_main.organisation, (LENGTH(odk_unregistered_farmers_tree_handout_main.organisation) - strpos(odk_unregistered_farmers_tree_handout_main.organisation, '/')-1)))
ElSE
''
END AS sub_partner,

odk_unregistered_farmers_tree_handout_main.contract_number AS sub_contract,
odk_unregistered_farmers_tree_handout_main.recipient_full_name AS name_tree_receiver,
odk_unregistered_farmers_tree_handout_main.recipient_gender AS gender_tree_receiver,
odk_unregistered_farmers_tree_handout_main.choice_tree_ownership AS check_ownership_trees,
'' AS check_ownership_land,
odk_unregistered_farmers_tree_handout_main.recipient_photo AS url_photo_receiver_trees,
odk_unregistered_farmers_tree_handout_main.id_recipient AS url_photo_id_card_tree_receiver,
odk_unregistered_farmers_tree_handout_main.name_location_tree_planting AS location_house_tree_receiver,
odk_unregistered_farmers_tree_handout_main.planting_site_id AS name_site_id_tree_planting,
'' AS confirm_planting_location,
odk_unregistered_farmers_tree_handout_main.total_tree_nr_handed_out AS total_number_trees_received,
'' AS url_signature_tree_receiver

FROM odk_unregistered_farmers_tree_handout_main);


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
CALC_TAB_tree_submissions_per_contract."Number of tree species registered",
CALC_TAB_tree_submissions_per_contract.percentage_native,
CALC_TAB_tree_submissions_per_contract.percentage_exotic,
CALC_TAB_tree_submissions_per_contract.percentage_unknown

FROM CALC_TAB_tree_submissions_per_contract;

--The column below is UPDATED by the following sql. This is to create clean contract numbers for Superset
ALTER TABLE superset_ecosia_contract_overview
ADD contract NUMERIC(10,0);

UPDATE superset_ecosia_contract_overview
SET contract = TRUNC(sub_contract);'''

conn.commit()

# Here we create a seperate table for KANOP analysis. We first going to detect where self-intersections are located
# Then we are going to correct the self-intersected polygons with an ST-buffer(0.0). In the geometric error detection script
# Then we run the overlap analysis again, but with the corrected self-intersected polygons. In the geometric error detection script
# With this, we can filter out ALL overlapping polygons, including the ones that have a self-intersection (and have an overlap)
#create_a51 = ('''
# CREATE TABLE akvo_tree_registration_areas_updated_remotesensing
# AS (SELECT * FROM akvo_tree_registration_areas_updated);
#
# ALTER TABLE akvo_tree_registration_areas_updated_remotesensing
# DROP COLUMN self_intersection,
# DROP COLUMN overlap,
# DROP COLUMN needle_shape,
# DROP COLUMN check_duplicate_polygons;
#
# ALTER TABLE akvo_tree_registration_areas_updated_remotesensing
# ADD polygon_corr_self_intersections geography (polygon, 4326),
# ADD self_intersection_before_corr_pol BOOLEAN,
# ADD self_intersection_after_corr_pol BOOLEAN,
# ADD overlap_before_self_intersection_corrections BOOLEAN,
# ADD overlap_after_self_intersection_corrections BOOLEAN,
# ADD needle_shape_before_self_interection_corrections BOOLEAN,
# ADD needle_shape_after_self_interection_corrections BOOLEAN,
# ADD check_duplicate_polygons_after_self_corrections TEXT,
# ADD check_duplicate_polygons_before_self_corrections TEXT;''')
#
# conn.commit()

create_a52 = '''CREATE TABLE IF NOT EXISTS superset_ecosia_kanop_chloris_results AS (SELECT
t1.identifier_akvo,
t1.organisation,
t1.id_planting_site,
t1.contract_number,
t1.year_of_analisis,
t1.chloris_above_ground_dry_biomass,
t2.kanop_above_ground_living_biomass AS kanop_above_ground_dry_biomass

FROM (SELECT
identifier_akvo,
organisation,
contract_number AS contract_number,
id_planting_site,
year_of_analisis AS year_of_analisis,
forest_agb_stock_per_year_mt AS chloris_above_ground_dry_biomass
FROM superset_ecosia_CHLORIS_polygon_results) t1

JOIN

(SELECT
identifier_akvo,
name_project AS contract_number,
'unknown' AS id_planting_site,
year_of_analisis AS year_of_analisis,
request_measurement_date AS year_analysis,
livingabovegroundbiomass_present AS kanop_above_ground_living_biomass
FROM superset_ecosia_kanop_polygon_level_1_moment) t2
ON t1.identifier_akvo = t2.identifier_akvo
and t1.year_of_analisis = t2.year_of_analisis);'''

conn.commit()


# This login (see below) and the associated grands is being used by the QGIS users
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
GRANT SELECT ON TABLE superset_ecosia_contract_overview TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_new_devices TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_firms_historic_fires TO ecosia_superset;
GRANT SELECT ON TABLE superset_ecosia_kanop_chloris_results TO ecosia_superset;
GRANT SELECT ON TABLE akvo_tree_registration_areas_edits TO ecosia_superset;

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
DROP POLICY IF EXISTS ecosia_superset_policy ON superset_ecosia_kanop_chloris_results;
DROP POLICY IF EXISTS ecosia_superset_policy ON akvo_tree_registration_areas_edits;

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
ALTER TABLE superset_ecosia_kanop_chloris_results enable ROW LEVEL SECURITY;
ALTER TABLE akvo_tree_registration_areas_edits enable ROW LEVEL SECURITY;

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
CREATE POLICY ecosia_superset_policy ON superset_ecosia_firms_historic_fires TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON superset_ecosia_kanop_chloris_results TO ecosia_superset USING (true);
CREATE POLICY ecosia_superset_policy ON akvo_tree_registration_areas_edits TO ecosia_superset USING (true);'''

conn.commit()

#This login (see below) and the associated grands is being used by the superset dashboard!! AS such this query is de-activated.
create_a21_ecosia_editing = '''
DROP POLICY IF EXISTS ecosia_edit_policy ON akvo_tree_registration_areas_edits;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ecosia_editing;

GRANT USAGE ON SCHEMA public TO ecosia_editing;
GRANT USAGE ON SCHEMA heroku_ext TO ecosia_editing;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ecosia_editing;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA heroku_ext TO ecosia_editing;

GRANT SELECT ON TABLE public.akvo_tree_registration_areas_edits TO ecosia_editing;
GRANT SELECT ON ALL TABLES IN SCHEMA heroku_ext TO ecosia_editing;

GRANT SELECT ON geometry_columns TO ecosia_editing;
GRANT SELECT ON spatial_ref_sys TO ecosia_editing;

--GRANT CONNECT ON DATABASE d7ln221qt6944k TO ecosia_editing;

--GRANT SELECT ON geometry_columns TO ecosia_editing;
--GRANT SELECT ON spatial_ref_sys TO ecosia_editing;
--GRANT SELECT ON geography_columns TO ecosia_editing;

ALTER TABLE akvo_tree_registration_areas_edits enable ROW LEVEL SECURITY;

--ALTER DEFAULT PRIVILEGES IN SCHEMA heroku_ext GRANT SELECT ON TABLES TO ecosia_editing;

CREATE POLICY ecosia_edit_policy ON akvo_tree_registration_areas_edits TO ecosia_editing USING (true);

'''

conn.commit()


# Execute drop tables
cur.execute(drop_tables)

conn.commit()


# Execute create tables
cur.execute(create_a1_integrated)
cur.execute(create_a1_updated)
cur.execute(create_a1_updates_from_odk_akvo_server_side_updated)
cur.execute(create_a1_insertion)
cur.execute(create_a1_edit)
cur.execute(create_a1_integrate_new_data)
cur.execute(create_a1_updates_from_odk_akvo_server_side_edits)
cur.execute(create_a1_edit_integration)
cur.execute(create_a1_updates_from_updated_to_edits_geometric_corr)
cur.execute(create_a1_remote_sensing_results)
cur.execute(create_a1_remote_sensing_update)
cur.execute(create_a2_akvo)
cur.execute(create_a2_odk)
cur.execute(create_a2_merge_akvo_odk)
cur.execute(create_a3)
cur.execute(create_a6)
cur.execute(create_a14)
cur.execute(create_a15)
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
#cur.execute(create_a51)
cur.execute(create_a52)

cur.execute(create_a20_ecosia_superset) # This gives grand access to QGIS users. With this login (inside QGIS) they will only see the superset tables
cur.execute(create_a21_ecosia_editing) # Used by the Preset dashboard. No grand limitation. As such it is de-activated

conn.commit()

cur.close()

print('all queries are processed')
