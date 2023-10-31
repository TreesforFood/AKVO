# THis script needs to be executed after a full download of the AKVO database.

import requests
import json
import re
import geojson
import geodaisy.converters as convert
from area import area
import psycopg2
import os
import cloudpickle
from ogr2ogr import ogr
#import ogr2ogr

print("The Python script that is running is version: 6.7")

# get the token from AKVO
data = {"client_id": os.environ["CLIENT_ID"], "username" : os.environ["USERNAME"], "password": os.environ["PASSWORD"], "grant_type": os.environ["GRANT_TYPE"], "scope": os.environ["SCOPE"]}
response = requests.post("https://akvofoundation.eu.auth0.com/oauth/token", data=data)

if response.status_code in [200]: # in case good response from AKVO server
    tok_dict = json.loads(response.text)
    expires_in = tok_dict["expires_in"]
    token_type = tok_dict["token_type"]
    access_token = tok_dict["access_token"]
    token_id = tok_dict["id_token"]
else: # in case of error from AKVO server
    print(response.text)

headers = {'Authorization': "Bearer {}".format(token_id), 'Accept': 'application/vnd.akvo.flow.v2+json'}

#print(url_list)
#connect to Postgresql database
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode='require')

cur = conn.cursor()

cur.execute('''Select sync_url FROM url_latest_sync where id=1;''')
fetch_latest_sync_url = cur.fetchall()
for latest_sync_url in fetch_latest_sync_url:
    open_sync_url = latest_sync_url[0]
    print("Latest sync url used to start sync: ", open_sync_url)

url_list = list()
url_list.append(open_sync_url)


get_status_akvo_server = requests.get(open_sync_url, headers=headers) # gives ouyput in bytes

if get_status_akvo_server.status_code in [200]:
    print("Connection with server was successful (code: 200)")
    for url_sync in url_list:
        #print("See type: ", url_sync, "Type: ", type(url_sync))
        store_nextsyncurl = requests.get(url_sync, headers=headers).content
        if len(store_nextsyncurl) > 0:
            page_decode_urls = store_nextsyncurl.decode()
            json_instance_urls = json.loads(page_decode_urls)
            try:
                url_subseq_page = json_instance_urls.get('nextSyncUrl')
                print("Next sync url: ", url_subseq_page)
                url_list.append(url_subseq_page)
            except (KeyError, IndexError): # Most likely this is not needed. As long as 'store_nextsyncurl' has a value (>0) there will always be a "NextSyncUrl". That is why a 'len(store_nextsyncurl) == 0:' exception was build in to capture the latest sync url.
                cur.execute('''UPDATE url_latest_sync SET sync_url = %s WHERE id = %s;''', (url_sync, 1))
                conn.commit()
                print("New sync url stored in database after ending the sync: ", url_sync)
                break
        if len(store_nextsyncurl) == 0:
            cur.execute('''UPDATE url_latest_sync SET sync_url = %s WHERE id = %s;''', (url_sync, 1))
            conn.commit()
            print("New sync url STORED in database after ending the sync: ", url_sync)
            break

else:
    print("Connection with server failed. Server status code received: ", get_status_akvo_server.status_code)


# create list_sync
changes_form_48090001_lst = list()
changes_form_11980001_lst = list()
changes_form_30050006_lst = list()
changes_form_6070006_lst = list()
changes_form_141500001_lst = list()
changes_form_252327848_lst = list() # Tree distribution of unregistered farmers
changes_form_258280042_lst = list() # Tree registration of unregistered farmers
changes_form_242817396_lst = list() # Light tree registration form (easy registration)


# Check and remove double id's by sets
unique_lst_48090001 = set()
unique_lst_11980001 = set()
unique_lst_30050006 = set()
unique_lst_6070006 = set()
unique_lst_141500001 = set()
unique_lst_252327848 = set() # Tree distribution of unregistered farmers
unique_lst_258280042 = set() # Tree registration of unregistered farmers
unique_lst_242817396 = set() # Light tree registration form (easy registration)


# function to truncate for year
def left(var, amount):
    return var[:amount]

# function to truncate (remove) time from date
def mid(var,begin,end):
    return var[begin:end]


for open_syncurl in url_list:
    load_nextsyncurl = requests.get(open_syncurl, headers=headers).content
    if len(load_nextsyncurl) > 0:
        page_decode = load_nextsyncurl.decode()
        json_instance = json.loads(page_decode)
        print("TOTAL JSON IN: ", json_instance)

        try:
            for get_changes_form in json_instance['changes']['formInstanceChanged']:
                print("ONLY THE CHANGED JSON: ", get_changes_form)
                if len(get_changes_form) > 0:
                    if get_changes_form.get('formId') == '48090001': # Changes in the registration of planting sites
                        instance_to_change_48090001 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_48090001 not in unique_lst_48090001:
                            unique_lst_48090001.add(instance_to_change_48090001)
                            #print('CHECK SET: ',unique_lst_48090001)
                            changes_form_48090001_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '11980001': # Changes in the monitoring of planting sites
                        instance_to_change_11980001 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_11980001 not in unique_lst_11980001:
                            unique_lst_11980001.add(instance_to_change_11980001)
                            #print('CHECK SET: ',unique_lst_11980001)
                            changes_form_11980001_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '30050006': # Changes in the registration of nurseries
                        instance_to_change_30050006 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_30050006 not in unique_lst_30050006:
                            unique_lst_30050006.add(instance_to_change_30050006)
                            #print('CHECK SET: ',unique_lst_30050006)
                            changes_form_30050006_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '6070006': # Changes in the monitoring of nurseries
                        instance_to_change_6070006 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_6070006 not in unique_lst_6070006:
                            unique_lst_6070006.add(instance_to_change_6070006)
                            #print('CHECK SET: ',unique_lst_6070006)
                            changes_form_6070006_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '141500001': # Changes in the registration external audits
                        instance_to_change_141500001 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_141500001 not in unique_lst_141500001:
                            unique_lst_141500001.add(instance_to_change_141500001)
                            #print('CHECK SET: ',unique_lst_141500001)
                            changes_form_141500001_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '252327848': # Changes in the Tree distribution of unregistered farmers
                        instance_to_change_252327848 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_252327848 not in unique_lst_252327848:
                            unique_lst_252327848.add(instance_to_change_252327848)
                            #print('CHECK SET: ',unique_lst_141500001)
                            changes_form_252327848_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '258280042': # Changes in the Tree registration of unregistered farmers
                        instance_to_change_258280042 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_258280042 not in unique_lst_258280042:
                            unique_lst_258280042.add(instance_to_change_258280042)
                            #print('CHECK SET: ',unique_lst_141500001)
                            changes_form_258280042_lst.append(get_changes_form)
                        else:
                            continue

                    elif get_changes_form['formId'] == '242817396': # Changes in the light registration form
                        instance_to_change_242817396 = get_changes_form['id']
                        # Seems that the sync file sometimes gives back double id's. Maybe due to the fact that 'change-ids' are both new uploads and changes(?). Another issue can be that the same instance appears in multiple sync urls. It is only one time deleted, but the multple instances will be still in the loop and added to the database
                        if instance_to_change_242817396 not in unique_lst_242817396:
                            unique_lst_242817396.add(instance_to_change_242817396)
                            #print('CHECK SET: ',unique_lst_141500001)
                            changes_form_242817396_lst.append(get_changes_form)
                        else:
                            continue

        except (KeyError, IndexError):
            get_changes_form = ''



        # Remove the deleted instances (received from the DELETION JSON). Deletion goes on instance level. Only the instance number of the deleted instance is given in the sync file
        try:
            for level1_delete in json_instance['changes']['formInstanceDeleted']:
                #print("Deleted instances: ", level1_delete)
                if len(level1_delete) > 0:
                    level1_delete = int(level1_delete)

                    cur.execute('''DELETE FROM akvo_tree_registration_areas WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_registration_photos WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_registration_species WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_registration_areas_baseline_counts WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_registration_areas_baseline_pcq WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_monitoring_areas WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_monitoring_pcq WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_monitoring_counts WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_monitoring_photos WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_monitoring_remapped_areas WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Nursery_registration WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Nursery_registration_photos WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Nursery_monitoring WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Nursery_monitoring_photos WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Nursery_monitoring_tree_species WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_external_audits_areas WHERE instance = (%s);''', (level1_delete,))
                    #conn.commit()

                    cur.execute('''DELETE FROM AKVO_Tree_external_audits_pcq WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_external_audits_counts WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_distribution_unregistered_farmers WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_distribution_unregistered_tree_species WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_site_registration_distributed_trees WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_site_registration_distributed_trees_photos WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_site_registration_distributed_trees_species WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_registration_locations_light_version WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_registration_species_light_version WHERE instance = (%s);''', (level1_delete,))


                    cur.execute('''DELETE FROM AKVO_Tree_registration_photos_light_version WHERE instance = (%s);''', (level1_delete,))


                    conn.commit()

                else:
                    level1_delete = ''

        except:
            level1_delete = ''

# Remove the changed instances first before adding the modified ones to the database. This seems to be the
# most straight forward and secure way to do it (instead of using INSERT OR REPLACE OR UPDATE OF UPSERT...)
# These commands do not seem to work as modifications might be made on a repeat within a monitoring. Repeats do not have an id
# so distinguish them is not possible. That is why a REPLACE or SELECT is not possible, as the replacement (as well as the syncing)
# is on a row by row basis. So the replaced row could be an instance that was just added the update loop before...

# Despite the fact that FOREIGN KEYS are defined, it seems that deleting the primary key values also deletes the corresponding
# FOREIGN KEYS (in the many tables) when doing this directly on the database. However, a DELETE from within Python
# coding does not seem to work... The FOREIGN KEY values (many tables) keep existing. Very strange. So in order to make sure all FOREIGN KEYS are deleted in all many tables, we
# need to hard-code the DELETE SQL for every many table....

print('LIST REGISTRATIONS GOING IN: ', changes_form_48090001_lst)

# Here we first delete an instance before updating it again in a next for-loop (= changes). These ID's come from the UPDATE SYNC JSON
for level1_change in changes_form_48090001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_areas WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE REGISTRATIONS: ", instance_to_delete)
    except:
        break

for level1_change in changes_form_48090001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_photos WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE REGISTRATIONS: ", instance_to_delete)
    except:
        break

for level1_change in changes_form_48090001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_species WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE REGISTRATIONS: ", instance_to_delete)
    except:
        break

for level1_change in changes_form_48090001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_areas_baseline_counts WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE REGISTRATIONS: ", instance_to_delete)
    except:
        break

for level1_change in changes_form_48090001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_areas_baseline_pcq WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE REGISTRATIONS: ", instance_to_delete)
    except:
        break

for level1_change in changes_form_11980001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM akvo_tree_monitoring_areas WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE MONITORING: ", instance_to_delete)
    except:
        break

for level1_change in changes_form_11980001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_monitoring_pcq WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE MONITORING PCQ: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_11980001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_monitoring_counts WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE MONITORING SPECIES: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_11980001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_monitoring_photos WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE MONITORING: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_11980001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_monitoring_remapped_areas WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES TREE MONITORING: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_30050006_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Nursery_registration WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES NURSRERY REGISTRATIONS: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_30050006_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Nursery_registration_photos WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES NURSRERY REGISTRATIONS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_6070006_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Nursery_monitoring WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES NURSRERY REGISTRATIONS: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_6070006_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Nursery_monitoring_photos WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES NURSRERY REGISTRATIONS: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_6070006_lst:
    try:
        instance_to_delete = level1_change['id']

        cur.execute('''DELETE FROM AKVO_Nursery_monitoring_tree_species WHERE instance = (%s);''', (instance_to_delete,))

        conn.commit()
        #print("DELETED CHANGES NURSRERY REGISTRATIONS: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_141500001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_external_audits_areas WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break

for level1_change in changes_form_141500001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_external_audits_pcq WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_141500001_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_external_audits_counts WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_252327848_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_distribution_unregistered_farmers WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_252327848_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_distribution_unregistered_tree_species WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_258280042_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_site_registration_distributed_trees WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_258280042_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_site_registration_distributed_trees_photos WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_258280042_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_site_registration_distributed_trees_species WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_242817396_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_locations_light_version WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_242817396_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_species_light_version WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break


for level1_change in changes_form_242817396_lst:
    try:
        instance_to_delete = level1_change['id']
        cur.execute('''DELETE FROM AKVO_Tree_registration_photos_light_version WHERE instance = (%s);''', (instance_to_delete,))
        conn.commit()
        #print("DELETED CHANGES AUDITS: ", instance_to_delete)
    except KeyError:
        break



# Get all the CHANGED/UPDATED tree registration data
for level1 in changes_form_48090001_lst:
    print('Instance registration being updated :', level1)
    #print('CHECKING IF THE OUTCOME IS GOOD: ',level1)
    modifiedat = level1['modifiedAt']

    try:
        formversion = level1['formVersion']
    except (KeyError, IndexError):
        formversion = ''

    try:
        identifier = level1['identifier']
    except (KeyError, IndexError):
        identifier = ''

    try:
        displayname = level1['displayName']
    except (KeyError, IndexError):
        displayname = ''

    try:
        deviceidentifier = level1['deviceIdentifier']
    except (KeyError, IndexError):
        deviceidentifier = ''

    instance = level1['id']

    submissiondate = level1['submissionDate']

    submissiontime = mid(submissiondate, 11,19)

    try:
        submissiondate_trunc = left(submissiondate,10)
    except:
        submissiondate_trunc = ''

    try:
        submissiondate_trunc_year = left(submissiondate,4)
    except:
        submissiondate_trunc_year = ''

    try:
        submitter = level1['submitter']
    except (KeyError, IndexError):
        submitter = ''

    try:
        country = level1['responses']['1960001'][0]['42120001'][0]['name']
    except (KeyError, IndexError):
        country = ''

    try:
        test = level1['responses']['1960001'][0]['50100002'][0]['text']
    except (KeyError, IndexError):
        test = ''

    try:
        name_organisation = level1['responses']['1960001'][0]['42120001'][1]['name']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        name_organisation = ''

    try:
        contract_number = level1['responses']['1960001'][0].get('52070068', 0.0)
    except (KeyError, IndexError):
        contract_number = 0.0

    try:
        id_planting_site = level1['responses']['1960001'][0].get('58000002','')
    except (KeyError, IndexError):
        id_planting_site = ''

    try:
        land_title = level1['responses']['1960001'][0]['52070069'][0]['text']
    except (KeyError, IndexError): # Since landtitle has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
        land_title = ''

    try:
        name_village = level1['responses']['1960001'][0].get('61910570','')
    except (KeyError, IndexError):
        name_village = ''

    try:
        name_region = level1['responses']['1960001'][0].get('44110002','')
    except (KeyError, IndexError):
        name_region = ''

    try:
        name_owner = level1['responses']['1960001'][0].get('54050003','')
    except (KeyError, IndexError):
        name_owner = ''

    try:
        photo_owner = level1['responses']['1960001'][0]['31840003']['filename']
    except (KeyError, IndexError):
        photo_owner = ''

    try:
        gender_owner = level1['responses']['1960001'][0]['42120003'][0]['text']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        gender_owner = ''

    try:
        landuse_objective = level1['responses']['1960001'][0]['25860010'][0]['text']
    except (KeyError, IndexError): # Has option 'other' so build in keyerror option
        landuse_objective = ''

    try:
        site_preparation = level1['responses']['1960001'][0]['15910006'][0]['text']
    except (KeyError, IndexError): # Since site preparation has 'other' as option, the list will always be created. As such, there will never be an IndexError. However, it might still be that no value is submitted. In that case the Key will not be found ( as the list will be empty)
        site_preparation = ''

    try:
        planting_technique = level1['responses']['1960001'][0]['42120008'][0]['text']
    except (KeyError, IndexError): # Has option 'other' so build in keyerror option
        planting_technique = ''

    try:
        planting_system = level1['responses']['1960001'][0]['50100006'][0]['text']
    except (KeyError, IndexError): # Has option 'other' so build in keyerror option
        planting_system = ''

    try:
        remark = level1['responses']['1960001'][0].get('50120004','')
    except (KeyError, IndexError):
        remark = ''

    try:
        more_less_200_trees = level1['responses']['1960001'][0]['50120005'][0].get('text','')
    except (KeyError, IndexError):
        more_less_200_trees = ''

    try:
        nr_trees_planted = level1['responses']['56230114'][0].get('3990003')
    except (KeyError, IndexError):
        nr_trees_planted = None

    try:
        if level1['responses']['56230114'][0].get('54050005','') != None:
            planting_date = level1['responses']['56230114'][0].get('54050005','')
            planting_date_trunc = left(planting_date,10)
        else:
            planting_date = ''
    except (KeyError, IndexError):
        planting_date = ''


    try:
        estimated_area = level1['responses']['56230114'][0].get('39860006')
    except (KeyError, IndexError):
        estimated_area = None

    try:
        lat_centr = level1['responses']['1960007'][0]['25860015']['lat'] # It seems that Lat can be None, so the key will be found and 'None' is parsed into a string...?
    except (KeyError, IndexError):
        lat_centr = None
    else:
        lat_centr = level1['responses']['1960007'][0]['25860015']['lat']

    try:
        lon_centr = level1['responses']['1960007'][0]['25860015']['long']
    except (KeyError, IndexError):
        lon_centr = None
    else:
        lon_centr = level1['responses']['1960007'][0]['25860015']['long']

    if lat_centr is None or lon_centr is None:
        centroid_coord = None
    elif lat_centr == 'None' or lon_centr == 'None':
        centroid_coord = None
    else:
        lat_centr_conv = str(lat_centr)
        lon_centr_conv = str(lon_centr)
        centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'

    try:
        geom_get = level1['responses']['1960007'][0]['50110008'] # Up to this level it can go wrong (due to empty entry)
        if geom_get != None:
            geom_get = level1['responses']['1960007'][0]['50110008']['features'][0].get('geometry','')
            area_ha = area(geom_get)
            area_ha = round((area_ha/10000),3)
            geometry = convert.geojson_to_wkt(geom_get)
            get_geom_type = geometry.split(' ',1)

            if get_geom_type[0] == 'POLYGON':
                polygon_check = convert.geojson_to_wkt(geom_get)
                polygon_z = ogr.CreateGeometryFromWkt(polygon_check)
                polygon_z.FlattenTo2D()
                polygon_z.ExportToWkt()
                polygon_z = str(polygon_z.ExportToWkt())
                coord = re.findall('\s', polygon_check)
                number_coord_pol = int((len(coord)/2)-1)
                if number_coord_pol < 3:
                    polygon = None
                    multipoint = None
                else:
                    polygon = polygon_z
                    multipoint = None

            elif get_geom_type[0] == 'MULTIPOINT':
                multipoint = convert.geojson_to_wkt(geom_get)
                polygon = None

        else:
            geom_get = None
            area_ha = None
            multipoint = None
            polygon = None

    except (IndexError, KeyError):
        polygon = None
        coord = None
        number_coord_pol = None
        multipoint = None
        geom_get = None
        area_ha = None
        geometry = None
        get_geom_type = None


    # Populate the tree registration table
    #try:

    cur.execute('''INSERT INTO akvo_tree_registration_areas (identifier_akvo, display_name, device_id, instance, submission, submission_year, submissiontime, submitter, modifiedAt, AKVO_form_version, country, test, organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, photo_owner, gender_owner, objective_site, site_preparation, planting_technique, planting_system, remark, nr_trees_option, planting_date, tree_number, estimated_area, calc_area, lat_y, lon_x, number_coord_polygon, centroid_coord, polygon, multipoint) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', (identifier, displayname, deviceidentifier, instance, submissiondate_trunc, submissiondate_trunc_year, submissiontime, submitter, modifiedat, formversion, country, test, name_organisation, contract_number, id_planting_site, land_title, name_village, name_region, name_owner, photo_owner, gender_owner, landuse_objective, site_preparation, planting_technique, planting_system, remark, more_less_200_trees, planting_date_trunc, nr_trees_planted, estimated_area, area_ha, lat_centr, lon_centr, number_coord_pol, centroid_coord, polygon, multipoint))

    conn.commit()

    try:
        level1['responses']['1960007']

    except KeyError:
        photo_r4_location = None
        photo_r4_url = None
        photo_r4_gps_location = None
    else:
        for photo in level1['responses']['1960007']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
            photo.pop('5900011', None)
            photo.pop('50110008', None)
            photo.pop('25860015', None)

            for photo_value in photo.values():
                photo_items4=[]
                if photo_value is not None:
                    photo_items4.append(photo_value)

                for url4 in photo_items4:
                    if url4['filename'] is not None:
                        photo_r4_url = url4['filename']
                        try:
                            url4['location']
                        except KeyError:
                            photo_r4_location = None
                        else:
                            if url4['location'] is not None:
                                photo_lat1 = url4['location']['latitude']
                                photo_lon1 = url4['location']['longitude']
                                photo_lat = str(photo_lat1)
                                photo_lon = str(photo_lon1)
                                photo_r4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                                photo_r4_gps_location = None
                            else:
                                photo_r4_location = None
                                photo_r4_gps_location = None

                    else:
                        photo_r4_url = None
                        photo_r4_location = None
                        photo_r4_gps_location = None


                    #print('photo:',photo_url, photo_location, identifier, count) Prints well multiple photos and id's up to here.
                    cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_geotag_location, photo_gps_location)
                    VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, photo_r4_url, photo_r4_location, photo_r4_gps_location))

                    conn.commit()


    try:
        for photo in level1['responses']['3990009']: # Get other 36 photos from registration form. This loop was tested in file: AKVO_database_download_v7_test_rest_36_reg_photos.py
            photo_items36 = list(photo.values())
            for url36 in photo_items36:
                try:
                    photo_r36_url = url36['filename']
                except KeyError:
                    photo_r36_url = None
                    photo_r36_gps_location = None
                    photo_r36_location = None

                else:
                    photo_r36_url = url36['filename']
                    try:
                        if url36['location'] is not None:
                            photo_lat = url36['location']['latitude']
                            photo_lat = str(photo_lat)
                            photo_lon = url36['location']['longitude']
                            photo_lon = str(photo_lon)
                            photo_r36_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                            photo_r36_gps_location = None

                        else:
                            photo_lat = None
                            photo_lon = None
                            photo_r36_location = None
                            photo_r36_gps_location = None

                    except KeyError:
                        photo_lat = None
                        photo_lon = None
                        photo_r36_location = None
                        photo_r36_gps_location = None

                cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_geotag_location, photo_gps_location)
                VALUES (%s,%s,%s,%s,%s);''', (identifier, instance, photo_r36_url, photo_r36_location, photo_r36_gps_location))
                conn.commit()



    except (IndexError,KeyError):
        photo = ''
        photo_r36_url = ''
        photo_r36_location = ''

    # Include photos from the GPS points. These questions were introduced from the form version 130 onwards
    try:
        level1['responses']['282760413']

    except (KeyError):
        photo_repeat_url = ''
        photo_repeat_gps_point = ''

    else:

        photo_list_gps = []

        for photo_gps in level1['responses'].get('282760413'):
            photo_list_gps.clear()

            try:
                note_answer = photo_gps['293111364']
            except(KeyError):
                note_answer = None
            try:
                photo_repeat_url_get1 = photo_gps['277761190']['filename']
                photo_list_gps.append(photo_repeat_url_get1)
            except(KeyError):
                photo_repeat_url_get1 = None
            try:
                photo_repeat_url_get2 = photo_gps['287121276']['filename']
                photo_list_gps.append(photo_repeat_url_get2)
            except(KeyError):
                photo_repeat_url_get2 = None
            try:
                photo_repeat_url_get3 = photo_gps['290900474']['filename']
                photo_list_gps.append(photo_repeat_url_get3)
            except(KeyError):
                photo_repeat_url_get3 = None
            try:
                photo_repeat_url_get4 = photo_gps['304590496']['filename']
                photo_list_gps.append(photo_repeat_url_get4)
            except(KeyError):
                photo_repeat_url_get4 = None
            try:
                gps_lon = photo_gps['308430745']['long']
            except(KeyError):
                gps_lon = None
            try:
                gps_lat = photo_gps['308430745']['lat']
            except(KeyError):
                gps_lat = None

            if gps_lon is not None and gps_lat is not None:
                photo_repeat_gps_point_y = str(gps_lat)
                photo_repeat_gps_point_x = str(gps_lon)
                photo_gps_location = 'POINT('+ photo_repeat_gps_point_x + ' ' + photo_repeat_gps_point_y + ')'
            else:
                photo_gps_location = None

            for populate_photos_gps in photo_list_gps:
                #print(populate_photos_gps, photo_repeat_gps_point)
                photo_repeat_url = populate_photos_gps
                photo_geotag_location = None

                cur.execute('''INSERT INTO AKVO_Tree_registration_photos (identifier_akvo, instance, photo_url, photo_geotag_location, photo_gps_location)
                VALUES (%s,%s,%s,%s,%s)''', (identifier, instance, photo_repeat_url, photo_geotag_location, photo_gps_location))

                conn.commit()

    # Create table for the registration of tree species and number per species. This is a loop within the instance
    try:
        level1['responses']['50330190']

    except KeyError:
        species_latin = ''
        species_local = ''
        number_species = 0

    else:
        for x in level1['responses']['50330190']:
            try:
                species_latin = x['50340047'][1]['code']
            except (KeyError, IndexError):
                try:
                    species_latin = x['50340047'][0]['code']
                except (KeyError, IndexError):
                    species_latin = None
                else:
                    species_latin = x['50340047'][0]['code']
            else:
                species_latin = x['50340047'][1]['code']

            try:
                species_local = x['50340047'][1]['name']
            except (KeyError, IndexError):
                try:
                    species_local = x['50340047'][0]['name']
                except (KeyError, IndexError):
                    species_local = None
                else:
                    species_local = x['50340047'][0]['name']
            else:
                species_local = x['50340047'][1]['name']

            number_species = x.get('50530001', 0)
            #print(code, name, get_number_species)


            cur.execute('''INSERT INTO AKVO_Tree_registration_species (identifier_akvo, instance, lat_name_species, local_name_species, number_species)
            VALUES (%s,%s,%s,%s,%s);''', (identifier, instance, species_latin, species_local, number_species))
            conn.commit()

    # Create table for the PCQ data from the baseline measurement
    try:
        level1['responses']['264220236']
    except KeyError:
        pass

    else:
        for pcq_results_baseline in level1['responses']['264220236']:
            try:
                lat_sample_pcq = pcq_results_baseline['264250152']['lat']
            except (IndexError,KeyError):
                lat_sample_pcq = None
                lat_sample_pcq_str = None
            else:
                lat_sample_pcq = pcq_results_baseline['264250152']['lat']
                lat_sample_pcq_str = str(lat_sample_pcq)

            try:
                lon_sample_pcq = pcq_results_baseline['264250152']['long']
            except (IndexError,KeyError):
                lon_sample_pcq = None
                lon_sample_pcq_str = None
            else:
                lon_sample_pcq = pcq_results_baseline['264250152']['long']
                lon_sample_pcq_str = str(lon_sample_pcq)

            if not lat_sample_pcq_str or not lon_sample_pcq_str:
                pcq_location = None
            else:
                pcq_location = 'POINT('+ lon_sample_pcq_str + ' ' + lat_sample_pcq_str + ')'

            try:
                Q1_distance = pcq_results_baseline['243640004']
            except (IndexError,KeyError):
                Q1_distance = None
            try:
                Q1_height = pcq_results_baseline['264220326']
            except (IndexError,KeyError):
                Q1_height = None
            try:
                Q1_circom = pcq_results_baseline['266050247']
            except (IndexError,KeyError):
                Q1_circom = None

            try:
                Q1_species = pcq_results_baseline['262700255']
            except (IndexError,KeyError):
                Q1_species = ''

            try:
                Q2_distance = pcq_results_baseline['248370163']
            except (IndexError,KeyError):
                Q2_distance = None
            try:
                Q2_height = pcq_results_baseline['254340003']
            except (IndexError,KeyError):
                Q2_height = None
            try:
                Q2_circom = pcq_results_baseline['250980255']
            except (IndexError,KeyError):
                Q2_circom = None

            try:
                Q2_species = pcq_results_baseline['205600055']
            except (IndexError,KeyError):
                Q2_species = ''

            try:
                Q3_distance = pcq_results_baseline['251000184']
            except (IndexError,KeyError):
                Q3_distance = None
            try:
                Q3_height = pcq_results_baseline['243630282']
            except (IndexError,KeyError):
                Q3_height = None
            try:
                Q3_circom = pcq_results_baseline['247430289']
            except (IndexError,KeyError):
                Q3_circom = None

            try:
                Q3_species = pcq_results_baseline['262710003']
            except (IndexError,KeyError):
                Q3_species = ''

            try:
                Q4_distance = pcq_results_baseline['264220238']
            except (IndexError,KeyError):
                Q4_distance = None
            try:
                Q4_height = pcq_results_baseline['245670203']
            except (IndexError,KeyError):
                Q4_height = None
            try:
                Q4_circom = pcq_results_baseline['232710250']
            except (IndexError,KeyError):
                Q4_circom = None

            try:
                Q4_species = pcq_results_baseline['256740291']
            except (IndexError,KeyError):
                Q4_species = ''


            cur.execute('''INSERT INTO AKVO_Tree_registration_areas_baseline_pcq (identifier_akvo, instance, lat_pcq_sample, lon_pcq_sample, Q1_dist, Q1_hgt, Q1_circom, Q1_spec, Q2_dist, Q2_hgt, Q2_circom, Q2_spec, Q3_dist, Q3_hgt, Q3_circom, Q3_spec, Q4_dist, Q4_hgt, Q4_circom, Q4_spec, pcq_location)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, lat_sample_pcq, lon_sample_pcq, Q1_distance, Q1_height, Q1_circom, Q1_species, Q2_distance, Q2_height, Q2_circom, Q2_species, Q3_distance, Q3_height, Q3_circom, Q3_species, Q4_distance, Q4_height, Q4_circom, Q4_species, pcq_location))

            conn.commit()

    # Create table for the COUNT data from the baseline measurement
    try:
        level1['responses']['215991355']
    except KeyError:
        pass

    else:
        for x in level1['responses']['215991355']:

            try:
                number_mature_trees = x.get('264811956')
            except (IndexError,KeyError):
                number_mature_trees = None

            try:
                avg_diameter_dbh = x.get('264851349')
            except (IndexError,KeyError):
                avg_diameter_dbh = None

            try:
                avg_height = x.get('260610793')
            except (IndexError,KeyError):
                avg_height = None

            try:
                example_photo = x['253251438']['filename']
            except (IndexError,KeyError):
                example_photo = None


            cur.execute('''INSERT INTO AKVO_Tree_registration_areas_baseline_counts (identifier_akvo, instance, number_mature_trees,
            avg_diameter_dbh, avg_height, example_photo)
            VALUES (%s,%s,%s,%s,%s,%s)''', (identifier, instance, number_mature_trees,
            avg_diameter_dbh, avg_height, example_photo))

            conn.commit()


# Get all the tree monitoring changes
for level1_monitoring in changes_form_11980001_lst:
    submitter_m = level1_monitoring['submitter']
    identifier_m = level1_monitoring['identifier']
    displayname_m = level1_monitoring['displayName']
    device_id_m = level1_monitoring['deviceIdentifier']
    instance_m = level1_monitoring['id']
    submissiondate_m = level1_monitoring['submissionDate']
    try:
        submissiondate_trunc_m = left(submissiondate_m,10)
    except:
        submissiondate_trunc_m = ''

    try:
        submission_year_m = left(submissiondate_m,4)
    except:
        submission_year_m = ''

    formversion_m = level1_monitoring['formVersion']

    try:
        impression_site = level1_monitoring['responses']['50110001'][0]['5900001']
    except KeyError:
        impression_site = ''

    try:
        testing_m = level1_monitoring['42120002'][0]['text']
    except (IndexError,KeyError):
        testing_m = ''

    # Part of the Raw Data sheet questions: Count Method
    try:
        avg_tree_height = level1_monitoring['responses']['50110001'][0]['40300003']
    except (IndexError,KeyError):
        avg_tree_height = None

    try:
        tot_nr_trees_estimate = level1_monitoring['responses']['50110001'][0]['25860003']
    except (IndexError,KeyError):
        tot_nr_trees_estimate = None

    try:
        select_method = level1_monitoring['responses']['50110001'][0]['25860004'][0]['text']
    except (IndexError,KeyError):
        select_method = ''

    try:
        circom_indication_tree_pcq = level1_monitoring['responses']['50110001'][0]['183530001'][0]['text']
    except (IndexError,KeyError):
        circom_indication_tree_pcq = None

    try:
        circom_indication_tree_count = level1_monitoring['responses']['50110001'][0]['176761123'][0]['text']
    except (IndexError,KeyError):
        circom_indication_tree_count = None

    try:
        units_circom_pcq = level1_monitoring['responses']['50110001'][0]['183520002'][0]['code']
    except (IndexError,KeyError):
        units_circom_pcq = ''

    try:
        units_circom_count = level1_monitoring['responses']['50110001'][0]['184541091'][0]['code']
    except (IndexError,KeyError):
        units_circom_count = ''

    try:
        polygon_remapped = level1_monitoring['responses']['243590074'][0]['252970039']['features'][0].get('geometry',None) # Up to this level it can go wrong (due to empty entry)
        if polygon_remapped != None:
            polygon_remapped = level1_monitoring['responses']['243590074'][0]['252970039']['features'][0].get('geometry')
            area_ha = area(polygon_remapped)
            area_ha_remapped = round((area_ha/10000),3)
            geometry_remapped = convert.geojson_to_wkt(polygon_remapped)
            get_geom_type_remapped = geometry_remapped.split(' ',1)
            if get_geom_type_remapped[0] == 'POLYGON':
                polygon_check = convert.geojson_to_wkt(polygon_remapped)
                coord = re.findall('\s', polygon_check)
                number_coord_pol_remapped = int((len(coord)/2)-1)
                if number_coord_pol_remapped < 3:
                    polygon_remapped = None
                    area_ha_remapped = None
                    number_coord_pol_remapped = None
                else:
                    polygon_remapped = polygon_check

        else:
            polygon_remapped = None
            area_ha_remapped = None
            number_coord_pol_remapped = None

    except (IndexError, KeyError):
        polygon_remapped = None
        number_coord_pol_remapped = None
        area_ha_remapped = None

    # Create the tree monitoring raw table. Remapped polygons are excluded from this table and stored in a seperate table called "tree_monitoring_remapped_areas"
    cur.execute('''INSERT INTO AKVO_Tree_monitoring_areas (identifier_akvo, display_name, device_id, instance, submission, submission_year, submitter, AKVO_form_version, site_impression, test, avg_tree_height, number_living_trees, method_selection, avg_circom_tree_count, avg_circom_tree_pcq)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, displayname_m, device_id_m, instance_m, submissiondate_trunc_m, submission_year_m, submitter_m, formversion_m, impression_site, testing_m, avg_tree_height, tot_nr_trees_estimate, select_method, circom_indication_tree_count, circom_indication_tree_pcq))

    conn.commit()

    # Create the "tree_monitoring_remapped_areas" table with remapped polygons. It is seperated from the "tree monitoring raw table" to prevent the "tree monitoring raw table" appears as a geometry table in QGIS.
    cur.execute('''INSERT INTO AKVO_Tree_monitoring_remapped_areas (identifier_akvo, instance, submission, submitter, polygon_remapped, calc_area_remapped, number_coord_polygon_remapped)
    VALUES (%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, instance_m, submissiondate_trunc_m, submitter_m, polygon_remapped, area_ha_remapped, number_coord_pol_remapped))

    conn.commit()

    #get the 4 first photos in N, S, E, W direction from monitoring
    try:
        level1_monitoring['responses']['50110007'][0]
        print("CHECK :", level1_monitoring)
    except (IndexError,KeyError):
        photo_m4_url = None
        photo_m4_location = None
    else:
        try:
            photo_remark = level1_monitoring['responses']['50110007'][0].get('40300008')
            response_check = level1_monitoring['responses']['50110007'][0].get('311130282')
            if photo_remark is not None or response_check is not None:
                try:
                    del level1_monitoring['responses']['50110007'][0]['40300008']
                    photo = level1_monitoring['responses']['50110007'][0]
                except (IndexError,KeyError):
                    pass

                try:
                    del level1_monitoring['responses']['50110007'][0]['311130282']
                    photo = level1_monitoring['responses']['50110007'][0]
                except (IndexError,KeyError):
                    pass

                try:
                    photo_items_4 = list(photo.values())
                except (IndexError,KeyError):
                    photo_m4_url = None
                    photo_m4_location = None
            else:
                photo = level1_monitoring['responses']['50110007'][0]
                photo_items_4 = list(photo.values())

        except (IndexError,KeyError):
            photo_m4_url = None
            photo_m4_location = None

        for url in photo_items_4:
            try:
                url['filename']
            except KeyError:
                photo_m4_url = None
                photo_m4_location = None
            else:
                photo_m4_url = url['filename']
                try:
                    url['location']
                except KeyError:
                    photo_m4_location = None
                else:
                    if url['location'] is not None:
                        photo_lat = url['location']['latitude']
                        photo_lon = url['location']['longitude']
                        photo_lat = str(photo_lat)
                        photo_lon = str(photo_lon)
                        photo_m4_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                    else:
                        photo_m4_location = None


                #print('photo:',photo_url, photo_location, identifier, count) Prints well multiple photos and id's up to here.
                cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_url, photo_location)
                VALUES (%s,%s,%s,%s)''', (identifier_m, instance_m, photo_m4_url, photo_m4_location))

                conn.commit()


    # get the 36 other photos from the monitoring form
    try:
        for photo in level1_monitoring['responses']['40300009']:
            photo_items_36 = list(photo.values())
            for url in photo_items_36:
                try:
                    photo_m36_url = url['filename']
                    if url['location'] is not None:
                        photo_lat = url['location']['latitude']
                        photo_lon = url['location']['longitude']
                        photo_lat = str(photo_lat)
                        photo_lon = str(photo_lon)
                        photo_m36_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                    else:
                        photo_lat = None
                        photo_lon = None
                        photo_m36_location = None


                    cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_url, photo_location)
                    VALUES (%s,%s,%s,%s);''', (identifier_m, instance_m, photo_m36_url, photo_m36_location))

                    conn.commit()

                except (IndexError,KeyError):
                        photo_url = ''

    except (IndexError,KeyError):
        photo = ''


    # Part of the Group 2 sheet questions: Counting trees method
    try:
        for species_count in level1_monitoring['responses']['40310002']:

            try:
                tree_lat_species_name = species_count['50120001'][1]['code']
            except (IndexError,KeyError):
                tree_lat_species_name = ''
            try:
                tree_loc_species_name = species_count['50120001'][1]['name']
            except (IndexError,KeyError):
                tree_loc_species_name = ''
            try:
                tree_number_count = species_count['68330044']
            except (IndexError,KeyError):
                tree_number_count = None
            try:
                avg_tree_circom_count = level1_monitoring['169290073']
            except (IndexError,KeyError):
                avg_tree_circom_count = None

                cur.execute('''INSERT INTO AKVO_Tree_monitoring_counts (identifier_akvo, instance, name_species, loc_name_spec, number_species, avg_circom_tree)
                VALUES (%s,%s,%s,%s,%s,%s);''', (identifier_m, instance_m, tree_lat_species_name, tree_loc_species_name, tree_number_count, avg_tree_circom_count))

                conn.commit()

    except (IndexError,KeyError):
        tree_species_count = ''
        tree_number_count = ''
        avg_tree_circom_count = ''
        tree_loc_species_count = ''


    # Part of the Group 2 sheet questions: PCQ method
    try:
        if len(identifier_m) > 0:
            for pcq_results in level1_monitoring['responses']['39860004']:
                print('XXXXX: ', pcq_results)
                try:
                    lat_sample_pcq = pcq_results['54050004']['lat']
                except (IndexError,KeyError):
                    lat_sample_pcq = None
                    lat_sample_pcq_str = None
                else:
                    lat_sample_pcq = pcq_results['54050004']['lat']
                    lat_sample_pcq_str = str(lat_sample_pcq)

                try:
                    lon_sample_pcq = pcq_results['54050004']['long']
                except (IndexError,KeyError):
                    lon_sample_pcq = None
                    lon_sample_pcq_str = None
                else:
                    lon_sample_pcq = pcq_results['54050004']['long']
                    lon_sample_pcq_str = str(lon_sample_pcq)

                if not lat_sample_pcq_str or not lon_sample_pcq_str:
                    pcq_location = None
                else:
                    pcq_location = 'POINT('+ lon_sample_pcq_str + ' ' + lat_sample_pcq_str + ')'

                try:
                    elev_sample_pcq = pcq_results['54050004']['elev']
                except (IndexError,KeyError):
                    elev_sample_pcq = None
                try:
                    Q1_distance = pcq_results['15920003']
                except (IndexError,KeyError):
                    Q1_distance = None
                try:
                    Q1_height = pcq_results['11980002']
                except (IndexError,KeyError):
                    Q1_height = None
                try:
                    Q1_circom = pcq_results['183410039']
                except (IndexError,KeyError):
                    Q1_circom = None

                try:
                    pcq_results['40310003'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['40310003'][0]['name']
                    except (IndexError,KeyError):
                        Q1_species = ''
                    else:
                        Q1_species = pcq_results['40310003'][0]['name']
                else:
                    Q1_species = pcq_results['40310003'][1]['code']

                try:
                    Q2_distance = pcq_results['40300005']
                except (IndexError,KeyError):
                    Q2_distance = None
                try:
                    Q2_height = pcq_results['21860001']
                except (IndexError,KeyError):
                    Q2_height = None
                try:
                    Q2_circom = pcq_results['183440030']
                except (IndexError,KeyError):
                    Q2_circom = None

                try:
                    pcq_results['40300006'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['40300006'][0]['name']
                    except (IndexError,KeyError):
                        Q2_species = ''
                    else:
                        Q2_species = pcq_results['40300006'][0]['name']
                else:
                    Q2_species = pcq_results['40300006'][1]['code']

                try:
                    Q3_distance = pcq_results['25860009']
                except (IndexError,KeyError):
                    Q3_distance = None
                try:
                    Q3_height = pcq_results['5900005']
                except (IndexError,KeyError):
                    Q3_height = None
                try:
                    Q3_circom = pcq_results['190350004']
                except (IndexError,KeyError):
                    Q3_circom = None

                try:
                    pcq_results['48090004'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['48090004'][0]['name']
                    except (IndexError,KeyError):
                        Q3_species = ''
                    else:
                        Q3_species = pcq_results['48090004'][0]['name']
                else:
                    Q3_species = pcq_results['48090004'][1]['code']

                try:
                    Q4_distance = pcq_results['54040005']
                except (IndexError,KeyError):
                    Q4_distance = None
                try:
                    Q4_height = pcq_results['50110005']
                except (IndexError,KeyError):
                    Q4_height = None
                try:
                    Q4_circom = pcq_results['173040032']
                except (IndexError,KeyError):
                    Q4_circom = None

                try:
                    pcq_results['11990005'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['11990005'][0]['name']
                    except (IndexError,KeyError):
                        Q4_species = ''
                    else:
                        Q4_species = pcq_results['11990005'][0]['name']
                else:
                    Q4_species = pcq_results['11990005'][1]['code']


                # Create the tree monitoring pcq method table
                cur.execute('''INSERT INTO AKVO_Tree_monitoring_pcq (identifier_akvo, instance, lat_pcq_sample, lon_pcq_sample, height_pcq_sample, units_circom, Q1_dist, Q1_hgt, Q1_circom, Q1_spec, Q2_dist, Q2_hgt, Q2_circom, Q2_spec, Q3_dist, Q3_hgt, Q3_circom, Q3_spec, Q4_dist, Q4_hgt, Q4_circom, Q4_spec, pcq_location)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_m, instance_m, lat_sample_pcq, lon_sample_pcq, elev_sample_pcq, units_circom_pcq, Q1_distance, Q1_height, Q1_circom, Q1_species, Q2_distance, Q2_height, Q2_circom, Q2_species, Q3_distance, Q3_height, Q3_circom, Q3_species, Q4_distance, Q4_height, Q4_circom, Q4_species, pcq_location))

                conn.commit()

                # Download the PCQ monitoring PHOTOS
                for k, v in pcq_results.items():
                    try:
                        if k == '306650234' or k == '294310788' or k == '302780012' or k == '277771051':
                            url_photo_pcq = v['filename']
                            photo_source_pcq = 'pcq photos'
                        else:
                            continue
                            #print(url_photo_pcq)
                    except (IndexError,KeyError):
                        continue

                    cur.execute('''INSERT INTO AKVO_Tree_monitoring_photos (identifier_akvo, instance, photo_source, photo_url, photo_location)
                    VALUES (%s,%s,%s,%s,%s)''', (identifier_m, instance_m, photo_source_pcq, url_photo_pcq, pcq_location))

                    conn.commit()

    except KeyError:
        continue

# Get all the nursery registration changes
for level1 in changes_form_30050006_lst:
    #modifiedat = level1['modifiedAt']
    #formversion = level1['formVersion']
    try:
        identifier = level1['identifier']
    except (KeyError, IndexError):
        identifier = ''

    try:
        displayname = level1['displayName']
    except (KeyError, IndexError):
        displayname = ''

    try:
        deviceidentifier = level1['deviceIdentifier']
    except (KeyError, IndexError):
        deviceidentifier = ''

    instance = level1['id']

    try:
        submissiondate = level1['submissionDate']
    except (KeyError, IndexError):
        submissiondate = ''

    if not submissiondate:
        submissiontime = ''
    else:
        submissiontime = mid(submissiondate, 11,19)

    try:
        submissiondate_trunc = left(submissiondate,10)
    except (KeyError, IndexError):
        submissiondate_trunc = None

    try:
        submissiondate_trunc_year = left(submissiondate,4)
    except:
        submissiondate_trunc_year = None

    try:
        submitter = level1['submitter']
    except (KeyError, IndexError):
        submitter = ''

    try:
        country = level1['responses']['10050016'][0]['14200004'][0]['name']
    except (KeyError, IndexError):
        country = ''

    try:
        test = level1['responses']['10050016'][0]['6410091'][0]['text']
    except (KeyError, IndexError):
        test = ''

    try:
        name_organisation = level1['responses']['10050016'][0]['14200004'][1]['name']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        name_organisation = ''

    try:
        nursery_type = level1['responses']['10050016'][0]['2120005'][0]['text']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        nursery_type = ''

    try:
        nursery_name = level1['responses']['10050016'][0]['170003']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        nursery_name = ''

    try:
        new_established = level1['responses']['10050016'][0]['12210002'][0]['text']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        new_established = ''

    try:
        tree_production_full_cap = level1['responses']['10050016'][0]['24170005']
    except (KeyError, IndexError): # It may be that country is not filled in. In that case, the list will not be created and an Index error will occur (listitem 1 is not found)
        tree_production_full_cap = 0

    try:
        lat_centr = level1['responses']['10050016'][0]['30140002']['lat']
        lat_centr_conv = str(lat_centr)
    except (KeyError, IndexError):
        lat_centr = None
        lat_centr_conv = ''

    try:
        lon_centr = level1['responses']['10050016'][0]['30140002']['long']
        lon_centr_conv = str(lon_centr)
    except (KeyError, IndexError):
        lon_centr = None
        lon_centr_conv = ''

    if not lat_centr_conv or not lon_centr_conv:
        centroid_coord = None
    else:
        centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'

    try:
        elevation = level1['responses']['10050016'][0]['30140002']['elev']
    except (KeyError, IndexError):
        elevation = None


    # Populate the tree registration table
    #try:
    cur.execute('''INSERT INTO AKVO_Nursery_registration (identifier_akvo, display_name, device_id, instance, submission, submission_year, submitter, country, test, organisation, nursery_type, nursery_name, newly_established, full_tree_capacity, lat_y, lon_x, elevation, centroid_coord) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', (identifier, displayname, deviceidentifier, instance, submissiondate_trunc, submissiondate_trunc_year, submitter, country, test, name_organisation, nursery_type, nursery_name, new_established, tree_production_full_cap, lat_centr, lon_centr, elevation, centroid_coord))
    #except:
        #conn.rollback()
    #else:
    conn.commit()

    #cur.close()
#CREATE TABLE AKVO_Nursery_registration (identifier_akvo TEXT, display_name TEXT, device_id TEXT,
#instance INTEGER, submission DATE, submission_year NUMERIC,
#submitter TEXT, country TEXT, test TEXT, organisation TEXT, nursery_type TEXT, nursery_name TEXT, newly_established TEXT,
#full_tree_capacity NUMERIC, lat_y REAL, lon_x REAL, elevation REAL, centroid_coord geography(POINT, 4326));
    #cur = conn.cursor()

    try:
        level1['responses']['46400174']
    except KeyError:
        photo_location = None
        photo_url = ''
    else:
        for photo in level1['responses']['46400174']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
            photo_list = list(photo.values())
            for photo in photo_list:
                try:
                    photo_url = photo['filename']
                except KeyError:
                    photo_url = ''
                else:
                    photo_url = photo['filename']

                try: #print(photo_url) # print multiple rows well up to here with only urls
                    if photo['location'] != None:
                        photo_lat = photo['location']['latitude']
                        photo_lon = photo['location']['longitude']
                        photo_lat = str(photo_lat)
                        photo_lon = str(photo_lon)
                        if not photo_lat or not photo_lon:
                            photo_location = None
                        else:
                            photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                    else:
                        photo_location = None

                except:
                    photo_location = None

                #try:
                cur.execute('''INSERT INTO AKVO_Nursery_registration_photos (identifier_akvo, instance, photo_url, centroid_coord) VALUES (%s,%s,%s,%s);''', (identifier, instance, photo_url, photo_location))
                #except:
                    #conn.rollback()
                #else:
                conn.commit()



# Get all the nursery monitoring changes
for level1_m in changes_form_6070006_lst:
    #print('Json nurery monitoring changes: ', level1_m)
    identifier = level1_m['identifier']
    displayname = level1_m['displayName']
    deviceidentifier = level1_m['deviceIdentifier']
    instance = level1_m['id']
    submission_date = level1_m['submissionDate']
    submission_time = mid(submission_date, 11,19)
    submitter = level1_m['submitter']


    try:
        name_nursery_manager = level1_m['responses']['28140007'][0]['24170016']
    except (KeyError, IndexError):
        name_nursery_manager = ''

    try:
        test = level1_m['responses']['28140007'][0]['33540002'][0].get('text')
    except (KeyError, IndexError):
        test = ''

    try:
        gender_nursery_manager = level1_m['responses']['28140007'][0]['38140012'][0]['text']
    except (KeyError, IndexError):
        gender_nursery_manager = ''

    try:
        challenges_nursery = level1_m['responses']['28140007'][0]['6170011']
    except (KeyError, IndexError):
        challenges_nursery = ''

    try:
        number_trees_produced_currently = level1_m['responses']['28020008'][0]['6060029']
    except (KeyError, IndexError):
        number_trees_produced_currently = None

    try:
        month_planting_stock = level1_m['responses']['28020008'][0]['196392790']
        print_month_planting_stock = ''
    except (KeyError, IndexError):
        print_month_planting_stock = ''
    else:
        print_month_planting_stock = ''
        month_planting_stock = level1_m['responses']['28020008'][0]['196392790']
        for list_month_planting_stock in month_planting_stock:
            select_value = list_month_planting_stock['code']
            print_month_planting_stock += select_value + ' '

    try:
        nr_working_personel = level1_m['responses']['28020008'][0]['26180008']
    except (KeyError, IndexError):
        nr_working_personel = None

    cur.execute('''INSERT INTO AKVO_Nursery_monitoring (identifier_akvo, instance, submission_date, submission_time,
    submitter, name_nursery_manager, test, gender_nursery_manager, challenges_nursery,
    number_trees_produced_currently, month_planting_stock, nr_working_personel)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', (identifier, instance, submission_date, submission_time,
    submitter, name_nursery_manager, test, gender_nursery_manager, challenges_nursery,
    number_trees_produced_currently, print_month_planting_stock, nr_working_personel))

    conn.commit()

    try:
        level1_m['responses']['28020008']

    except KeyError:
        photo_location_m = None
        photo_url_m = None
    else:
        for photo_m in level1_m['responses']['28020008']: # Get first 4 photos from registration. This loop was tested in file: AKVO_database_download_v7_test_first_4_reg_photos.py
            photo_m.pop('26180008', None)
            photo_m.pop('6060029', None)
            photo_m.pop('196392790', None)
            photo_list_m = list(photo_m.values())

            for photo_m in photo_list_m:
                photo_url_m = photo_m['filename']

                try: #print(photo_url) # print multiple rows well up to here with only urls
                    if photo_m['location'] is None:
                        photo_location_m = None
                    else:
                        photo_lat_m = photo_m['location']['latitude']
                        photo_lon_m = photo_m['location']['longitude']
                        photo_lat_m = str(photo_lat_m)
                        photo_lon_m = str(photo_lon_m)
                        if not photo_lat_m or not photo_lon_m:
                            photo_location_m = None
                        else:
                            photo_location_m = 'POINT('+ photo_lon_m + ' ' + photo_lat_m + ')'

                except:
                    photo_lat_m = None
                    photo_lon_m = None
                    photo_location_m = None

                cur.execute('''INSERT INTO AKVO_Nursery_monitoring_photos (identifier_akvo, instance, photo_url, centroid_coord)
                VALUES (%s,%s,%s,%s);''', (identifier, instance, photo_url_m, photo_location_m))

                conn.commit()

    try:
        level1_m['responses']['16220016']

    except KeyError:
        species_latin_m = ''
        species_local_m = ''

    else:
        for x in level1_m['responses']['16220016']:
            try:
                species_latin_m = x['28130014'][1]['code']

            except (KeyError, IndexError):
                species_latin_m = None
            else:
                species_latin_m = x['28130014'][1]['code']

            try:
                species_local_m = x['28130014'][1]['name']

            except (KeyError, IndexError):
                species_local_m = None
            else:
                species_local_m = x['28130014'][1]['name']

            cur.execute('''INSERT INTO AKVO_Nursery_monitoring_tree_species (identifier_akvo, instance, tree_species_latin, tree_species_local)
            VALUES (%s,%s,%s,%s);''', (identifier, instance, species_latin_m, species_local_m))

            conn.commit()

# Changes in the external audit data
for level1_exaudits in changes_form_141500001_lst:
    identifier = level1_exaudits['identifier']
    instance = level1_exaudits['id']
    submitter = level1_exaudits['submitter']
    submissiondate = level1_exaudits['submissionDate']
    display_name = level1_exaudits['displayName']

    try:
        impression_site = level1_exaudits['responses']['145310001'][0]['160020001']
    except (KeyError, IndexError):
        impression_site = 'No comment'

    try:
        test = level1_exaudits['responses']['145310001'][0]['135610001'][0]['text']
    except (KeyError, IndexError):
        test = ''

    try:
        farmer_reported_tree_nr = level1_exaudits['responses']['145310001'][0]['219340289']
        farmer_reported_tree_nr = int(farmer_reported_tree_nr)
    except (KeyError, IndexError):
        farmer_reported_tree_nr = None

    try:
        farmer_reported_tree_species = level1_exaudits['responses']['145310001'][0]['236450229']
        farmer_reported_tree_species = int(farmer_reported_tree_species)
    except (KeyError, IndexError):
        farmer_reported_tree_species = None

    try:
        farmer_reported_died_trees = level1_exaudits['responses']['145310001'][0]['217360306']
        farmer_reported_died_trees = int(farmer_reported_died_trees)
    except (KeyError, IndexError):
        farmer_reported_died_trees = None

    try:
        audit_reported_trees = level1_exaudits['responses']['207590361'][0]['229630303']
        audit_reported_trees = int(audit_reported_trees)
    except (KeyError, IndexError):
        audit_reported_trees = None

    try:
        if level1_exaudits['responses']['207590361'][0]['229620295'] != None:
            audit_reported_tree_height = level1_exaudits['responses']['207590361'][0]['229620295']
            audit_reported_tree_height = float(audit_reported_tree_height)
        else:
            audit_reported_tree_height = None
    except (KeyError, IndexError):
        audit_reported_tree_height = None

    try:
        option_tree_count = level1_exaudits['responses']['145310001'][0]['217360308'][0]['text']
    except (KeyError, IndexError):
        option_tree_count = ''

    try:
        location_external_audit_lat = level1_exaudits['responses']['145310001'][0]['252970031']['lat']
        location_external_audit_lon = level1_exaudits['responses']['145310001'][0]['252970031']['long']
    except (KeyError, IndexError):
        location_external_audit_lat = None
        location_external_audit_lon = None

    if location_external_audit_lat is not None and location_external_audit_lon is not None:
        location_external_audit_lat_str = str(location_external_audit_lat)
        location_external_audit_lon_str = str(location_external_audit_lon)
        location_external_audit = 'POINT('+ location_external_audit_lon_str + ' ' + location_external_audit_lat_str + ')'
    else:
        location_external_audit = None


    #living trees_found = level1_exaudits['identifier']229630303

    try:
        geom_get = level1_exaudits['responses']['145680002'][0]['223780044'] # Up to this level it can go wrong (due to empty entry)
        if geom_get != None:
            geom_get = level1_exaudits['responses']['145680002'][0]['223780044']['features'][0].get('geometry','')
            area_ha = area(geom_get)
            area_ha = round((area_ha/10000),3)
            geometry = convert.geojson_to_wkt(geom_get)
            get_geom_type = geometry.split(' ',1)
            if get_geom_type[0] == 'POLYGON':
                polygon = convert.geojson_to_wkt(geom_get)
                coord = re.findall('\s', polygon)
                number_coord_pol = int((len(coord)/2)-1)
                multipoint = None

            elif get_geom_type[0] == 'MULTIPOINT':
                multipoint = convert.geojson_to_wkt(geom_get)
                polygon = None

        else:
            geom_get = None

    except (IndexError, KeyError):
        polygon = None
        coord = None
        number_coord_pol = None
        multipoint = None
        geom_get = None
        area_ha = None
        geometry = None
        get_geom_type = None


    cur.execute('''INSERT INTO AKVO_Tree_external_audits_areas (identifier_akvo, instance, display_name, submitter, submission, test, farmer_reported_tree_nr, farmer_reported_nr_tree_species, farmer_reported_died_trees, audit_reported_trees, audit_reported_tree_height, option_tree_count, impression_site, lat_y, lon_x, calc_area, location_external_audit, polygon)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', (identifier, instance, display_name, submitter, submissiondate, test, farmer_reported_tree_nr, farmer_reported_tree_species, farmer_reported_died_trees, audit_reported_trees, audit_reported_tree_height, option_tree_count, impression_site, location_external_audit_lat, location_external_audit_lon, area_ha, location_external_audit, polygon))

    conn.commit()

    # Get the photos from external verification for North/South/East/West direction
    try:
        x = level1_exaudits['responses']['154700035']
        #print('list: ', x)
    except (IndexError, KeyError):
        url_photo = None
        location_photo = None
        photo_lat = None
        photo_lon = None
    else:
        photos = level1_exaudits['responses']['154700035'][0]
        photos.pop('137520089', None)
        for urls in photos.values():
            url_photo = urls['filename']
            loc_photo = urls['location']
            if loc_photo is not None:
                location_photo_lat = loc_photo.get('latitude')
                location_photo_lon = loc_photo.get('longitude')
                photo_lat = str(location_photo_lat)
                photo_lon = str(location_photo_lon)
                location_photo = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
            else:
                photo_lat = None
                photo_lon = None
                location_photo = None

            cur.execute('''INSERT INTO AKVO_Tree_external_audits_photos (identifier_akvo, instance, lat_photo, lon_photo, url_photo, location_photo)
            VALUES (%s,%s,%s,%s,%s,%s);''',
            (identifier, instance, photo_lat, photo_lon, url_photo, location_photo))

            conn.commit()

    # Get the additional photos from external verification (see question "More photos needed?")
    try:
        photos_additional = level1_exaudits['responses']['158130003'][0]
        print('YYYYYY: ', identifier, photos_additional)
        #print('list: ', x)
    except (IndexError, KeyError):
        url_photo_additional = None
        location_photo_add = None
        photo_add_lat = None
        photo_add_lon = None
    else:
        for k,v in photos_additional.items():
            if isinstance(v, dict):
                url_photo_additional = v['filename']
                print(url_photo_additional)
                try:
                    v['location']
                except (IndexError, KeyError):
                    location_photo_add = None
                    photo_add_lat = None
                    photo_add_lon = None
                    url_photo_additional = v['filename']
                else:
                    if v['location'] is None:
                        location_photo_add = None
                        photo_add_lat = None
                        photo_add_lon = None
                        url_photo_additional = v['filename']
                    else:
                        location_photo_add_lat = v['location']['latitude']
                        location_photo_add_lon = v['location']['longitude']
                        photo_add_lat = str(location_photo_add_lat)
                        photo_add_lon = str(location_photo_add_lon)
                        location_photo_add = 'POINT('+ photo_add_lon + ' ' + photo_add_lat + ')'
                        url_photo_additional = v['filename']
            else:
                photo_add_lat = None # REAL datatype does not accept None values
                photo_add_lon = None
                location_photo_add = None
                url_photo_additional = None

            print('XXXXXX: ',identifier, url_photo_additional)

            cur.execute('''INSERT INTO AKVO_Tree_external_audits_photos (identifier_akvo, instance, lat_photo, lon_photo, url_photo, location_photo)
            VALUES (%s,%s,%s,%s,%s,%s);''',
            (identifier, instance, photo_add_lat, photo_add_lon, url_photo_additional, location_photo_add))

            conn.commit()


    try:
        level1_exaudits['responses']['133480001']

    except (IndexError, KeyError):
        number_species = None
        name_species = None
        loc_name_spec = None
        name_not_in_list = None

    else:
        for audit_tree_count in level1_exaudits['responses']['133480001']:
            number_species = audit_tree_count.get('150790031')
            try:
                name_species = audit_tree_count['160050001'][1].get('code')
            except (IndexError, KeyError):
                name_species = None
            else:
                name_species = audit_tree_count['160050001'][1].get('code')

            try:
                loc_name_spec = audit_tree_count['160050001'][1].get('name')
            except (IndexError, KeyError):
                loc_name_spec = None
            else:
                loc_name_spec = audit_tree_count['160050001'][1].get('name')

            name_not_in_list = audit_tree_count.get('334970350')

            cur.execute('''INSERT INTO AKVO_Tree_external_audits_counts (identifier_akvo, instance, name_species, loc_name_spec, name_not_in_list, number_species)
            VALUES (%s,%s,%s,%s,%s,%s);''',
            (identifier, instance, name_species, loc_name_spec, name_not_in_list, number_species))

            conn.commit()


    # Part of the Group 2 sheet questions: PCQ method
    try:
        create_Qlist = list()
        Q1 = '156360017'
        Q2 = '158160026'
        Q3 = '137590011'
        Q4 = '154610120'

        for pcq_results in level1_exaudits['responses']['158130010']:
            if (Q1 in pcq_results.keys() or Q2 in pcq_results.keys() or Q3 in pcq_results.keys() or Q4 in pcq_results.keys()):

                try:
                    lat_sample_pcq = pcq_results['158130011']['lat']
                except (IndexError,KeyError):
                    lat_sample_pcq = None
                try:
                    lon_sample_pcq = pcq_results['158130011']['long']
                except (IndexError,KeyError):
                    lon_sample_pcq = None

                if lat_sample_pcq is not None:
                    lat_sample_pcq_str = str(lat_sample_pcq)
                else:
                    lat_sample_pcq_str = ''

                if lon_sample_pcq is not None:
                    lon_sample_pcq_str = str(lon_sample_pcq)
                else:
                    lon_sample_pcq_str = ''

                if not lat_sample_pcq_str or not lon_sample_pcq_str:
                    pcq_location = None
                else:
                    pcq_location = 'POINT('+ lon_sample_pcq_str + ' ' + lat_sample_pcq_str + ')'

                try:
                    elev_sample_pcq = pcq_results['158130011']['elev']
                except (IndexError,KeyError):
                    elev_sample_pcq = None
                try:
                    Q1_distance = pcq_results['156360017']
                except (IndexError,KeyError):
                    Q1_distance = None
                try:
                    Q1_height = pcq_results['137590009']
                except (IndexError,KeyError):
                    Q1_height = None

                try:
                    Q1_circom = pcq_results['243590056']
                except (IndexError,KeyError):
                    Q1_circom = None

                try:
                    pcq_results['160020007'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['160020007'][0]['name']
                    except (IndexError,KeyError):
                        Q1_species = ''
                    else:
                        Q1_species = pcq_results['160020007'][0]['name']
                else:
                    Q1_species = pcq_results['160020007'][1]['code']

                try:
                    Q2_distance = pcq_results['158160026']
                except (IndexError,KeyError):
                    Q2_distance = None
                try:
                    Q2_height = pcq_results['115380039']
                except (IndexError,KeyError):
                    Q2_height = None

                try:
                    Q2_circom = pcq_results['266060041']
                except (IndexError,KeyError):
                    Q2_circom = None

                try:
                    pcq_results['119150011'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['119150011'][0]['name']
                    except (IndexError,KeyError):
                        Q2_species = ''
                    else:
                        Q2_species = pcq_results['119150011'][0]['name']
                else:
                    Q2_species = pcq_results['119150011'][1]['code']

                try:
                    Q3_distance = pcq_results['137590011']
                except (IndexError,KeyError):
                    Q3_distance = None
                try:
                    Q3_height = pcq_results['119150013']
                except (IndexError,KeyError):
                    Q3_height = None

                try:
                    Q3_circom = pcq_results['264260053']
                except (IndexError,KeyError):
                    Q3_circom = None

                try:
                    pcq_results['156350009'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['156350009'][0]['name']
                    except (IndexError,KeyError):
                        Q3_species = ''
                    else:
                        Q3_species = pcq_results['156350009'][0]['name']
                else:
                    Q3_species = pcq_results['156350009'][1]['code']

                try:
                    Q4_distance = pcq_results['154610120']
                except (IndexError,KeyError):
                    Q4_distance = None
                try:
                    Q4_height = pcq_results['137630011']
                except (IndexError,KeyError):
                    Q4_height = None

                try:
                    Q4_circom = pcq_results['190990060']
                except (IndexError,KeyError):
                    Q4_circom = None

                try:
                    pcq_results['137630012'][1]['code']
                except (IndexError,KeyError):
                    try:
                        pcq_results['137630012'][0]['name']
                    except (IndexError,KeyError):
                        Q4_species = ''
                    else:
                        Q4_species = pcq_results['137630012'][0]['name']
                else:
                    Q4_species = pcq_results['137630012'][1]['code']


                # Create the tree monitoring pcq method table
                cur.execute('''INSERT INTO AKVO_Tree_external_audits_pcq (identifier_akvo, instance, lat_pcq_sample, lon_pcq_sample, pcq_location, height_pcq_sample, Q1_dist, Q1_hgt, Q1_circom, Q1_spec, Q2_dist, Q2_hgt, Q2_circom, Q2_spec, Q3_dist, Q3_hgt, Q3_circom, Q3_spec, Q4_dist, Q4_hgt, Q4_circom, Q4_spec)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier, instance, lat_sample_pcq, lon_sample_pcq, pcq_location, elev_sample_pcq, Q1_distance, Q1_height, Q1_circom, Q1_species, Q2_distance, Q2_height, Q2_circom, Q2_species, Q3_distance, Q3_height, Q3_circom, Q3_species, Q4_distance, Q4_height, Q4_circom, Q4_species))

                conn.commit()

    except KeyError:
        pcq_results = ''


# Changes in the tree distribution to unregistered farmers form
for registration_distributed_trees in changes_form_252327848_lst:

    try:
        identifier_akvo = registration_distributed_trees['identifier']
    except (IndexError,KeyError):
        identifier_akvo = None

    try:
        displayname = registration_distributed_trees['displayName']
    except (IndexError,KeyError):
        displayname = None

    try:
        device_id = registration_distributed_trees['deviceIdentifier']
    except (IndexError,KeyError):
        device_id = None

    try:
        instance = registration_distributed_trees['id']
    except (IndexError,KeyError):
        instance = None

    try:
        submitter = registration_distributed_trees['submitter']
    except (IndexError,KeyError):
        submitter = None

    try:
        submission_date = registration_distributed_trees['submissionDate']
    except (IndexError,KeyError):
        submission_date = None

    try:
        form_version = registration_distributed_trees['formVersion']
    except (IndexError,KeyError):
        form_version = None


    try:
        country = registration_distributed_trees['responses']['241552098'][0]['248221840'][0].get('name')
        print(country)

    except (IndexError,KeyError):
        country = None

    try:
        organisation = registration_distributed_trees['responses']['241552098'][0]['248221840'][1].get('name')
        print(organisation)
    except (IndexError,KeyError):
        organisation = None

    try:
        contract_number = registration_distributed_trees['responses']['241552098'][0].get('211873508')
        print(contract_number)
    except (IndexError,KeyError):
        contract_number = None

    try:
        test = registration_distributed_trees['responses']['241552098'][0]['315384624'][0].get('text')
        print(test)
    except (IndexError,KeyError):
        test = None

    try:
        name_tree_receiver = registration_distributed_trees['responses']['241552098'][0].get('239313615')
        print(name_tree_receiver)
    except (IndexError,KeyError):
        name_tree_receiver = None

    try:
        gender_tree_receiver = registration_distributed_trees['responses']['241552098'][0]['266122082'][0].get('text')
        print(gender_tree_receiver)
    except (IndexError,KeyError):
        gender_tree_receiver = None

    try:
        check_ownership_trees = registration_distributed_trees['responses']['241552098'][0]['262782006'][0].get('text')
        print(check_ownership_trees)
    except (IndexError,KeyError):
        check_ownership_trees = None

    try:
        check_ownership_land = registration_distributed_trees['responses']['241552098'][0]['258620407'][0].get('text')
        print(check_ownership_land)
    except (IndexError,KeyError):
        check_ownership_land = None

    try:
        photo_receiver_trees = registration_distributed_trees['responses']['241552098'][0]['244788272'].get('filename')
        print(photo_receiver_trees)
    except (IndexError,KeyError):
        photo_receiver_trees = None

    try:
        photo_id_card_tree_receiver = registration_distributed_trees['responses']['241552098'][0]['213993970'].get('filename')
        print(photo_id_card_tree_receiver)
    except (IndexError,KeyError):
        photo_id_card_tree_receiver = None

    try:
        location_house_tree_receiver = registration_distributed_trees['responses']['241552098'][0].get('231082082')
        print(location_house_tree_receiver)
    except (IndexError,KeyError):
        location_house_tree_receiver = None

    try:
        name_site_id_tree_planting = registration_distributed_trees['responses']['241552098'][0].get('278476531')
        print(name_site_id_tree_planting)
    except (IndexError,KeyError):
        name_site_id_tree_planting = None

    try:
        confirm_planting_location = registration_distributed_trees['responses']['241552098'][0]['258620407'][0].get('text')
        print(confirm_planting_location)
    except (IndexError,KeyError):
        confirm_planting_location = None

    try:
        total_number_trees_received = registration_distributed_trees['responses']['237550277'][0]['231056596']
        print(total_number_trees_received)
    except (IndexError,KeyError):
        total_number_trees_received = None

    try:
        signature_tree_receiver = registration_distributed_trees['responses']['237550277'][0]['52318270'].get('filename')
    except (IndexError,KeyError):
        signature_tree_receiver = None

    cur.execute('''INSERT INTO AKVO_Tree_distribution_unregistered_farmers (identifier_akvo, display_name, device_id, instance,
    submission, submitter, AKVO_form_version, country, test, organisation, contract_number,
    name_tree_receiver, gender_tree_receiver, check_ownership_trees, check_ownership_land, url_photo_receiver_trees,
    url_photo_id_card_tree_receiver, location_house_tree_receiver, name_site_id_tree_planting, confirm_planting_location,
    total_number_trees_received, url_signature_tree_receiver)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
    (identifier_akvo, displayname, device_id, instance, submission_date, submitter, form_version, country,
    test, organisation, contract_number, name_tree_receiver, gender_tree_receiver,
    check_ownership_trees, check_ownership_land, photo_receiver_trees, photo_id_card_tree_receiver, location_house_tree_receiver,
    name_site_id_tree_planting, confirm_planting_location, total_number_trees_received, signature_tree_receiver))

    conn.commit()

    try:
        name_species_local = registration_distributed_trees['responses']['242845264']
    except (IndexError,KeyError):
        name_species_local = None
        name_species_latin = None
    else:
        for x in registration_distributed_trees['responses']['242845264']:
            print(x)
            try:
                number_species = x.get('258032656')
            except (IndexError,KeyError):
                number_species = None
            try:
                name_species_local = x.get('211873502')[1].get('name')
            except (IndexError,KeyError):
                name_species_local = None
            try:
                name_species_latin = x.get('211873502')[1].get('code')
            except (IndexError,KeyError):
                name_species_latin = None

        cur.execute('''INSERT INTO AKVO_Tree_distribution_unregistered_tree_species (identifier_akvo, display_name,
        device_id, instance, species_lat, species_local, number_species)
        VALUES (%s,%s,%s,%s,%s,%s,%s)''',
        (identifier_akvo, displayname, device_id, instance, name_species_latin, name_species_local, number_species))

        conn.commit()



# Changes in the tree registration of unregistered farmers form
for site_registration_distributed_trees in changes_form_258280042_lst:

    try:
        identifier_akvo = site_registration_distributed_trees['identifier']
    except (IndexError,KeyError):
        identifier_akvo = None
        print(identifier_akvo)

    try:
        displayname = site_registration_distributed_trees['displayName']
    except (IndexError,KeyError):
        displayname = None
        print(displayname)

    try:
        device_id = site_registration_distributed_trees['deviceIdentifier']
    except (IndexError,KeyError):
        device_id = None
        print(device_id)

    try:
        instance = site_registration_distributed_trees['id']
    except (IndexError,KeyError):
        instance = None
        print(instance)

    try:
        submitter = site_registration_distributed_trees['submitter']
    except (IndexError,KeyError):
        submitter = None
        print(submitter)

    try:
        submission_date = left(site_registration_distributed_trees['submissionDate'],10)
    except (IndexError,KeyError):
        submission_date = None
        print(submission_date)

    try:
        form_version = site_registration_distributed_trees['formVersion']
    except (IndexError,KeyError):
        form_version = None
        form_version = str(form_version)

    try:
        test = site_registration_distributed_trees['responses']['205421525'][0]['262680218'][0].get('text')
        print(test)
    except (IndexError,KeyError):
        test = None

    try:
        name_region_village_planting_site = site_registration_distributed_trees['responses']['205421525'][0].get('205610424')
        print(name_region_village_planting_site)
    except (IndexError,KeyError):
        name_region_village_planting_site = None


    try:
        name_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0].get('251020087')
        print(name_owner_planting_site)
    except (IndexError,KeyError):
        name_owner_planting_site = None

    try:
        gender_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0]['234700078'][0].get('text')
        print(gender_owner_planting_site)
    except (IndexError,KeyError):
        gender_owner_planting_site = None

    try:
        if site_registration_distributed_trees['responses']['205421525'][0]['251000760'] != None:
            photo_owner_planting_site = site_registration_distributed_trees['responses']['205421525'][0]['251000760'].get('filename')
        else:
            photo_owner_planting_site = None
    except (IndexError,KeyError):
        photo_owner_planting_site = None

    try:
        nr_trees_received = site_registration_distributed_trees['responses']['205421525'][0].get('258590461')
        print(nr_trees_received)
    except (IndexError,KeyError):
        nr_trees_received = None

    try:
        confirm_plant_location_own_land = site_registration_distributed_trees['responses']['205421525'][0]['245660427'][0].get('text')
        print(confirm_plant_location_own_land)
    except (IndexError,KeyError):
        confirm_plant_location_own_land = None

    try:
        one_multiple_planting_sites = site_registration_distributed_trees['responses']['205421525'][0]['251052748'][0].get('text')
        print(one_multiple_planting_sites)
    except (IndexError,KeyError):
        one_multiple_planting_sites = None

    try:
        nr_trees_given_away = site_registration_distributed_trees['responses']['205421525'][0].get('262731726')
        print(nr_trees_given_away)
    except (IndexError,KeyError):
        nr_trees_given_away = None

    try:
        more_less_200_trees = site_registration_distributed_trees['responses']['205421525'][0]['215732096'][0].get('text')
        print(more_less_200_trees)
    except (IndexError,KeyError):
        more_less_200_trees = None

    try:
        date_tree_planting = left(site_registration_distributed_trees['responses']['205421525'][0]['247472690'],10)
        print(date_tree_planting)
    except (IndexError,KeyError):
        date_tree_planting = None

    try:
        centroid_coord_lat = site_registration_distributed_trees['responses']['231532129'][0]['205652116'].get('lat')
        centroid_coord_lon = site_registration_distributed_trees['responses']['231532129'][0]['205652116'].get('long')
    except (IndexError,KeyError):
        centroid_coord_lat = None
        centroid_coord_lon = None

    if centroid_coord_lat is None or centroid_coord_lon is None:
        centroid_coord = None
    else:
        lat_centr_conv = str(centroid_coord_lat)
        lon_centr_conv = str(centroid_coord_lon)
        centroid_coord = 'POINT (' + lon_centr_conv +' '+ lat_centr_conv +')'


    try:
        geom_get = site_registration_distributed_trees['responses']['231532129'] # Up to this level it can go wrong (due to empty entry)
        if geom_get != None:
            geom_get = site_registration_distributed_trees['responses']['231532129'][0]['264382254']['features'][0].get('geometry','')
            area_ha = area(geom_get)
            area_ha = round((area_ha/10000),3)
            geometry = convert.geojson_to_wkt(geom_get)
            get_geom_type = geometry.split(' ',1)
            if get_geom_type[0] == 'POLYGON':
                polygon = convert.geojson_to_wkt(geom_get)
                coord = re.findall('\s', polygon)
                number_coord_pol = int((len(coord)/2)-1)
                if number_coord_pol < 3:
                    polygon = None
            else:
                geom_get = None
                area_ha = None
                polygon = None
        else:
            geom_get = None
            area_ha = None
            polygon = None

    except (IndexError, KeyError):
        polygon = None
        number_coord_pol = None
        area_ha = None

    try:
        avg_tree_distance = site_registration_distributed_trees['responses']['251064121'][0].get('247454719')
        print(avg_tree_distance)
    except (IndexError,KeyError):
        avg_tree_distance = None

    try:
        estimated_area = site_registration_distributed_trees['responses']['251064121'][0].get('256794593')
        print(estimated_area)
    except (IndexError,KeyError):
        estimated_area = None

    try:
        unit_estimated_area = site_registration_distributed_trees['responses']['251064121'][0]['258644833'][0].get('text')
        print(unit_estimated_area)
    except (IndexError,KeyError):
        unit_estimated_area = None

    try:
        estimated_tree_number_planted = site_registration_distributed_trees['responses']['251064121'][0].get('256784230')
        print(estimated_tree_number_planted)
    except (IndexError,KeyError):
        estimated_tree_number_planted = None

    try:
        confirm_additional_photos = site_registration_distributed_trees['responses']['245650331'][0]['260300823'][0].get('text')
        print('22222 :', confirm_additional_photos)
    except (IndexError,KeyError):
        confirm_additional_photos = None

    try:
        status_of_trees = site_registration_distributed_trees['responses']['262740058'][0]['247390855'][0].get('text')
        print('33333 :',status_of_trees)
    except (IndexError,KeyError):
        status_of_trees = None

    try:
        comment_enumerator = site_registration_distributed_trees['responses']['262740058'][0].get('2231510689')
        print('44444 :',comment_enumerator)
    except (IndexError,KeyError):
        comment_enumerator = None


    cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees
    (identifier_akvo, displayname, device_id, instance, submitter, submission_date, form_version, test,
    name_region_village_planting_site, name_owner_planting_site, gender_owner_planting_site,
    photo_owner_planting_site, nr_trees_received, confirm_plant_location_own_land,
    one_multiple_planting_sites, nr_trees_given_away, more_less_200_trees, date_tree_planting, centroid_coord,
    polygon, number_coord_pol, area_ha, avg_tree_distance, estimated_area, unit_estimated_area,
    estimated_tree_number_planted, confirm_additional_photos, comment_enumerator)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, submitter, submission_date, form_version, test,
    name_region_village_planting_site, name_owner_planting_site, gender_owner_planting_site,
    photo_owner_planting_site, nr_trees_received, confirm_plant_location_own_land,
    one_multiple_planting_sites, nr_trees_given_away, more_less_200_trees, date_tree_planting, centroid_coord,
    polygon, number_coord_pol, area_ha, avg_tree_distance, estimated_area, unit_estimated_area,
    estimated_tree_number_planted, confirm_additional_photos, comment_enumerator))

    conn.commit()

    try:
        site_registration_distributed_trees['responses']['245650331']
    except (IndexError,KeyError):
        photo_url_4 = None
        photo_location = None
    else:
        for x in site_registration_distributed_trees['responses']['245650331']:
            remove_list = x.pop('260300823')
            for k,v in x.items():
               photo_url_4 = v.get('filename')
               try:
                   v['location']
               except (IndexError,KeyError):
                   photo_location = None
               else:
                   if v['location'] is not None:
                       photo_geotag_lat = v.get('location')['latitude']
                       photo_lat = str(photo_geotag_lat)
                       photo_geotag_lon = v.get('location')['longitude']
                       photo_lon = str(photo_geotag_lon)
                       photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'

                   else:
                       photo_location = None

                   cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees_photos
                   (identifier_akvo, displayname, device_id, instance, photo_url_4, photo_location)
                   VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, photo_url_4, photo_location))

                   conn.commit()

    try:
        site_registration_distributed_trees['responses']['252970460']
    except (IndexError,KeyError):
        species_lat = None
        species_local = None
    else:
        for x in site_registration_distributed_trees['responses'].get('252970460'):
            for y in x.values():
                print('AAAAA: ', y)
                if not y: # check if list is empty
                    species_lat = None
                    species_local = None
                else:
                    species_lat = y[1].get('code')
                    species_local = y[1].get('name')
                    #print(species_lat, species_local)

        cur.execute('''INSERT INTO AKVO_site_registration_distributed_trees_species
        (identifier_akvo, displayname, device_id, instance, species_lat, species_local)
        VALUES (%s,%s,%s,%s,%s,%s)''', (identifier_akvo, displayname, device_id, instance, species_lat, species_local))

        conn.commit()




# Changes in the tree registration of unregistered farmers form
for registration_light in changes_form_242817396_lst:

    #print(registration_light)
    try:
        identifier_akvo = registration_light['identifier']
    except (IndexError,KeyError):
        identifier_akvo = None
    else:
        identifier_akvo = registration_light['identifier']
        #print(identifier_akvo)

    try:
        displayname = registration_light['displayName']
    except (IndexError,KeyError):
        displayname = None
    else:
        displayname = registration_light['displayName']
        #print(displayname)

    try:
        device_id = registration_light['deviceIdentifier']
    except (IndexError,KeyError):
        device_id = None
    else:
        device_id = registration_light['deviceIdentifier']
        #print(device_id)

    try:
        instance = registration_light['id']
    except (IndexError,KeyError):
        instance = None
    else:
        instance = registration_light['id']
        #print(instance)

    try:
        submitter = registration_light['submitter']
    except (IndexError,KeyError):
        submitter = None
    else:
        submitter = registration_light['submitter']
        #print(submitter)

    try:
        submission_date = registration_light['submissionDate']
    except (IndexError,KeyError):
        submission_date = None
    else:
        submission_date = registration_light['submissionDate']
        submission_date_trunc = left(submission_date,10)
        #print(submission_date)

    try:
        form_version = registration_light['formVersion']
    except (IndexError,KeyError):
        form_version = None
    else:
        form_version = registration_light['formVersion']
        form_version = str(form_version)

    try:
        country = registration_light['responses']['252306883'][0]['241550309'][0]['name']
    except (IndexError,KeyError):
        country = None
    else:
        country = registration_light['responses']['252306883'][0]['241550309'][0].get('name')

    try:
        organisation = registration_light['responses']['252306883'][0]['241550309'][1]['name']
    except (IndexError,KeyError):
        organisation = None
    else:
        organisation = registration_light['responses']['252306883'][0]['241550309'][1].get('name')


    test = registration_light['responses']['252306883'][0]['241550308'][0].get('text')
    #print(test)

    contract_number = registration_light['responses']['252306883'][0].get('241550306')
    #print(contract_number)

    name_village = registration_light['responses']['252306883'][0].get('264400049')
    #print(name_village)

    planting_site_id = registration_light['responses']['252306883'][0].get('241550310')
    #print(planting_site_id)

    name_owner = registration_light['responses']['252306883'][0].get('241550307')
    #print(name_owner)

    number_trees_registered = registration_light['responses']['252306883'][0].get('241550312')
    #print(number_trees_registered)

    try:
        lat_y_1 = registration_light['responses']['252306885'][0]['205357333']['lat']
    except (IndexError,KeyError):
        lat_y_str_1 = None
    else:
        lat_y_1 = registration_light['responses']['252306885'][0]['205357333']['lat']
        lat_y_str_1 = str(lat_y_1)

    try:
        lon_x_1 = registration_light['responses']['252306885'][0]['205357333']['long']
    except (IndexError,KeyError):
        lon_x_str_1 = None
    else:
        lon_x_1 = registration_light['responses']['252306885'][0]['205357333']['long']
        lon_x_str_1 = str(lon_x_1)

    if lat_y_1 is None or lon_x_1 is None:
        gps_corner_1 = None
    elif lat_y_1 == 'None' or lon_x_1 == 'None':
        gps_corner_1 = None
    else:
        lat_y_coord_1 = str(lat_y_1)
        lon_x_coord_1 = str(lon_x_1)
        gps_corner_1 = 'POINT (' + lon_x_coord_1 +' '+ lat_y_coord_1 +')'


    try:
        lat_y_2 = registration_light['responses']['252306885'][0]['205357333']['lat']
    except (IndexError,KeyError):
        lat_y_str_2 = None
    else:
        lat_y_2 = registration_light['responses']['252306885'][0]['205357333']['lat']
        lat_y_str_2 = str(lat_y_2)

    try:
        lon_x_2 = registration_light['responses']['252306885'][0]['205357333']['long']
    except (IndexError,KeyError):
        lon_x_str_2 = None
    else:
        lon_x_2 = registration_light['responses']['252306885'][0]['205357333']['long']
        lon_x_str_2 = str(lon_x_2)

    if lat_y_2 is None or lon_x_2 is None:
        gps_corner_1 = None
    elif lat_y_2 == 'None' or lon_x_2 == 'None':
        gps_corner_2 = None
    else:
        lat_y_coord_2 = str(lat_y_2)
        lon_x_coord_2 = str(lon_x_2)
        gps_corner_2 = 'POINT (' + lon_x_coord_2 +' '+ lat_y_coord_2 +')'


    try:
        lat_y_3 = registration_light['responses']['252306885'][0]['205357333']['lat']
    except (IndexError,KeyError):
        lat_y_str_3 = None
    else:
        lat_y_3 = registration_light['responses']['252306885'][0]['205357333']['lat']
        lat_y_str_3 = str(lat_y_3)

    try:
        lon_x_3 = registration_light['responses']['252306885'][0]['205357333']['long']
    except (IndexError,KeyError):
        lon_x_str_3 = None
    else:
        lon_x_3 = registration_light['responses']['252306885'][0]['205357333']['long']
        lon_x_str_3 = str(lon_x_3)

    if lat_y_3 is None or lon_x_3 is None:
        gps_corner_3 = None
    elif lat_y_3 == 'None' or lon_x_3 == 'None':
        gps_corner_3 = None
    else:
        lat_y_coord_3 = str(lat_y_3)
        lon_x_coord_3 = str(lon_x_3)
        gps_corner_3 = 'POINT (' + lon_x_coord_3 +' '+ lat_y_coord_3 +')'

    try:
        lat_y_4 = registration_light['responses']['252306885'][0]['205357333']['lat']
    except (IndexError,KeyError):
        lat_y_str_4 = None
    else:
        lat_y_4 = registration_light['responses']['252306885'][0]['205357333']['lat']
        lat_y_str_4 = str(lat_y_4)

    try:
        lon_x_4 = registration_light['responses']['252306885'][0]['205357333']['long']
    except (IndexError,KeyError):
        lon_x_str_4 = None
    else:
        lon_x_4 = registration_light['responses']['252306885'][0]['205357333']['long']
        lon_x_str_4 = str(lon_x_4)

    if lat_y_4 is None or lon_x_4 is None:
        gps_corner_4 = None
    elif lat_y_4 == 'None' or lon_x_4 == 'None':
        gps_corner_4 = None
    else:
        lat_y_coord_4 = str(lat_y_4)
        lon_x_coord_4 = str(lon_x_4)
        gps_corner_4 = 'POINT (' + lon_x_coord_4 +' '+ lat_y_coord_4 +')'


    avg_planting_distance = registration_light['responses']['252306885'][0].get('245710152')
    #print(avg_planting_distance)

    planting_date = registration_light['responses']['252306885'][0].get('258591057')
    planting_date_trunc = left(planting_date,10)
    #print(planting_date_trunc)

    remark = registration_light['responses']['264300438'][0].get('264360226')
    #print(remark)

    multiple_planting_locations = registration_light['responses']['264300438'][0]['250980826'][0].get('text')
    #print(multiple_planting_locations)

    # Populate the tree registration table
    cur.execute('''INSERT INTO AKVO_Tree_registration_locations_light_version (identifier_akvo, display_name, device_id, instance, submission, submitter, AKVO_form_version, country, test, organisation, contract_number, id_planting_site, name_village, name_owner, remark, planting_date, tree_number, planting_distance, only_location, gps_corner_1, gps_corner_2, gps_corner_3, gps_corner_4)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
    (identifier_akvo, displayname, device_id, instance, submission_date_trunc, submitter, form_version, country, test, organisation, contract_number, planting_site_id, name_village, name_owner, remark, planting_date_trunc, number_trees_registered, avg_planting_distance, multiple_planting_locations, gps_corner_1, gps_corner_2, gps_corner_3, gps_corner_4))

    conn.commit()

    # populate database with the 10 photos taken
    for k,v in registration_light['responses']['252306886'][0].items():
        photo_urls = v.get('filename')
        if v['location'] is not None:
                photo_geotag_lat = v.get('location')['latitude']
                photo_lat = str(photo_geotag_lat)
                photo_geotag_lon = v.get('location')['longitude']
                photo_lon = str(photo_geotag_lon)
                photo_location = 'POINT('+ photo_lon + ' ' + photo_lat + ')'
                photo_geotag_location = None

        else:
            photo_lat = None
            photo_lon = None
            photo_geotag_location = None

        cur.execute('''INSERT INTO AKVO_Tree_registration_photos_light_version (identifier_akvo, instance, photo_url, photo_geotag_location)
        VALUES (%s,%s,%s,%s)''', (identifier_akvo, instance, photo_urls, photo_geotag_location))

        conn.commit()


    # populate database with species and species number.
    # due to the fact that the species naming convention was changed, the json became corrupted. Instead of species
    # being stored in a list/dictionary, species was stored in a string format. This could not be changed back.
    # Since this form is not of major importance, I kept it like this and now just read the entire string (latin and local) in both columns
    try:
        registration_light['responses']['252306884']
        #print('CHECK OUTPUT: ', registration_light['responses']['252306884'])

    except KeyError:
        species_latin = ''
        species_local = ''
        number_species = 0

    else:
        for x in registration_light['responses']['252306884']:
            #print('SEE ERROR 2: ', x)
            try:
                species_latin = x['248220146']
            except (KeyError, IndexError):
                species_latin = None
                species_local = None
            else:
                species_latin = x['248220146']
                species_local = x['248220146']

            number_trees_planted_per_species = x.get('248220145', 0)
            #print(number_trees_planted_per_species)
            #print(species_latin, species_local)

            cur.execute('''INSERT INTO AKVO_Tree_registration_species_light_version (identifier_akvo, instance, lat_name_species, local_name_species, number_species)
            VALUES (%s,%s,%s,%s,%s)''', (identifier_akvo, instance, species_latin, species_local, number_trees_planted_per_species))

            conn.commit()
