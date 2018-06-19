"""
DRAFT - Combine data from two csv's and update data in AGOL hosted feature layer with the new combined dataset
"""
#TODO: major weakness/pain point, when need to add/revise/delete field etc you have to manually change in multiple spots. Make code more flexible to solve this issue.

# IMPORTS
from collections import namedtuple
import json
import os
import sqlite3


def main():

    # CLASSES
    class Bridge_Class():
        def __init__(self, data_dict):
            self.row_id = data_dict["Row_ID"]
            self.md_district = data_dict["MD_District"]
            self.us_district = data_dict["US_District"]
    class MDGov_Class():
        def __init__(self, data_dict):
            self.md_district = data_dict["MD_District"]
            self.state_senator = data_dict["State_Senator"]
            self.state_senator_party = data_dict["State_Senator_Party"]
            self.state_senator_maryland_manual_online = data_dict["State_Senator_Maryland_Manual_Online"]
            self.state_representative_1 = data_dict["State_Representative_1"]
            self.state_representative_1_party = data_dict["State_Representative_1_Party"]
            self.state_representative_1_maryland_manual_online = data_dict["State_Representative_1_Maryland_Manual_Online"]
            self.state_representative_2 = data_dict["State_Representative_2"]
            self.state_representative_2_party = data_dict["State_Representative_2_Party"]
            self.state_representative_2_maryland_manual_online = data_dict["State_Representative_2_Maryland_Manual_Online"]
            self.state_representative_3 = data_dict["State_Representative_3"]
            self.state_representative_3_party = data_dict["State_Representative_3_Party"]
            self.state_representative_3_maryland_manual_online = data_dict["State_Representative_3_Maryland_Manual_Online"]
            self.governor = data_dict["Governor"]
            self.governor_maryland_manual_online = data_dict["Governor_Maryland_Manual_Online"]
            self.lt_governor = data_dict["Lt_Governor"]
            self.lt_governor_maryland_manual_online = data_dict["Lt_Governor_Maryland_Manual_Online"]
            self.attorney_general = data_dict["Attorney_General"]
            self.attorney_general_maryland_manual_online = data_dict["Attorney_General_Maryland_Manual_Online"]
            self.comptroller = data_dict["Comptroller"]
            self.comptroller_maryland_manual_online = data_dict["Comptroller_Maryland_Manual_Online"]
    class USGov_Class():
        def __init__(self, data_dict):
            self.us_district = data_dict["US_District"]
            self.name = data_dict["Name"]
            self.label = data_dict["Label"]
            self.us_senator_1 = data_dict["US_Senator_1"]
            self.us_senator_1_party = data_dict["US_Senator_1_Party"]
            self.us_senator_1_maryland_manual_online = data_dict["US_Senator_1_Maryland_Manual_Online"]
            self.us_senator_2 = data_dict["US_Senator_2"]
            self.us_senator_2_party = data_dict["US_Senator_2_Party"]
            self.us_senator_2_maryland_manual_online = data_dict["US_Senator_2_Maryland_Manual_Online"]
            self.us_representatives = data_dict["US_Representatives"]
            self.us_representatives_party = data_dict["US_Representatives_Party"]
            self.us_representatives_maryland_manual_online = data_dict["US_Representatives_Maryland_Manual_Online"]

    # VARIABLES - CONSTANTS
    Variable = namedtuple("Variable", "value")
    CSV_BRIDGE = Variable(r"Docs\20180613_Bridge.csv")
    CSV_MDGOV = Variable(r"Docs\20180619_ElectedOfficialsMarylandGovernment.csv")
    CSV_USGOV = Variable(r"Docs\20180619_ElectedOfficialsUSGovernment.csv")
    SD_FILE_STORAGE_LOCATION = Variable(r"E:\DoIT_MDElectAppProcess\Docs\sd_file_storage")
    SQL_CREATE_BRIDGE = Variable("""CREATE TABLE BRIDGE (
                        Row_ID text primary key, 
                        MD_District text,
                        US_District text                    
                        )""")
    SQL_CREATE_MDGOV = Variable("""CREATE TABLE MDGOV (
                        MD_District text primary key,
                        State_Senator text,
                        State_Senator_Party text,
                        State_Senator_Maryland_Manual_Online text,
                        State_Representative_1 text,
                        State_Representative_1_Maryland_Manual_Online text,
                        State_Representative_1_Party text,
                        State_Representative_2 text,
                        State_Representative_2_Party text,
                        State_Representative_2_Maryland_Manual_Online text,
                        State_Representative_3 text,
                        State_Representative_3_Party text,
                        State_Representative_3_Maryland_Manual_Online text,
                        Governor text,
                        Governor_Maryland_Manual_Online text,
                        Lt_Governor text,
                        Lt_Governor_Maryland_Manual_Online text,
                        Attorney_General text,
                        Attorney_General_Maryland_Manual_Online text,
                        Comptroller text,
                        Comptroller_Maryland_Manual_Online text
                        )""")
    SQL_CREATE_USGOV = Variable("""CREATE TABLE USGOV (
                        US_District text primary key,
                        Name integer,
                        Label text,
                        US_Senator_1 text,
                        US_Senator_1_Party text,
                        US_Senator_1_Maryland_Manual_Online text,
                        US_Senator_2 text,
                        US_Senator_2_Party text,
                        US_Senator_2_Maryland_Manual_Online text,
                        US_Representatives text,
                        US_Representatives_Party text,
                        US_Representative_Maryland_Manual_Online text
                        )""")
    SQL_INSERT_BRIDGE = Variable("""INSERT OR IGNORE INTO bridge VALUES (:row_id, :md_district, :us_district)""")
    SQL_INSERT_MDGOV = Variable("""INSERT OR IGNORE INTO mdgov VALUES (:md_district, :state_senator, :state_senator_party, :state_senator_maryland_manual_online, :state_representative_1, :state_representative_1_party, :state_representative_1_maryland_manual_online, :state_representative_2, :state_representative_2_party, :state_representative_2_maryland_manual_online, :state_representative_3,  :state_representative_3_party, :state_representative_3_maryland_manual_online, :governor, :governor_maryland_manual_online, :lt_governor, :lt_governor_maryland_manual_online, :attorney_general, :attorney_general_maryland_manual_online, :comptroller, :comptroller_maryland_manual_online)""")
    SQL_INSERT_USGOV = Variable("""INSERT OR IGNORE INTO usgov VALUES (:us_district, :name, :label, :us_senator_1, :us_senator_1_party, :us_senator_1_maryland_manual_online, :us_senator_2, :us_senator_2_party, :us_senator_2_maryland_manual_online, :us_representatives, :us_representatives_party, :us_representatives_maryland_manual_online)""")
    SQL_SELECT_OUTPUT_DATA = Variable("""SELECT MDGOV.*, USGOV.*, BRIDGE.Row_ID FROM MDGOV, BRIDGE, USGOV WHERE MDGOV.MD_District = BRIDGE.MD_District AND BRIDGE.US_District = USGOV.US_District""")

    # VARIABLES - OTHER
    arcpro_project_path = r"E:\DoIT_MDElectAppProcess\Docs\ElectedOfficals\ElectedOfficals.aprx"
    csv_namedtuples_list = [CSV_BRIDGE, CSV_MDGOV, CSV_USGOV]
    class_types_namedtuples_list = [Bridge_Class, MDGov_Class, USGov_Class]
    credentials_path = r"Docs\credentials.json"
    csv_classobject_pairing = dict(zip(csv_namedtuples_list, class_types_namedtuples_list))
    csv_headers_to_fc_field_names_dict = {"MD_District": "MD_District",
                                          "State_Senator": "MD_Senator",
                                          "State_Senator_Party": "MD_Senator_Party",
                                          "State_Senator_Maryland_Manual_Online": "MD_Senator_Manual_Online",
                                          "State_Representative_1": "MD_Representative_1",
                                          "State_Representative_1_Party": "MD_Rep_1_Party",
                                          "State_Representative_1_Maryland_Manual_Online": "MD_Rep_1_Manual_Online",
                                          "State_Representative_2": "MD_Representative_2",
                                          "State_Representative_2_Party": "MD_Rep_2_Party",
                                          "State_Representative_2_Maryland_Manual_Online": "MD_Rep_2_Manual_Online",
                                          "State_Representative_3": "MD_Representative_3",
                                          "State_Representative_3_Party": "MD_Rep_3_Party",
                                          "State_Representative_3_Maryland_Manual_Online": "MD_Rep_3_Manual_Online",
                                          "Governor": "Governor",
                                          "Governor_Maryland_Manual_Online": "Governor_Manual_Online",
                                          "Lt_Governor": "Lt_Governor",
                                          "Lt_Governor_Maryland_Manual_Online": "Lt_Governor_Manual_Online",
                                          "Attorney_General": "Attorney_General",
                                          "Attorney_General_Maryland_Manual_Online": "Attorney_General_Manual_Online",
                                          "Comptroller": "Comptroller",
                                          "Comptroller_Maryland_Manual_Online": "Comptroller_Manual_Online",
                                          "US_District": "US_District",
                                          "Name": "Name",
                                          "Label": "Label",
                                          "US_Senator_1": "US_Senator_1",
                                          "US_Senator_1_Party": "US_Senator_1_Party",
                                          "US_Senator_1_Maryland_Manual_Online": "US_Senator_1_Manual_Online",
                                          "US_Senator_2": "US_Senator_2",
                                          "US_Senator_2_Party": "US_Senator_2_Party",
                                          "US_Senator_2_Maryland_Manual_Online": "US_Senator_2_Manual_Online",
                                          "US_Representatives": "US_Representatives",
                                          "US_Representatives_Party": "US_Representatives_Party",
                                          "US_Representative_Maryland_Manual_Online": "US_Reps_Manual_Online",
                                          "Row_ID": "Row_ID"
                                          }
    fc_name = "ElectedOfficials"
    path_master_dataset_csv = r"E:\DoIT_MDElectAppProcess\Docs\test_sql_pull_data.csv"      # TESTING
    path_project_gdb = r"E:\DoIT_MDElectAppProcess\Docs\ElectedOfficals\ElectedOfficals.gdb"
    portal = "http://maryland.maps.arcgis.com"
    record_list_list = []
    record_strings_list = []
    share_everyone = False
    share_groups = ""
    share_organization = False
    sd_featureservice_name = "Elected_Officials"
    sql_create_namedtuples_list = [SQL_CREATE_BRIDGE, SQL_CREATE_MDGOV, SQL_CREATE_USGOV]
    sql_insert_namedtuples_list = [SQL_INSERT_BRIDGE, SQL_INSERT_MDGOV, SQL_INSERT_USGOV]
    test_database = r"Docs\testdb.db"           # TESTING
        # Dependent Variables - Derived
    csv_sqlinsert_pairing = dict(zip(csv_namedtuples_list, sql_insert_namedtuples_list))
    sd_draft_filename = os.path.join(SD_FILE_STORAGE_LOCATION.value, "Elected_Officals.sddraft")
    sd_filename = os.path.join(SD_FILE_STORAGE_LOCATION.value, "Elected_Officals.sd")

    assert os.path.exists(credentials_path)
    assert os.path.exists(CSV_BRIDGE.value)
    assert os.path.exists(CSV_MDGOV.value)
    assert os.path.exists(CSV_USGOV.value)
    assert os.path.exists(path_project_gdb)


    # FUNCTIONS
    def close_database_connection(connection):
        connection.close()
        return
    def commit_to_database(connection):
        connection.commit()
        return
    def create_database_connection(database):
        if database == ":memory:":
            return sqlite3.connect(database=":memory:")
        else:
            return sqlite3.connect(database=database)
    def create_database_cursor(connection):
        return connection.cursor()
    def execute_sql_command(cursor, sql_command, parameters_sequence=()):
        result = cursor.execute(sql_command, parameters_sequence)
        return result
    def reverse_dictionary(dictionary):
        return {value: key for key, value in dictionary.items()}

    # FUNCTIONALITY
    # ___________________________________________
    # PART 1 - Build in-memory sqlite3 database and populate from csv files
    # ___________________________________________

    # Set up SQLite3 in memory database. Establish database tables
    # conn = create_database_connection(test_database)                  # TESTING
    conn = create_database_connection(":memory:")
    curs = create_database_cursor(conn)
    for sql_namedtuple in sql_create_namedtuples_list:
        execute_sql_command(cursor=curs, sql_command=sql_namedtuple.value)

    # Access CSV's, work on each one storing contents as objects and writing to database
    for csv_namedtuple in csv_namedtuples_list:
        with open(csv_namedtuple.value, 'r') as csv_file_handler:
            records_list_list = [(line.strip()).split(",") for line in csv_file_handler]
        headers_list = records_list_list.pop(0)

        for record_list in records_list_list:
            record_dictionary = dict(zip(headers_list, record_list))

            # create the type of object appropriate to csv being inspected
            data_object = csv_classobject_pairing[csv_namedtuple](record_dictionary)

            # get the appropriate insert sql statement and write CSV's to appropriate database table
            insert_sql_namedtuple = csv_sqlinsert_pairing[csv_namedtuple]
            execute_sql_command(cursor=curs,
                                sql_command=insert_sql_namedtuple.value,
                                parameters_sequence=data_object.__dict__)

    # SQL call to database to join tables and make one master dataset for upload
    results = execute_sql_command(cursor=curs,sql_command=SQL_SELECT_OUTPUT_DATA.value)
    headers_master_dataset = tuple([desc_tup[0] for desc_tup in curs.description])
    full_data_dictionary_from_csv_data = {row[-1] : tuple(row) for row in results}

    # SQL Commit and Close things out
    commit_to_database(conn)
    close_database_connection(conn)


    #___________________________________________
    # PART 2 - Access feature class and update using data from in-memory database master query results from Step 1
    #___________________________________________

    # SPATIAL
    # access feature class
    import arcpy        # Delayed import for performance
    arcpy.env.workspace = path_project_gdb
    fc_fields = arcpy.ListFields(fc_name)

    # grab the field names
    fc_field_names_list = [(fc_field.name).strip() for fc_field in fc_fields]

    # reverse the header mapping between gis data and csv data.
    # TODO: May be able to reverse hardcoded variable and eliminate this step
    fc_field_names_to_csv_headers_dict = reverse_dictionary(csv_headers_to_fc_field_names_dict)

    # Isolate the fc fields that have a corresponding header in the csv file. Excluding spatial fields like ObjectID and Shape
    fc_field_names_matching_header_list = [field_name for field_name in fc_field_names_list if
                                           field_name in fc_field_names_to_csv_headers_dict.keys()]

    # Build dictionary of header keys with their index position values
    fc_field_names_matching_header_index_dictionary = reverse_dictionary(
        dict(enumerate(fc_field_names_matching_header_list)))

    with arcpy.da.UpdateCursor(in_table=fc_name, field_names=fc_field_names_matching_header_list) as update_cursor:
        for row in update_cursor:
            # Use header index dictionary to supply index position of row_id in modified pull (no spatial fields)
            current_row_id = row[fc_field_names_matching_header_index_dictionary["Row_ID"]]
            # use the current row_id to get the correct record of data from the updated csv data in the in-memory database
            csv_data_for_current_row_id = full_data_dictionary_from_csv_data[current_row_id]
            # use the new record to replace the existing fc row
            update_cursor.updateRow(csv_data_for_current_row_id)


    #___________________________________________
    # PART 3 - Overwrite arcgis online hosted feature layer using republishing from arcpro project.
    # From example on ArcGIS for Python API. See resources below.
    # https://www.esri.com/arcgis-blog/products/api-python/analytics/updating-your-hosted-feature-services-with-arcgis-pro-and-the-arcgis-api-for-python/
    # https://esri.github.io/arcgis-python-api/apidoc/html/index.html
    #___________________________________________

    from arcgis.gis import GIS          # Delayed import for performance

    # Gather credentials
    with open(credentials_path, 'r') as file_handler:
        file_contents = file_handler.read()
    credentials_json = json.loads(file_contents)
    agol_username = credentials_json["username"]
    agol_password = credentials_json["password"]

    # Create a new SDDraft and stage to SD
    print("Creating SD file")
    arcpy.env.overwriteOutput = True
    project = arcpy.mp.ArcGISProject(aprx_path=arcpro_project_path)
    map = project.listMaps()[0]  # Keep it simple, have only one map in aprx. Process grabs first map.
    arcpy.mp.CreateWebLayerSDDraft(map_or_layers=map,
                                   out_sddraft=sd_draft_filename,
                                   service_name=sd_featureservice_name,
                                   server_type="MY_HOSTED_SERVICES",
                                   service_type="FEATURE_ACCESS",
                                   folder_name="",
                                   overwrite_existing_service=True,
                                   copy_data_to_server=True,
                                   enable_editing=False,
                                   allow_exporting=False,
                                   enable_sync=False,
                                   summary=None,
                                   tags=None,
                                   description=None,
                                   credits=None,
                                   use_limitations=None)
    arcpy.StageService_server(in_service_definition_draft=sd_draft_filename,
                              out_service_definition=sd_filename)

    # Make connection with AGOL
    print("Connecting to {}".format(portal))
    gis = GIS(url=portal,
              username=agol_username,
              password=agol_password,
              key_file=None,
              cert_file=None,
              verify_cert=True,
              set_active=True,
              client_id=None,
              profile=None)

    # Find the existingSD, update it, publish to overwrite and set sharing and metadata.
    # Must be owned by the account whose credentials this process uses, and named the same
    print("Search for original service definition file (.sd) on portal…")
    try:
        agol_sd_item = gis.content.search("{} AND owner:{}".format(sd_featureservice_name, agol_username),
                                          item_type="Service Definition")[0]
    except:
        print(
            "Search not successful. Check that .sd file is present and named identically, and that the account credentials supplied are for the owner of the sd file.")
        exit()

    print("Found SD: {}, ID: {} \n Uploading and overwriting…".format(agol_sd_item.title, agol_sd_item.id))
    agol_sd_item.update(data=sd_filename)

    print("Overwriting existing feature service…")
    feature_service = agol_sd_item.publish(overwrite=True)

    if share_organization or share_everyone or share_groups:
        print("Setting sharing options…")
        feature_service.share(org=share_organization, everyone=share_everyone, groups=share_groups)

    print("Finished updating: {} – ID: {}".format(feature_service.title, feature_service.id))

if __name__ == "__main__":
    main()
