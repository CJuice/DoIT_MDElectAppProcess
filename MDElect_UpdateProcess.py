"""
Read data in csv files and create sqlite3 database in-memory, overwrite feature class data using database, and overwrite hosted feature layer on ArcGIS Online.

Process looks for csv files of data on elected officials. The Maryland State Archive agency maintains the elected
officials data using two tabs in a Google Sheet. We have a process that pulls csv's of the tabs. One tab is MD specific,
the other is Federal specific. The process takes the data from those csv's and builds and loads tables using sqlite3.
Due to many-to-many relationships between MD District and US District, a bridge table is necessary. A Bridge csv exists.
The csv is read and used to create a database table the same as the elected officials data. The database is used to build
a set of new records. These records, with unique identifier row_id, are used to update/overwrite the attributes in a
feature class of polygons representinng unique district combination areas. The combination areas are unique combinations
of the MD Districts and the US Districts layers. An ArcGIS Pro project exists. It contains a feature class of the
conbination polygons. This layer is geometrically identical to the hosted feature layer on ArcGIS Online.
Once the feature class is updated, the Python API for ArcGIS is used to republish the ArcPro project and overwrite the
hosted feature layer on ArcGIS Online.
Author: CJuice
Date: 20180619
"""
#TODO: weakness/pain point, when need to add/revise/delete field etc you have to manually change in multiple spots. Make code more flexible to solve this issue.

# IMPORTS - Some delayed imports exist, for performance improvement
from collections import namedtuple
import configparser
import os
import sqlite3

def main():

    # CLASSES
    class Bridge_Class():
        """
        Bridge table object between US Districts and MD Districts.
        """
        def __init__(self, data_dict):
            """
            Instantiate object and populate with values from data_dictionary.

            :param data_dict:  dictionary of header name key to record value for Bridge csv
            """
            self.row_id = data_dict["Row_ID"]
            self.md_district = data_dict["MD_District"]
            self.us_district = data_dict["US_District"]
    class MDGov_Class():
        """
        MD Government specific elected officials data object.
        """
        def __init__(self, data_dict):
            """
            Instantiate object and populate with values from data_dictionary.

            :param data_dict: dictionary of header name key to record value for MD Districts csv
            """
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
        """
        US Government specific elected officials data object.
        """
        def __init__(self, data_dict):
            """
            Instantiate object and populate with values from data_dictionary.

            :param data_dict: dictionary of header name key to record value for US Districts csv
            """
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
    ARCGIS_ONLINE_PORTAL = Variable("https://maryland.maps.arcgis.com")
    ARCPRO_PROJECT_PATH = Variable(r"E:\DoIT_MDElectAppProcess\Docs\ElectedOfficals\ElectedOfficals.aprx")
    CREDENTIALS_PATH = Variable(r"Docs\credentials.cfg")
    CSV_PATH_BRIDGE = Variable(r"Docs\20180613_Bridge.csv")
    CSV_PATH_MDGOV = Variable(r"Docs\20180619_ElectedOfficialsMarylandGovernment.csv")
    CSV_PATH_USGOV = Variable(r"Docs\20180619_ElectedOfficialsUSGovernment.csv")
    FC_NAME = Variable("ElectedOfficials")
    GDB_PATH_ARCPRO_PROJECT = Variable(r"E:\DoIT_MDElectAppProcess\Docs\ElectedOfficals\ElectedOfficals.gdb")
    SD_FEATURE_SERVICE_NAME = Variable("Elected_Officials")
    SD_FILE_STORAGE_LOCATION = Variable(r"E:\DoIT_MDElectAppProcess\Docs\sd_file_storage")
    SD_FILENAME_DRAFT = Variable("Elected_Officals.sddraft")
    SD_FILENAME = Variable("Elected_Officals.sd")
    SQL_CREATE_BRIDGE = Variable("""CREATE TABLE BRIDGE (Row_ID text primary key, MD_District text, US_District text)""")
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
    class_types_namedtuples_list = [Bridge_Class, MDGov_Class, USGov_Class]
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
    csv_paths_namedtuples_list = [CSV_PATH_BRIDGE, CSV_PATH_MDGOV, CSV_PATH_USGOV]
    sd_draft_filename = os.path.join(SD_FILE_STORAGE_LOCATION.value, SD_FILENAME_DRAFT.value)
    sd_filename = os.path.join(SD_FILE_STORAGE_LOCATION.value, SD_FILENAME.value)
    share_everyone = False
    share_groups = ""
    share_organization = False
    sql_create_namedtuples_list = [SQL_CREATE_BRIDGE, SQL_CREATE_MDGOV, SQL_CREATE_USGOV]
    sql_insert_namedtuples_list = [SQL_INSERT_BRIDGE, SQL_INSERT_MDGOV, SQL_INSERT_USGOV]
        # Dependent Variables
    csv_classobject_pairing = dict(zip(csv_paths_namedtuples_list, class_types_namedtuples_list))
    csv_sqlinsert_pairing = dict(zip(csv_paths_namedtuples_list, sql_insert_namedtuples_list))

    # FUNCTIONS
    def close_database_connection(connection):
        """
        Close the sqlite3 database connection.

        :param connection: sqlite3 connection to be closed
        :return: Nothin
        """
        connection.close()
        return
    def clean_and_split(line):
        """
        Strip line string and split on commas into a list, return list.

        :param line: record string from csv dataset
        :return: cleaned and split line as list
        """
        return (line.strip()).split(",")
    def commit_to_database(connection):
        """
        Make a commit to sqlite3 database.

        :param connection: sqlite database connection
        :return: Nothing
        """
        connection.commit()
        return
    def create_database_connection(database):
        """
        Establish sqlite3 database connection and return connection.

        :param database: database to create
        :return: database connection
        """
        if database == ":memory:":
            return sqlite3.connect(database=":memory:")
        else:
            return sqlite3.connect(database=database)
    def create_database_cursor(connection):
        """
        Create cursor for database data access and return cursor.

        :param connection: database connection to use
        :return: database cursor
        """
        return connection.cursor()
    def execute_sql_command(cursor, sql_command, parameters_sequence=()):
        """
        Execute a sql command and return result.

        :param cursor: database cursor to be used
        :param sql_command: sql command to be executed
        :param parameters_sequence: parameters for substitution in sql string placeholder
        :return: results of query
        """
        result = cursor.execute(sql_command, parameters_sequence)
        return result
    def reverse_dictionary(dictionary):
        """
        Swap the key and value, creating new dictionary, and return new dictionary.
        :param dictionary: in dictionary to be reversed
        :return: reversed dictionary
        """
        return {value: key for key, value in dictionary.items()}

    # FUNCTIONALITY
    # Assert that core files are present.
    assert os.path.exists(CREDENTIALS_PATH.value)
    assert os.path.exists(CSV_PATH_BRIDGE.value)
    assert os.path.exists(CSV_PATH_MDGOV.value)
    assert os.path.exists(CSV_PATH_USGOV.value)
    assert os.path.exists(GDB_PATH_ARCPRO_PROJECT.value)
    assert os.path.exists(SD_FILE_STORAGE_LOCATION.value)

    # ___________________________________________
    # PART 1 - Need to build in-memory sqlite3 database and populate from csv files
    # ___________________________________________

    # Need SQLite3 in memory database and tables for each csv contents.
    conn = create_database_connection(":memory:")
    curs = create_database_cursor(conn)
    for sql_namedtuple in sql_create_namedtuples_list:
        execute_sql_command(cursor=curs, sql_command=sql_namedtuple.value)

    # Need to access CSV's, work on each one storing contents as objects and writing to database
    for csv_path_namedtuple in csv_paths_namedtuples_list:
        with open(csv_path_namedtuple.value, 'r') as csv_file_handler:
            records_list_list = [(clean_and_split(line=line)) for line in csv_file_handler]

        # Need the headers from csv. Remove them from list so don't have to skip that row in the for loop below
        headers_list = records_list_list.pop(0)
        for record_list in records_list_list:
            record_dictionary = dict(zip(headers_list, record_list))

            # Need to create the type of class object (Bridge, MDGov, USGov) appropriate to csv being inspected
            data_object = csv_classobject_pairing[csv_path_namedtuple](record_dictionary)

            # Need to get the appropriate insert sql statement and write CSV's to appropriate database table
            insert_sql_namedtuple = csv_sqlinsert_pairing[csv_path_namedtuple]
            execute_sql_command(cursor=curs,
                                sql_command=insert_sql_namedtuple.value,
                                parameters_sequence=data_object.__dict__)

    # Need to make call to database to join tables and create one master dataset for overwrite/upload use
    query_results = execute_sql_command(cursor=curs,sql_command=SQL_SELECT_OUTPUT_DATA.value)
    full_data_dictionary_from_csv_data = {row[-1] : tuple(row) for row in query_results}

    # SQL Commit and Close out
    commit_to_database(conn)
    close_database_connection(conn)

    #___________________________________________
    # PART 2 - Need to access feature class and update using data from in-memory database master query results from Step 1
    #___________________________________________

    # SPATIAL
    # Need access to the feature class
    import arcpy        # Delayed import for performance
    arcpy.env.workspace = GDB_PATH_ARCPRO_PROJECT.value

    # Need the fc field names
    fc_fields = arcpy.ListFields(FC_NAME.value)
    fc_field_names_list = [(field.name).strip() for field in fc_fields]

    # Need to reverse the header mapping between gis data and csv data. Originally created opposite to end need, meh.
    fc_field_names_to_csv_headers_dict = reverse_dictionary(csv_headers_to_fc_field_names_dict)

    # Need to excludes spatial fields like ObjectID and Shape. Isolate the fc fields, whose field names have a corresponding header in the csv file.
    fc_field_names_matching_header_list = [name for name in fc_field_names_list if
                                           name in fc_field_names_to_csv_headers_dict.keys()]

    # Need index position of matching, but after isolating non-spatial fields need new index positions.
    #   Build dictionary of header keys with their 'new' index position values
    fc_field_names_matching_csv_header__index_dictionary = reverse_dictionary(
        dict(enumerate(fc_field_names_matching_header_list)))

    # Need to step through every feature class row and update the data with data from csv.
    with arcpy.da.UpdateCursor(in_table=FC_NAME.value, field_names=fc_field_names_matching_header_list) as update_cursor:
        for row in update_cursor:

            # Use header index dictionary to supply index position of row_id in modified pull (no spatial fields)
            current_row_id = row[fc_field_names_matching_csv_header__index_dictionary["Row_ID"]]

            # use the current row_id to get the correct record of data from the updated csv data in the in-memory database
            csv_data_for_current_row_id = full_data_dictionary_from_csv_data[current_row_id]

            # use the new record to replace the existing fc row
            update_cursor.updateRow(csv_data_for_current_row_id)


    #___________________________________________
    # PART 3 - Need to wverwrite arcgis online hosted feature layer using republishing from ArcPro project.
    # Taken from example on ArcGIS for Python API. See resources below.
    # https://www.esri.com/arcgis-blog/products/api-python/analytics/updating-your-hosted-feature-services-with-arcgis-pro-and-the-arcgis-api-for-python/
    #___________________________________________

    from arcgis.gis import GIS          # Delayed import for performance

    # Need credentials from config file
    config = configparser.ConfigParser()
    config.read(filenames=CREDENTIALS_PATH.value)
    agol_username = config['DEFAULT']["username"]
    agol_password = config['DEFAULT']["password"]

    # Need a new SDDraft and to stage it to SD
    arcpy.env.overwriteOutput = True
    arcpro_project = arcpy.mp.ArcGISProject(aprx_path=ARCPRO_PROJECT_PATH.value)
    arcpro_map = arcpro_project.listMaps()[0]  # Note: keep your pro project simple, have only one map in aprx. Process grabs first map.
    arcpy.mp.CreateWebLayerSDDraft(map_or_layers=arcpro_map,
                                   out_sddraft=sd_draft_filename,
                                   service_name=SD_FEATURE_SERVICE_NAME.value,
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

    # Need connection with AGOL
    gis = GIS(url=ARCGIS_ONLINE_PORTAL.value,
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
    try:
        agol_sd_item = gis.content.search("{} AND owner:{}".format(SD_FEATURE_SERVICE_NAME.value, agol_username),
                                          item_type="Service Definition")[0]
    except:
        print(
            "Search for .sd file not successful. Check that .sd file is present and named identically, and that the account credentials supplied are for the owner of the .sd file.")
        exit()

    agol_sd_item.update(data=sd_filename)
    feature_service = agol_sd_item.publish(overwrite=True)
    if share_organization or share_everyone or share_groups:
        feature_service.share(org=share_organization, everyone=share_everyone, groups=share_groups)

if __name__ == "__main__":
    main()
