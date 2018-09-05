"""
Read data in csv files and create sqlite3 database in-memory, overwrite feature class data using database, and overwrite hosted feature layer on ArcGIS Online.

Process looks for csv files of data on elected officials. The Maryland State Archive agency maintains the elected
officials data using two tabs in a Google Sheet. We have a process that pulls csv's of the tabs. One tab is MD specific,
the other is Federal specific. The process takes the data from those csv's and builds and loads tables using sqlite3.
Due to many-to-many relationships between MD District and US District, a bridge table is necessary. A Bridge csv exists.
The csv is read and used to create a database table the same as the elected officials data. The database is used to build
a set of new records. These records, with unique identifier row_id, are used to update/overwrite the attributes in a
feature class of polygons representing unique district combination areas. The combination areas are unique combinations
of the MD Districts and the US Districts layers. An ArcGIS Pro project exists. It contains a feature class of the
combination polygons. This layer is geometrically identical to the hosted feature layer on ArcGIS Online.
Once the feature class is updated, the Python API for ArcGIS is used to republish the ArcPro project and overwrite the
hosted feature layer on ArcGIS Online.
Author: CJuice
Date: 20180815
Revision: 20180905, NOTE: If encounter the following error:
    'Traceback: in CreateWebLayerSDDraft return _convertArcObjectToPythonObject(...) RuntimeError'
    it appears to be that the ArcPro session login has expired. Despite the credentials being available for login it
    looks like ArcPro must be opened, sign-in must be completed, and the sign-in must be valid when the process is run.
    ArcPro does not need to be open when the process is run; Just the login must be valid.
"""
# TODO: weakness/pain point, when need to add/revise/delete field etc you have to manually change in multiple spots. Make code more flexible to solve this issue.

# TODO: Test to see if this works after undergoing the overhaull and redesign


def main():
    # IMPORTS - Some delayed imports exist, for performance improvement
    import configparser
    import MDElect_Classes as mdcls
    import MDElect_Variables as myvars
    import os

    class_types_namedtuples_list = [mdcls.Bridge_Class, mdcls.MDGov_Class, mdcls.USGov_Class]
    csv_paths_namedtuples_list = [myvars.CSV_PATH_BRIDGE, myvars.CSV_PATH_MDGOV, myvars.CSV_PATH_USGOV]
    sd_draft_filename = os.path.join(myvars.SD_FILE_STORAGE_LOCATION.value, myvars.SD_FILENAME_DRAFT.value)
    sd_filename = os.path.join(myvars.SD_FILE_STORAGE_LOCATION.value, myvars.SD_FILENAME.value)
    share_everyone = False
    share_groups = ""
    share_organization = False
    sql_create_namedtuples_list = [myvars.SQL_CREATE_BRIDGE, myvars.SQL_CREATE_MDGOV, myvars.SQL_CREATE_USGOV]
    sql_insert_namedtuples_list = [myvars.SQL_INSERT_BRIDGE, myvars.SQL_INSERT_MDGOV, myvars.SQL_INSERT_USGOV]
    #   Dependent Variables
    csvobj_classobj_pairing = dict(zip(csv_paths_namedtuples_list, class_types_namedtuples_list))
    csvobj_sqlinsertobj_pairing = dict(zip(csv_paths_namedtuples_list, sql_insert_namedtuples_list))

    # FUNCTIONALITY
    #   Assert that core files are present.
    assert os.path.exists(myvars.ARCPRO_PROJECT_PATH.value)
    assert os.path.exists(myvars.CREDENTIALS_PATH.value)
    assert os.path.exists(myvars.CSV_PATH_BRIDGE.value)
    assert os.path.exists(myvars.CSV_PATH_MDGOV.value)
    assert os.path.exists(myvars.CSV_PATH_USGOV.value)
    assert os.path.exists(myvars.GDB_PATH_ARCPRO_PROJECT.value)
    assert os.path.exists(myvars.SD_FILE_STORAGE_LOCATION.value)

    # ___________________________________________
    # PART 1 - Need to build in-memory sqlite3 database and populate from csv files
    # ___________________________________________
    # Need SQLite3 in memory database and tables for each csv contents.
    conn = mdcls.Util_Class.create_database_connection(":memory:")
    curs = mdcls.Util_Class.create_database_cursor(conn)
    for sql_namedtuple in sql_create_namedtuples_list:
        mdcls.Util_Class.execute_sql_command(cursor=curs, sql_command=sql_namedtuple.value)

    # Need to access CSVs, work on each one storing contents as objects and writing to database
    for csv_path_namedtuple in csv_paths_namedtuples_list:
        with open(csv_path_namedtuple.value, 'r') as csv_file_handler:
            records_list_list = [(mdcls.Util_Class.clean_and_split(line=line)) for line in csv_file_handler]

        # Need the headers from csv. Remove them from list so don't have to skip that row in the for loop below
        headers_list = records_list_list.pop(0)
        for record_list in records_list_list:
            record_dictionary = dict(zip(headers_list, record_list))

            # Need to create an object of the type (Bridge, MDGov, USGov) appropriate to csv being inspected
            data_object = csvobj_classobj_pairing[csv_path_namedtuple](record_dictionary)

            # Need to get the appropriate insert sql statement and write CSV's to appropriate database table
            insert_sql_namedtuple = csvobj_sqlinsertobj_pairing[csv_path_namedtuple]
            mdcls.Util_Class.execute_sql_command(cursor=curs,
                                                 sql_command=insert_sql_namedtuple.value,
                                                 parameters_sequence=data_object.__dict__)

    # Need to make call to database to join tables and create one master dataset for overwrite/upload use
    query_results = mdcls.Util_Class.execute_sql_command(cursor=curs,sql_command=myvars.SQL_SELECT_OUTPUT_DATA.value)
    full_data_dictionary_from_csv_data = {row[-1] : tuple(row) for row in query_results}

    # SQL Commit and Close out
    mdcls.Util_Class.commit_to_database(conn)
    mdcls.Util_Class.close_database_connection(conn)

    #___________________________________________
    # PART 2 - Need to access feature class and update using data from in-memory database master query results from Step 1
    #___________________________________________

    # Need credentials from config file
    config = configparser.ConfigParser()
    config.read(filenames=myvars.CREDENTIALS_PATH.value)
    agol_username = config['DEFAULT']["username"]
    agol_password = config['DEFAULT']["password"]

    # SPATIAL
    import arcpy        # Delayed import for performance

    # Need to sign in to portal so Pro can write to agol
    maryland_portal = arcpy.SignInToPortal("https://maryland.maps.arcgis.com", agol_username, agol_password)
    active_portal = arcpy.GetActivePortalURL()
    print(f"Signed in to {active_portal}")
    # ArcInfo must be available for arcpy.mp.CreateWebLayerSDDraft and other processes to run
    print("ArcPro ArcInfo License Availability...")
    license_avail_arcinfo = arcpy.CheckProduct('arcinfo')
    if license_avail_arcinfo == "Available":
        print(f"Required license available.\n"
              f"\tarcpy.CheckProduct('arcinfo') returned {license_avail_arcinfo}\n"
              )
    else:
        install_info = arcpy.GetInstallInfo(product=None)
        product_info = arcpy.ProductInfo()
        print(f"Required license not available.\n"
              f"NOTE: Could be that ArcPro sign-in session has expired. Login to server using Visual Cron account that"
              f"triggers the script, open ArcPro, and sign into ArcGIS Online from within ArcPro."
              f"arcpy.CheckProduct('arcinfo') returned {license_avail_arcinfo}\n"
              f"arcpy.GetInstallInfo() returned {install_info}\n"
              f"arcpy.ProductInfo() returned {product_info}"
              )
        exit()

    arcpy.env.workspace = myvars.GDB_PATH_ARCPRO_PROJECT.value

    # Need the fc field names
    fc_fields = arcpy.ListFields(myvars.FC_NAME.value)
    fc_field_names_list = [field.name.strip() for field in fc_fields]

    # Need to reverse the header mapping between gis data and csv data. Originally created opposite to end need, meh.
    fc_field_names_to_csv_headers_dict = mdcls.Util_Class.reverse_dictionary(
        myvars.csv_headers_to_agol_fc_field_names_dict)

    # Need to excludes spatial fields like ObjectID and Shape. Isolate the fc fields, whose field names have a
    #   corresponding header in the csv file.
    fc_field_names_matching_header_list = [name for name in fc_field_names_list if
                                           name in fc_field_names_to_csv_headers_dict.keys()]

    # Need index position of matching, but after isolating non-spatial fields need new index positions.
    #   Build dictionary of header keys with their 'new' index position values
    fc_field_names_matching_csv_header__index_dictionary = mdcls.Util_Class.reverse_dictionary(
        dict(enumerate(fc_field_names_matching_header_list)))

    # Need to step through every feature class row and update the data with data from csv.
    with arcpy.da.UpdateCursor(in_table=myvars.FC_NAME.value, field_names=fc_field_names_matching_header_list) as update_cursor:
        for row in update_cursor:

            # Use header index dictionary to supply index position of row_id in modified pull (no spatial fields)
            current_row_id = row[fc_field_names_matching_csv_header__index_dictionary["Row_ID"]]

            # Use current row_id to get the correct record of data from the updated csv data in the in-memory database
            csv_data_for_current_row_id = full_data_dictionary_from_csv_data[current_row_id]

            # use the new record to replace the existing fc row
            update_cursor.updateRow(csv_data_for_current_row_id)


    #___________________________________________
    # PART 3 - Need to overwrite arcgis online hosted feature layer using republishing from ArcPro project.
    # Taken from example on ArcGIS for Python API. See resources below.
    # https://www.esri.com/arcgis-blog/products/api-python/analytics/updating-your-hosted-feature-services-with-arcgis-pro-and-the-arcgis-api-for-python/
    #___________________________________________

    from arcgis.gis import GIS          # Delayed import for performance

    # Need a new SDDraft and to stage it to SD
    arcpy.env.overwriteOutput = True
    arcpro_project = arcpy.mp.ArcGISProject(aprx_path=myvars.ARCPRO_PROJECT_PATH.value)

    # Note: keep pro project simple, have only one map in aprx. Process grabs first map.
    arcpro_map = arcpro_project.listMaps()[0]
    try:
        arcpy.mp.CreateWebLayerSDDraft(map_or_layers=arcpro_map,
                                       out_sddraft=sd_draft_filename,
                                       service_name=myvars.SD_FEATURE_SERVICE_NAME.value,
                                       server_type="MY_HOSTED_SERVICES",
                                       service_type="FEATURE_ACCESS",
                                       folder_name="MD Elect",
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
    except RuntimeError as rte:
        print(f"{rte}")
        exit()
    else:
        arcpy.StageService_server(in_service_definition_draft=sd_draft_filename,
                                  out_service_definition=sd_filename)

    # Need connection with AGOL
    gis = GIS(url=myvars.ARCGIS_ONLINE_PORTAL.value,
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
        ##        See https://community.esri.com/thread/166663
        ##        agol_sd_item = gis.content.search(query="{} AND owner:{}".format(SD_FEATURE_SERVICE_NAME.value, agol_username),
        ##                                          item_type="Service Definition")[0]
        # agol_sd_item = gis.content.search(query="{} AND owner:{}".format(myvars.SD_FEATURE_SERVICE_NAME.value, agol_username),
        #                                   item_type="Service Definition")[0]

        agol_sd_items = gis.content.search(query="title:{} AND owner:{}".format(myvars.SD_FEATURE_SERVICE_NAME.value,
                                                                                agol_username),
                                           item_type="Service Definition")
        print(agol_sd_items)
        if len(agol_sd_items) > 1:
            important_message_on_searching = """The query results for {} returned more than one layer (len = {}). We discovered
             that if you don't specify 'title:' in the query then searching for Elected_Officials returns both 
             ElectedOfficials and Elected_Officials. The blog example had us taking the zeroith index and 
             the value in zero was ElectedOfficals, which was incorrect.""".format(myvars.SD_FEATURE_SERVICE_NAME.value,
                                                                                   len(agol_sd_items))
            print(important_message_on_searching)
            raise Exception
        agol_sd_item = agol_sd_items[0]

        # I checked the agol_sd_item.id against the id in arcgis online and they match.
        print("FoundSD: {}, ID: {} Uploading and overwriting…".format(agol_sd_item.title, agol_sd_item.id))

        # After encountered error when deployed to server, found https://community.esri.com/thread/166663
        # agol_sd_item = gis.content.search(query="title:" + SD_FEATURE_SERVICE_NAME.value + " AND owner: " + agol_username,
        #                                   item_type="Service Definition")[0]
    except:
        print(
            "Search for .sd file not successful. Check that .sd file is present and named identically, and that the account credentials supplied are for the owner of the .sd file.")
        exit()

    try:
        print("Updating existing service definition file using {}".format(sd_filename))
        agol_sd_item.update(data=sd_filename)
    except Exception as e:
        print(e)
        exit()

    try:
        print("Overwriting existing feature service using {}".format(sd_filename))
        feature_service = agol_sd_item.publish(overwrite=True)
    except Exception as e:
        print(e)
        exit()

    if share_organization or share_everyone or share_groups:
        feature_service.share(org=share_organization, everyone=share_everyone, groups=share_groups)

    print("Process Complete")


if __name__ == "__main__":
    main()
