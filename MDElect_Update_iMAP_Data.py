"""
TODO

"""
# access the csv files generated from the google spreadsheet with tabs of US and Maryland election data
# read the data
# access the sde feature class to be updated
# establish an update cursor
# map the google spreadsheet headers to the sde feature class headers
# update the data in the feature class
# TODO: It appears that it is more efficient and clear to modify the MDElect_UpdateProcess and the MDElect_Update_iMap_Data process to be classed base and share functions and functionality as they perform similar processes

def main():
    import os

    import MDElect_Classes as mycls
    import MDElect_Variables as myvars

    # FUNCTIONALITY
    # make sure the required files are real/available
    assert(os.path.exists(myvars.CSV_PATH_MDGOV.value))
    assert(os.path.exists(myvars.CSV_PATH_USGOV.value))
    assert(os.path.exists(myvars.SDE_CONNECTION_FILE.value))

    # make data in csv files available in a memory efficient way
    # create data object appropriate to datasource (md/us) and store in list
    # md object creation

    md_data_objects = mycls.Util_Class.process_csv_data_to_objects(csv_path=myvars.CSV_PATH_MDGOV.value, object_type=mycls.MD_Data_Class)
    us_data_objects = mycls.Util_Class.process_csv_data_to_objects(csv_path=myvars.CSV_PATH_USGOV.value, object_type=mycls.US_Data_Class)
    md_district_ID_to_data_object_dict = {object.district : object for object in md_data_objects}
    us_district_ID_to_data_object_dict = {object.district : object for object in us_data_objects}

    import arcpy    # Delayed Import

    # Set overwrite output geo-processing parameter
    arcpy.env.overwriteOutput = True

    # Set GIS workspace
    districts_fd = mycls.Util_Class.clean_url_slashes(os.path.join(myvars._ROOT_PROJECT_PATH.value, myvars.SDE_CONNECTION_FILE.value, myvars.FEATURE_DATASET_NAME_SDE.value))
    arcpy.env.workspace = districts_fd

    md_fields_obj = arcpy.ListFields(dataset=myvars.MD_DISTRICTS_SDE_FC_NAME.value)
    us_fields_obj = arcpy.ListFields(dataset=myvars.US_DISTRICTS_SDE_FC_NAME.value)
    md_sde_fc_field_names = [(field.name).strip() for field in md_fields_obj if "Shape" not in field.name and "ID" not in field.name]
    us_sde_fc_field_names = [(field.name).strip() for field in us_fields_obj if "Shape" not in field.name and "ID" not in field.name]
    md_current_district_index = md_sde_fc_field_names.index("DISTRICT")
    us_current_district_index = us_sde_fc_field_names.index("DISTRICT")

    # md_fc_field_names_to_csv_headers_dict = mycls.Util_Class.reverse_dictionary(myvars.md_csv_headers_to_sde_fc_field_names_dict)
    # us_fc_field_names_to_csv_headers_dict = mycls.Util_Class.reverse_dictionary(myvars.us_csv_headers_to_sde_fc_field_names_dict)
    exit()
    # establish cursor on each feature class
    mycls.Util_Class.update_sde_feature_class(in_table=myvars.MD_DISTRICTS_SDE_FC_NAME.value,
                             field_names=myvars.md_sde_fc_districts_field_list,
                             current_district_index=md_current_district_index,
                             district_info_dict=md_district_ID_to_data_object_dict)
    mycls.Util_Class.update_sde_feature_class(in_table=myvars.US_DISTRICTS_SDE_FC_NAME.value,
                             field_names=myvars.us_sde_fc_districts_field_list,
                             current_district_index=us_current_district_index,
                             district_info_dict=us_district_ID_to_data_object_dict)

#TODO: check the above process updates the feature class in the correct field order, and run on staging.

if __name__ == "__main__":
    main()
