"""
Update the MD and US Election Boundaries SDE stored feature class attributes with data in CSVs created from MD State
Archives Google Sheet.

Process looks for CSV files of data on elected officials. The Maryland State Archive agency maintains the elected
officials data using two tabs in a Google Sheet. We have a process that pulls CSVs of the tabs. One tab is MD specific,
the other is Federal specific. Access the CSVs and use the data to update the SDE stored election boundaries
feature classes. The attributes are the only aspect updated. The geometries are not edited by this process. There are
two feature classes to be updated. These are the Maryland and the US Government election boundaries.
Date: 20180815
Author: CJuice
Revisions:
"""


def main():
    import MDElect_Classes as mycls
    import MDElect_Variables as myvars
    import os

    # FUNCTIONALITY
    #   make sure the required files are real/available
    assert(os.path.exists(myvars.CSV_PATH_MDGOV.value))
    assert(os.path.exists(myvars.CSV_PATH_USGOV.value))
    assert(os.path.exists(myvars.SDE_CONNECTION_FILE.value))

    #   access the csv files generated from the google spreadsheet with tabs of US and Maryland election data
    #   make data in csv files available in a memory efficient way
    #   create data object appropriate to data source (md/us) and store in list
    md_data_objects = mycls.Util_Class.process_csv_data_to_objects(csv_path=myvars.CSV_PATH_MDGOV.value,
                                                                   object_type=mycls.MD_Data_Class)
    us_data_objects = mycls.Util_Class.process_csv_data_to_objects(csv_path=myvars.CSV_PATH_USGOV.value,
                                                                   object_type=mycls.US_Data_Class)
    md_district_ID_to_data_object_dict = {data_object.district: data_object for data_object in md_data_objects}
    us_district_ID_to_data_object_dict = {data_object.district: data_object for data_object in us_data_objects}

    import arcpy    # Delayed Import

    #   Set overwrite output geo-processing parameter
    arcpy.env.overwriteOutput = True

    #   Set GIS workspace
    districts_fd = mycls.Util_Class.clean_url_slashes(url=os.path.join(myvars._ROOT_PROJECT_PATH.value,
                                                                       myvars.SDE_CONNECTION_FILE.value,
                                                                       myvars.FEATURE_DATASET_NAME_SDE.value))
    arcpy.env.workspace = districts_fd

    #   access the sde feature class to be updated and inventory field names. Store names for use after stripping
    #       the unnecessary fields. Get the index position of the District attribute for later use
    md_fields_obj = arcpy.ListFields(dataset=myvars.MD_DISTRICTS_SDE_FC_NAME.value)
    us_fields_obj = arcpy.ListFields(dataset=myvars.US_DISTRICTS_SDE_FC_NAME.value)
    md_sde_fc_field_names = [field.name.strip() for field in md_fields_obj if "Shape" not in field.name and "ID" not in field.name]
    us_sde_fc_field_names = [field.name.strip() for field in us_fields_obj if "Shape" not in field.name and "ID" not in field.name]
    md_current_district_index = md_sde_fc_field_names.index("DISTRICT")
    us_current_district_index = us_sde_fc_field_names.index("DISTRICT")

    #   update each feature class
    mycls.Util_Class.update_sde_feature_class(in_table=myvars.MD_DISTRICTS_SDE_FC_NAME.value,
                                              field_names=myvars.md_sde_fc_districts_field_list,
                                              current_district_index=md_current_district_index,
                                              district_info_dict=md_district_ID_to_data_object_dict)
    mycls.Util_Class.update_sde_feature_class(in_table=myvars.US_DISTRICTS_SDE_FC_NAME.value,
                                              field_names=myvars.us_sde_fc_districts_field_list,
                                              current_district_index=us_current_district_index,
                                              district_info_dict=us_district_ID_to_data_object_dict)


if __name__ == "__main__":
    main()
