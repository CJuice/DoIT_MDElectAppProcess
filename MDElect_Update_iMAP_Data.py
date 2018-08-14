# access the csv files generated from the google spreadsheet with tabs of US and Maryland election data
# read the data
# access the sde feature class to be updated
# establish an update cursor
# map the google spreadsheet headers to the sde feature class headers
# update the data in the feature class
# TODO: It appears that it is more efficient and clear to modify the MDElect_UpdateProcess and the MDElect_Update_iMap_Data process to be classed base and share functions and functionality as they perform similar processes

def main():
    import os
    import arcpy
    import MDElect_Classes as mycls
    import MDElect_Variables as myvars
    # VARIABLES
    # CONSTANT = namedtuple("CONSTANT", ["value"])
    # _ROOT_PROJECT_PATH = CONSTANT(value=os.path.dirname(__file__))
    # CSV_MARYLAND_GOVERNMENT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180719_MarylandGovernment.csv"))
    # CSV_US_GOVERNMENT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180719_USGovernment.csv"))
    # SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Staging on gis-db-imap01p.sde"))  # STAGING
    # FEATURE_DATASET_NAME = CONSTANT(value="Staging.SDE.Boundaries_MD_ElectionBoundaries")   # STAGING
    # MD_DISTRICTS_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_LegislativeDistricts2012_MDP")  # STAGING
    # US_DISTRICTS_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_CongressionalDistricts2011_MDP")  # STAGING
    # # SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Production as sde on gis-ags-imap01p.mdgov.maryland.gov.sde"))  # PRODUCTION
    # # FEATURE_DATASET_NAME = CONSTANT(value="Production.SDE.Boundaries_MD_ElectionBoundaries")   # PRODUCTION
    # # MD_DISTRICTS_FC_NAME = CONSTANT(value="Production.SDE.BNDY_LegislativeDistricts2011_MDP")  # PRODUCTION
    # # US_DISTRICTS_FC_NAME = CONSTANT(value="Production.SDE.BNDY_CongressionalDistricts2012_MDP")  # PRODUCTION
    #
    # md_fc_districts_field_list = ['DISTRICT', 'State_Senator', 'State_Representative_1', 'State_Representative_2',
    #  'State_Representative_3', 'State_Senator_Party', 'State_Representative_1_Party', 'State_Representative_2_Party',
    #  'State_Representative_3_Party', 'StateSenator_MDManualURL', 'StateRepresentative1MDManualURL',
    #  'StateRepresentative2MDManualURL', 'StateRepresentative3MDManualURL']
    # us_fc_districts_field_list = ['ID', 'DISTRICT', 'NAME', 'Label', 'US_Representatives', 'US_Representatives_Party', 'US_Senator_1',
    #  'US_Senator_1_Party', 'US_Senator_2', 'US_Senator_2_Party', 'US_Senator_1_MDManualURL', 'US_Senator_2_MDManualURL',
    #  'US_Representatives_MDManualURL']


    # Original feature class field list. Delete later when sure don't need
    # md_fc_districts_field_list = ['OBJECTID', 'DISTRICT', 'State_Senator', 'State_Representative_1', 'State_Representative_2',
    #  'State_Representative_3', 'State_Senator_Party', 'State_Representative_1_Party', 'State_Representative_2_Party',
    #  'State_Representative_3_Party', 'StateSenator_MDManualURL', 'StateRepresentative1MDManualURL',
    #  'StateRepresentative2MDManualURL', 'StateRepresentative3MDManualURL', 'Shape', 'Shape.STArea()',
    #  'Shape.STLength()']
    # us_fc_districts_field_list = ['OBJECTID', 'ID', 'DISTRICT', 'NAME', 'Label', 'US_Representatives', 'US_Representatives_Party', 'US_Senator_1',
    #  'US_Senator_1_Party', 'US_Senator_2', 'US_Senator_2_Party', 'US_Senator_1_MDManualURL', 'US_Senator_2_MDManualURL',
    #  'US_Representatives_MDManualURL', 'Shape', 'Shape.STArea()', 'Shape.STLength()']

    # # Mapping between the csv field headers and the sde feature class headers
    # # These do not exist in the fc but do exist in the csv
    # # 'Attorney_General': "",
    # # 'Attorney_General_Maryland_Manual_Online': "",
    # # 'Comptroller': "",
    # # 'Comptroller_Maryland_Manual_Online': "",
    # # 'Governor': "",
    # # 'Governor_Maryland_Manual_Online': "",
    # # 'Lt_Governor': "",
    # # 'Lt_Governor_Maryland_Manual_Online': "",
    # md_csv_headers_to_fc_field_names_dict = {'MD_District': "DISTRICT",
    #                                          'State_Representative_1': "State_Representative_1",
    #                                          'State_Representative_1_Maryland_Manual_Online': "StateRepresentative1MDManualURL",
    #                                          'State_Representative_1_Party': "State_Representative_1_Party",
    #                                          'State_Representative_2': "State_Representative_2",
    #                                          'State_Representative_2_Maryland_Manual_Online': "StateRepresentative2MDManualURL",
    #                                          'State_Representative_2_Party': "State_Representative_2_Party",
    #                                          'State_Representative_3': "State_Representative_3",
    #                                          'State_Representative_3_Maryland_Manual_Online': "StateRepresentative3MDManualURL",
    #                                          'State_Representative_3_Party': "State_Representative_3_Party",
    #                                          'State_Senator': "State_Senator",
    #                                          'State_Senator_Maryland_Manual_Online': "StateSenator_MDManualURL",
    #                                          'State_Senator_Party': "State_Senator_Party"}
    # us_csv_headers_to_fc_field_names_dict = {'Label': "Label",
    #                                          'Name': "NAME",
    #                                          'US_District': "DISTRICT",
    #                                          'US_Representatives': "US_Representatives",
    #                                          'US_Representatives_Maryland_Manual_Online': "US_Representatives_MDManualURL",
    #                                          'US_Representatives_Party': "US_Representatives_Party",
    #                                          'US_Senator_1': "US_Senator_1",
    #                                          'US_Senator_1_Maryland_Manual_Online': "US_Senator_1_MDManualURL",
    #                                          'US_Senator_1_Party': "US_Senator_1_Party",
    #                                          'US_Senator_2': "US_Senator_2",
    #                                          'US_Senator_2_Maryland_Manual_Online': "US_Senator_2_MDManualURL",
    #                                          'US_Senator_2_Party': "US_Senator_2_Party"}


    # FUNCTIONS
    # def create_file_generator(file_path):
    #     with open(file_path, 'r') as handler:
    #         for line in handler:
    #             line = line.strip()
    #             yield line
    # def clean_url_slashes(url):
    #     return url.replace("\\", "/")
    # def reverse_dictionary(dictionary):
    #     """
    #     Swap the key and value, creating new dictionary, and return new dictionary.
    #     :param dictionary: in dictionary to be reversed
    #     :return: reversed dictionary
    #     """
    #     return {value: key for key, value in dictionary.items()}

    # FUNCTIONALITY
        # make sure the required files are real/available
    assert(os.path.exists(myvars.CSV_MARYLAND_GOVERNMENT.value))
    assert(os.path.exists(myvars.CSV_US_GOVERNMENT.value))
    assert(os.path.exists(myvars.SDE_CONNECTION_FILE.value))

        # make data in csv files available in a memory efficient way
    line_generator_Maryland = mycls.Util_Class.create_file_generator(myvars.CSV_MARYLAND_GOVERNMENT.value)
    line_generator_US = mycls.Util_Class.create_file_generator(myvars.CSV_US_GOVERNMENT.value)

    for line in line_generator_Maryland:
        ls = line.split(",")
        break

    for line in line_generator_US:
        ls2 = line.split(",")
        break

    # Set overwrite output geo-processing parameter
    arcpy.env.overwriteOutput = True

    # Set GIS workspace
    districts_fd = mycls.Util_Class.clean_url_slashes(os.path.join(myvars._ROOT_PROJECT_PATH.value, myvars.SDE_CONNECTION_FILE.value, myvars.FEATURE_DATASET_NAME.value))
    arcpy.env.workspace = districts_fd

    md_fields_obj = arcpy.ListFields(dataset=myvars.MD_DISTRICTS_FC_NAME.value)
    us_fields_obj = arcpy.ListFields(dataset=myvars.US_DISTRICTS_FC_NAME.value)
    md_fc_field_names = [(field.name).strip() for field in md_fields_obj if "Shape" not in field.name and "ID" not in field.name]
    us_fc_field_names = [(field.name).strip() for field in us_fields_obj if "Shape" not in field.name and "ID" not in field.name]
    md_current_district_index = md_fc_field_names.index("DISTRICT")
    us_current_district_index = md_fc_field_names.index("DISTRICT")
    # md_fc_field_names.sort()
    # us_fc_field_names.sort()
    # print("MD")
    # print(mdheaders)
    # print(md_fc_field_names)
    # print("US")
    # print(usheaders)
    # print(us_fc_field_names)

    md_fc_field_names_to_csv_headers_dict = mycls.Util_Class.reverse_dictionary(myvars.md_csv_headers_to_fc_field_names_dict)
    us_fc_field_names_to_csv_headers_dict = mycls.Util_Class.reverse_dictionary(myvars.us_csv_headers_to_fc_field_names_dict)

    # establish cursor on each feature class
    with arcpy.da.UpdateCursor(in_table=myvars.MD_DISTRICTS_FC_NAME.value, field_names=myvars.md_fc_districts_field_list) as cursor:

        for row in cursor:
            current_district = row[md_current_district_index]
            print(current_district)
            print(row)


if __name__ == "__main__":
    main()
