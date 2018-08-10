# access the csv files generated from the google spreadsheet with tabs of US and Maryland election data
# read the data
# access the sde feature class to be updated
# establish an update cursor
# map the google spreadsheet headers to the sde feature class headers
# update the data in the feature class

def main():
    import os
    from collections import namedtuple
    import arcpy

    # VARIABLES
    CONSTANT = namedtuple("CONSTANT", ["value"])
    _ROOT_PROJECT_PATH = CONSTANT(value=os.path.dirname(__file__))
    CSV_MARYLAND_GOVERNMENT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180719_MarylandGovernment.csv"))
    CSV_US_GOVERNMENT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180719_USGovernment.csv"))
    SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Staging on gis-db-imap01p.sde"))  # STAGING
    FEATURE_DATASET_NAME = CONSTANT(value="Staging.SDE.Boundaries_MD_ElectionBoundaries")   # STAGING
    MD_DISTRICTS_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_LegislativeDistricts2012_MDP")  # STAGING
    US_DISTRICTS_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_CongressionalDistricts2011_MDP")  # STAGING
    # SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Production as sde on gis-ags-imap01p.mdgov.maryland.gov.sde"))  # PRODUCTION
    # FEATURE_DATASET_NAME = CONSTANT(value="Production.SDE.Boundaries_MD_ElectionBoundaries")   # PRODUCTION
    # MD_DISTRICTS_FC_NAME = CONSTANT(value="Production.SDE.BNDY_LegislativeDistricts2011_MDP")  # PRODUCTION
    # US_DISTRICTS_FC_NAME = CONSTANT(value="Production.SDE.BNDY_CongressionalDistricts2012_MDP")  # PRODUCTION

    md_districts_field_list = ['OBJECTID', 'DISTRICT', 'State_Senator', 'State_Representative_1', 'State_Representative_2',
     'State_Representative_3', 'State_Senator_Party', 'State_Representative_1_Party', 'State_Representative_2_Party',
     'State_Representative_3_Party', 'StateSenator_MDManualURL', 'StateRepresentative1MDManualURL',
     'StateRepresentative2MDManualURL', 'StateRepresentative3MDManualURL']
    us_districts_field_list = ['OBJECTID', 'ID', 'DISTRICT', 'NAME', 'Label', 'US_Representatives', 'US_Representatives_Party', 'US_Senator_1',
     'US_Senator_1_Party', 'US_Senator_2', 'US_Senator_2_Party', 'US_Senator_1_MDManualURL', 'US_Senator_2_MDManualURL',
     'US_Representatives_MDManualURL']

    # Original Field List. Delete later when sure don't need
    # md_districts_field_list = ['OBJECTID', 'DISTRICT', 'State_Senator', 'State_Representative_1', 'State_Representative_2',
    #  'State_Representative_3', 'State_Senator_Party', 'State_Representative_1_Party', 'State_Representative_2_Party',
    #  'State_Representative_3_Party', 'StateSenator_MDManualURL', 'StateRepresentative1MDManualURL',
    #  'StateRepresentative2MDManualURL', 'StateRepresentative3MDManualURL', 'Shape', 'Shape.STArea()',
    #  'Shape.STLength()']
    # us_districts_field_list = ['OBJECTID', 'ID', 'DISTRICT', 'NAME', 'Label', 'US_Representatives', 'US_Representatives_Party', 'US_Senator_1',
    #  'US_Senator_1_Party', 'US_Senator_2', 'US_Senator_2_Party', 'US_Senator_1_MDManualURL', 'US_Senator_2_MDManualURL',
    #  'US_Representatives_MDManualURL', 'Shape', 'Shape.STArea()', 'Shape.STLength()']

    # FUNCTIONS
    def create_file_generator(file_path):
        with open(file_path, 'r') as handler:
            for line in handler:
                line = line.strip()
                yield line
    def clean_url_slashes(url):
        return url.replace("\\", "/")

    # FUNCTIONALITY
        # make sure the required files are real/available
    assert(os.path.exists(CSV_MARYLAND_GOVERNMENT.value))
    assert(os.path.exists(CSV_US_GOVERNMENT.value))
    assert(os.path.exists(SDE_CONNECTION_FILE.value))

        # make data in csv files available in a memory efficient way
    line_generator_Maryland = create_file_generator(CSV_MARYLAND_GOVERNMENT.value)
    line_generator_US = create_file_generator(CSV_MARYLAND_GOVERNMENT.value)

    # for line in line_generator_Maryland:
    #     print(line)
    #
    # for line in line_generator_US:
    #     print(line)

    # Set overwrite output geo-processing parameter
    arcpy.env.overwriteOutput = True

    # Set GIS workspace
    districts_fd = clean_url_slashes(os.path.join(_ROOT_PROJECT_PATH.value, SDE_CONNECTION_FILE.value, FEATURE_DATASET_NAME.value))
    arcpy.env.workspace = districts_fd
    # for fc in arcpy.ListFeatureClasses():
    #     fields = arcpy.ListFields(fc)
    #     print([field.name for field in fields])
        # for field in fields:
        #     print(field.name)

    # establish cursor on each feature class
    with arcpy.da.UpdateCursor(in_table=MD_DISTRICTS_FC_NAME.value, field_names=md_districts_field_list) as cursor:
        for row in cursor:
            print(row)


if __name__ == "__main__":
    main()
