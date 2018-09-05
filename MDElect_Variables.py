"""
Centralized location for all variables used in the two steps of the Update Process.
Step 1 updates the hosted feature layer in ArcGIS Online. Step 2 updates the SDE stored election boundaries feature
classes. Variables are alphabetized within two categories, Constants and Other. The design is intended to ease editing
by non-developers, if necessary.
Date: 20180815
Author: CJuice
"""

from collections import namedtuple
import os


def go_up_one_directory_level():
    """Changes the current working directory to one higher than the location of this script.
    The folder structure in the server has the csv file sitting one directory up from the project python scripts.
    Rather than hard coding the path of the directory the script just steps up one from the location of the python
    files to see the csv's. Function created here instead of MDElect_Classes to avoid importing entire module, which
    is imported by script that imports this script."""
    os.chdir("..")
    return os.path.abspath(os.curdir)


CONSTANT = namedtuple("CONSTANT", ["value"])

# VARIABLES - CONSTANTS
_ROOT_PROJECT_PATH = CONSTANT(value=os.path.dirname(__file__))
ARCGIS_ONLINE_PORTAL = CONSTANT(value="https://maryland.maps.arcgis.com")
ARCPRO_PROJECT_PATH = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\ElectedOfficals\ElectedOfficals.aprx"))
CREDENTIALS_PATH = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\credentials.cfg"))
# CSV_DIRECTORY_PATH_IN_PRODUCTION = CONSTANT(value=go_up_one_directory_level())                                      # PRODUCTION
CSV_PATH_BRIDGE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180613_Bridge.csv"))
CSV_PATH_MDGOV = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180815_MarylandGovernment.csv"))    # TESTING
# CSV_PATH_MDGOV = CONSTANT(value=os.path.join(CSV_DIRECTORY_PATH_IN_PRODUCTION.value, r"MarylandGovernment.csv"))    # PRODUCTION
CSV_PATH_USGOV = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180815_USGovernment.csv"))          # TESTING
# CSV_PATH_USGOV = CONSTANT(value=os.path.join(CSV_DIRECTORY_PATH_IN_PRODUCTION.value, r"USGovernment.csv"))          # PRODUCTION
FC_NAME = CONSTANT(value="ElectedOfficials")
FEATURE_DATASET_NAME_SDE = CONSTANT(value="Staging.SDE.Boundaries_MD_ElectionBoundaries")           # STAGING
# FEATURE_DATASET_NAME_SDE = CONSTANT(value="Production.SDE.Boundaries_MD_ElectionBoundaries")        # PRODUCTION
GDB_PATH_ARCPRO_PROJECT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\ElectedOfficals\ElectedOfficals.gdb"))
MD_DISTRICTS_SDE_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_LegislativeDistricts2012_MDP")          # STAGING
# MD_DISTRICTS_SDE_FC_NAME = CONSTANT(value="Production.SDE.BNDY_LegislativeDistricts2011_MDP")       # PRODUCTION
SD_FEATURE_SERVICE_NAME = CONSTANT(value="Elected_Officials")   # SD = Service Definition
SD_FILE_STORAGE_LOCATION = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\sd_file_storage"))
SD_FILENAME = CONSTANT(value="Elected_Officals.sd")
SD_FILENAME_DRAFT = CONSTANT(value="Elected_Officals.sddraft")
SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Staging on gis-db-imap01p.sde"))                                     # STAGING
# SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Production as sde on gis-ags-imap01p.mdgov.maryland.gov.sde"))       # PRODUCTION
SQL_CREATE_BRIDGE = CONSTANT(value="""CREATE TABLE BRIDGE (Row_ID text primary key, MD_District text, US_District text)""")
SQL_CREATE_MDGOV = CONSTANT(value="""CREATE TABLE MDGOV (
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
SQL_CREATE_USGOV = CONSTANT(value="""CREATE TABLE USGOV (
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
SQL_INSERT_BRIDGE = CONSTANT(value="""INSERT OR IGNORE INTO bridge VALUES (:row_id, :md_district, :us_district)""")
SQL_INSERT_MDGOV = CONSTANT(value="""INSERT OR IGNORE INTO mdgov VALUES (:md_district, :state_senator, :state_senator_party, :state_senator_maryland_manual_online, :state_representative_1, :state_representative_1_party, :state_representative_1_maryland_manual_online, :state_representative_2, :state_representative_2_party, :state_representative_2_maryland_manual_online, :state_representative_3,  :state_representative_3_party, :state_representative_3_maryland_manual_online, :governor, :governor_maryland_manual_online, :lt_governor, :lt_governor_maryland_manual_online, :attorney_general, :attorney_general_maryland_manual_online, :comptroller, :comptroller_maryland_manual_online)""")
SQL_INSERT_USGOV = CONSTANT(value="""INSERT OR IGNORE INTO usgov VALUES (:us_district, :name, :label, :us_senator_1, :us_senator_1_party, :us_senator_1_maryland_manual_online, :us_senator_2, :us_senator_2_party, :us_senator_2_maryland_manual_online, :us_representatives, :us_representatives_party, :us_representatives_maryland_manual_online)""")
SQL_SELECT_OUTPUT_DATA = CONSTANT(value="""SELECT MDGOV.*, USGOV.*, BRIDGE.Row_ID FROM MDGOV, BRIDGE, USGOV WHERE MDGOV.MD_District = BRIDGE.MD_District AND BRIDGE.US_District = USGOV.US_District""")
US_DISTRICTS_SDE_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_CongressionalDistricts2011_MDP")            # STAGING
# US_DISTRICTS_SDE_FC_NAME = CONSTANT(value="Production.SDE.BNDY_CongressionalDistricts2012_MDP")         # PRODUCTION



# VARIABLES - OTHER
csv_headers_to_agol_fc_field_names_dict = {"MD_District": "MD_District",
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

md_sde_fc_districts_field_list = ['DISTRICT', 'State_Senator', 'State_Representative_1', 'State_Representative_2',
                                  'State_Representative_3', 'State_Senator_Party', 'State_Representative_1_Party',
                                  'State_Representative_2_Party',
                                  'State_Representative_3_Party', 'StateSenator_MDManualURL',
                                  'StateRepresentative1MDManualURL',
                                  'StateRepresentative2MDManualURL', 'StateRepresentative3MDManualURL']

us_sde_fc_districts_field_list = ['DISTRICT', 'NAME', 'Label', 'US_Representatives', 'US_Representatives_Party',
                                  'US_Senator_1', 'US_Senator_1_Party', 'US_Senator_2', 'US_Senator_2_Party',
                                  'US_Senator_1_MDManualURL', 'US_Senator_2_MDManualURL',
                                  'US_Representatives_MDManualURL']

# _____________________________________________________________________________________________________________
# Preserving for reference. Used the csv headers when building.
# md_csv_headers_to_sde_fc_field_names_dict = {'MD_District': "DISTRICT",
#                                              'State_Representative_1': "State_Representative_1",
#                                              'State_Representative_1_Maryland_Manual_Online': "StateRepresentative1MDManualURL",
#                                              'State_Representative_1_Party': "State_Representative_1_Party",
#                                              'State_Representative_2': "State_Representative_2",
#                                              'State_Representative_2_Maryland_Manual_Online': "StateRepresentative2MDManualURL",
#                                              'State_Representative_2_Party': "State_Representative_2_Party",
#                                              'State_Representative_3': "State_Representative_3",
#                                              'State_Representative_3_Maryland_Manual_Online': "StateRepresentative3MDManualURL",
#                                              'State_Representative_3_Party': "State_Representative_3_Party",
#                                              'State_Senator': "State_Senator",
#                                              'State_Senator_Maryland_Manual_Online': "StateSenator_MDManualURL",
#                                              'State_Senator_Party': "State_Senator_Party"}

# us_csv_headers_to_sde_fc_field_names_dict = {'Label': "Label",
#                                              'Name': "NAME",
#                                              'US_District': "DISTRICT",
#                                              'US_Representatives': "US_Representatives",
#                                              'US_Representatives_Maryland_Manual_Online': "US_Representatives_MDManualURL",
#                                              'US_Representatives_Party': "US_Representatives_Party",
#                                              'US_Senator_1': "US_Senator_1",
#                                              'US_Senator_1_Maryland_Manual_Online': "US_Senator_1_MDManualURL",
#                                              'US_Senator_1_Party': "US_Senator_1_Party",
#                                              'US_Senator_2': "US_Senator_2",
#                                              'US_Senator_2_Maryland_Manual_Online': "US_Senator_2_MDManualURL",
#                                              'US_Senator_2_Party': "US_Senator_2_Party"}

# Mapping between the csv field headers and the sde feature class headers
# These do not exist in the fc but do exist in the csv. Could be needed at a later date.
# 'Attorney_General': "",
# 'Attorney_General_Maryland_Manual_Online': "",
# 'Comptroller': "",
# 'Comptroller_Maryland_Manual_Online': "",
# 'Governor': "",
# 'Governor_Maryland_Manual_Online': "",
# 'Lt_Governor': "",
# 'Lt_Governor_Maryland_Manual_Online': "",
# _____________________________________________________________________________________________________________
