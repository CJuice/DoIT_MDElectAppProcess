from collections import namedtuple
import os

CONSTANT = namedtuple("CONSTANT", ["value"])

# VARIABLES - CONSTANTS
_ROOT_PROJECT_PATH = CONSTANT(value=os.path.dirname(__file__))
ARCGIS_ONLINE_PORTAL = CONSTANT(value="https://maryland.maps.arcgis.com")
ARCPRO_PROJECT_PATH = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\ElectedOfficals\ElectedOfficals.aprx"))
CREDENTIALS_PATH = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\credentials.cfg"))
CSV_PATH_BRIDGE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180613_Bridge.csv"))
CSV_PATH_MDGOV = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180619_ElectedOfficialsMarylandGovernment.csv"))
CSV_PATH_USGOV = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180619_ElectedOfficialsUSGovernment.csv"))
FC_NAME = CONSTANT(value="ElectedOfficials")
GDB_PATH_ARCPRO_PROJECT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\ElectedOfficals\ElectedOfficals.gdb"))
SD_FEATURE_SERVICE_NAME = CONSTANT(value="Elected_Officials")
SD_FILE_STORAGE_LOCATION = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\sd_file_storage"))
SD_FILENAME_DRAFT = CONSTANT(value="Elected_Officals.sddraft")
SD_FILENAME = CONSTANT(value="Elected_Officals.sd")
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

#_____________________________________________________________________________________________________________
# Update Process Vars
CSV_MARYLAND_GOVERNMENT = CONSTANT(
    value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180719_MarylandGovernment.csv"))
CSV_US_GOVERNMENT = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\20180719_USGovernment.csv"))
SDE_CONNECTION_FILE = CONSTANT(
    value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Staging on gis-db-imap01p.sde"))  # STAGING
FEATURE_DATASET_NAME = CONSTANT(value="Staging.SDE.Boundaries_MD_ElectionBoundaries")  # STAGING
MD_DISTRICTS_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_LegislativeDistricts2012_MDP")  # STAGING
US_DISTRICTS_FC_NAME = CONSTANT(value="Staging.SDE.BNDY_CongressionalDistricts2011_MDP")  # STAGING
# SDE_CONNECTION_FILE = CONSTANT(value=os.path.join(_ROOT_PROJECT_PATH.value, r"Docs\SDE_CONNECTION_FILE\Production as sde on gis-ags-imap01p.mdgov.maryland.gov.sde"))  # PRODUCTION
# FEATURE_DATASET_NAME = CONSTANT(value="Production.SDE.Boundaries_MD_ElectionBoundaries")   # PRODUCTION
# MD_DISTRICTS_FC_NAME = CONSTANT(value="Production.SDE.BNDY_LegislativeDistricts2011_MDP")  # PRODUCTION
# US_DISTRICTS_FC_NAME = CONSTANT(value="Production.SDE.BNDY_CongressionalDistricts2012_MDP")  # PRODUCTION

md_fc_districts_field_list = ['DISTRICT', 'State_Senator', 'State_Representative_1', 'State_Representative_2',
                              'State_Representative_3', 'State_Senator_Party', 'State_Representative_1_Party',
                              'State_Representative_2_Party',
                              'State_Representative_3_Party', 'StateSenator_MDManualURL',
                              'StateRepresentative1MDManualURL',
                              'StateRepresentative2MDManualURL', 'StateRepresentative3MDManualURL']
us_fc_districts_field_list = ['ID', 'DISTRICT', 'NAME', 'Label', 'US_Representatives', 'US_Representatives_Party',
                              'US_Senator_1',
                              'US_Senator_1_Party', 'US_Senator_2', 'US_Senator_2_Party', 'US_Senator_1_MDManualURL',
                              'US_Senator_2_MDManualURL',
                              'US_Representatives_MDManualURL']

# Mapping between the csv field headers and the sde feature class headers
# These do not exist in the fc but do exist in the csv
# 'Attorney_General': "",
# 'Attorney_General_Maryland_Manual_Online': "",
# 'Comptroller': "",
# 'Comptroller_Maryland_Manual_Online': "",
# 'Governor': "",
# 'Governor_Maryland_Manual_Online': "",
# 'Lt_Governor': "",
# 'Lt_Governor_Maryland_Manual_Online': "",
md_csv_headers_to_fc_field_names_dict = {'MD_District': "DISTRICT",
                                         'State_Representative_1': "State_Representative_1",
                                         'State_Representative_1_Maryland_Manual_Online': "StateRepresentative1MDManualURL",
                                         'State_Representative_1_Party': "State_Representative_1_Party",
                                         'State_Representative_2': "State_Representative_2",
                                         'State_Representative_2_Maryland_Manual_Online': "StateRepresentative2MDManualURL",
                                         'State_Representative_2_Party': "State_Representative_2_Party",
                                         'State_Representative_3': "State_Representative_3",
                                         'State_Representative_3_Maryland_Manual_Online': "StateRepresentative3MDManualURL",
                                         'State_Representative_3_Party': "State_Representative_3_Party",
                                         'State_Senator': "State_Senator",
                                         'State_Senator_Maryland_Manual_Online': "StateSenator_MDManualURL",
                                         'State_Senator_Party': "State_Senator_Party"}
us_csv_headers_to_fc_field_names_dict = {'Label': "Label",
                                         'Name': "NAME",
                                         'US_District': "DISTRICT",
                                         'US_Representatives': "US_Representatives",
                                         'US_Representatives_Maryland_Manual_Online': "US_Representatives_MDManualURL",
                                         'US_Representatives_Party': "US_Representatives_Party",
                                         'US_Senator_1': "US_Senator_1",
                                         'US_Senator_1_Maryland_Manual_Online': "US_Senator_1_MDManualURL",
                                         'US_Senator_1_Party': "US_Senator_1_Party",
                                         'US_Senator_2': "US_Senator_2",
                                         'US_Senator_2_Maryland_Manual_Online': "US_Senator_2_MDManualURL",
                                         'US_Senator_2_Party': "US_Senator_2_Party"}
#_____________________________________________________________________________________________________________

# VARIABLES - OTHER
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