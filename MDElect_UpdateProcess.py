"""
DRAFT - Combine data from two csv's and update data in AGOL hosted feature layer with the new combined dataset
"""

# IMPORTS
from collections import namedtuple
import sqlite3

# CLASSES
class Bridge_Class():
    def __init__(self, data_dict):
        self.row_id = data_dict["Row_ID"]
        self.mddistrict = data_dict["MDDistrict"]
        self.usdistrict = data_dict["USDistrict"]
class MDGov_Class():
    def __init__(self, data_dict):
        self.district = data_dict["District"]
        self.state_senator = data_dict["State_Senator"]
        self.state_representative_1 = data_dict["State_Representative_1"]
        self.state_representative_2 = data_dict["State_Representative_2"]
        self.state_representative_3 = data_dict["State_Representative_3"]
        self.state_senator_party = data_dict["State_Senator_Party"]
        self.state_representative_1_party = data_dict["State_Representative_1_Party"]
        self.state_representative_2_party = data_dict["State_Representative_2_Party"]
        self.state_representative_3_party = data_dict["State_Representative_3_Party"]
        self.senator_maryland_manual_online = data_dict["Senator_Maryland_Manual_Online"]
        self.representative_1_maryland_manual_online = data_dict["Representative_1_Maryland_Manual_Online"]
        self.representative_2_maryland_manual_online = data_dict["Representative_2_Maryland_Manual_Online"]
        self.representative_3_maryland_manual_online = data_dict["Representative_3_Maryland_Manual_Online"]
class USGov_Class():
    def __init__(self, data_dict):
        self.district = data_dict["District"]
        self.name = data_dict["Name"]
        self.us_representatives = data_dict["US_Representatives"]
        self.party = data_dict["Party"]
        self.us_senator_1 = data_dict["US_Senator_1"]
        self.us_senator_1_party = data_dict["US_Senator_1_Party"]
        self.us_senator_2 = data_dict["US_Senator_2"]
        self.us_senator_2_party = data_dict["US_Senator_2_Party"]
        self.us_senator_1_maryland_manual_online = data_dict["US_Senator_1_Maryland_Manual_Online"]
        self.us_senator_2_maryland_manual_online = data_dict["US_Senator_2_Maryland_Manual_Online"]
        self.us_representative_maryland_manual_online = data_dict["US_Representative_Maryland_Manual_Online"]

# VARIABLES - CONSTANTS
Variable = namedtuple("Variable", "value")
CSV_BRIDGE = Variable(r"Docs\20180613_Bridge.csv")
CSV_MDGOV = Variable(r"Docs\20180613_ElectedOfficialsMarylandGovernment.csv")
CSV_USGOV = Variable(r"Docs\20180613_ElectedOfficialsUSGovernment.csv")
SQL_BRIDGE_CREATE = Variable("""CREATE TABLE BRIDGE (
                    Row_ID text primary key, 
                    MDDistrict text,
                    USDistrict text                    
                    )""")
SQL_MDGOV_CREATE = Variable("""CREATE TABLE MDGOV (
                    District text primary key,
                    State_Senator text,
                    State_Representative_1 text,
                    State_Representative_2 text,
                    State_Representative_3 text,
                    State_Senator_Party text,
                    State_Representative_1_Party text,
                    State_Representative_2_Party text,
                    State_Representative_3_Party text,
                    Senator_Maryland_Manual_Online text,
                    Representative_1_Maryland_Manual_Online text,
                    Representative_2_Maryland_Manual_Online text,
                    Representative_3_Maryland_Manual_Online text
                    )""")
SQL_USGOV_CREATE = Variable("""CREATE TABLE USGOV (
                    District text primary key,
                    Name integer,
                    US_Representatives text,
                    Party text,
                    US_Senator_1 text,
                    US_Senator_1_Party text,
                    US_Senator_2 text,
                    US_Senator_2_Party text,
                    US_Senator_1_Maryland_Manual_Online text,
                    US_Senator_2_Maryland_Manual_Online text,
                    US_Representative_Maryland_Manual_Online text
                    )""")
SQL_BRIDGE_INSERT = Variable("""INSERT OR IGNORE INTO bridge VALUES (:row_id, :mddistrict, :usdistrict)""")
SQL_MDGOV_INSERT = Variable("""INSERT OR IGNORE INTO mdgov VALUES (:district, :state_senator, :state_representative_1, :state_representative_2, :state_representative_3, :state_senator_party, :state_representative_1_party, :state_representative_2_party, :state_representative_3_party, :senator_maryland_manual_online, :representative_1_maryland_manual_online, :representative_2_maryland_manual_online, :representative_3_maryland_manual_online)""")
SQL_USGOV_INSERT = Variable("""INSERT OR IGNORE INTO usgov VALUES (:district, :name, :us_representatives, :party, :us_senator_1, :us_senator_1_party, :us_senator_2, :us_senator_2_party, :us_senator_1_maryland_manual_online, :us_senator_2_maryland_manual_online, :us_representative_maryland_manual_online)""")

# VARIABLES - OTHER
test_database = r"Docs\testdb.db"
csv_namedtuples_list = [CSV_BRIDGE, CSV_MDGOV, CSV_USGOV]
class_types_namedtuples_list = [Bridge_Class, MDGov_Class, USGov_Class]
csv_classobject_pairing = dict(zip(csv_namedtuples_list, class_types_namedtuples_list))
sql_create_namedtuples_list = [SQL_BRIDGE_CREATE, SQL_MDGOV_CREATE, SQL_USGOV_CREATE]
sql_insert_namedtuples_list = [SQL_BRIDGE_INSERT, SQL_MDGOV_INSERT, SQL_USGOV_INSERT]
csv_sqlinsert_pairing = dict(zip(csv_namedtuples_list, sql_insert_namedtuples_list))

# FUNCTIONS
def create_database_connection(database):
    if database == ":memory:":
        return sqlite3.connect(database=":memory:")
    else:
        return sqlite3.connect(database=database)
def create_database_cursor(connection):
    return connection.cursor()
def execute_sql_command(connection, cursor, sql_command, parameters_sequence=()):
    cursor.execute(sql_command, parameters_sequence)
    return
def close_database_connection(connection):
    connection.close()
    return
def commit_to_database(connection):
    connection.commit()
    return

# FUNCTIONALITY
# Set up SQLite3 in memory database. Establish database tables
conn = create_database_connection(test_database)
# conn = create_database_connection(":memory:")
curs = create_database_cursor(conn)
for sql_namedtuple in sql_create_namedtuples_list:
    execute_sql_command(connection=conn, cursor=curs, sql_command=sql_namedtuple.value)

# Access CSV's, work on each one storing contents as objects and writing to database
for csv_namedtuple in csv_namedtuples_list:
    # objects_list = []
    with open(csv_namedtuple.value, 'r') as csv_file_handler:
        records_list_list = [(line.strip()).split(",") for line in csv_file_handler]
    headers_list = records_list_list.pop(0)
    # make appropriate objects
    for record_list in records_list_list:
        record_dictionary = dict(zip(headers_list, record_list))

        # create the type of object appropriate to csv being inspected
        data_object = csv_classobject_pairing[csv_namedtuple](record_dictionary)

        # get the insert sql statement and write CSV's to database
        insert_sql_namedtuple = csv_sqlinsert_pairing[csv_namedtuple]
        execute_sql_command(connection=conn, cursor=curs, sql_command=insert_sql_namedtuple.value, parameters_sequence=data_object.__dict__)

# SQL call to database to join tables and make one master dataset for upload
# Make connection with AGOL
# Update data in hosted feature layer


# Commit and Close things out
commit_to_database(conn)
close_database_connection(conn)