"""
DRAFT - Combine data from two csv's and update data in AGOL hosted feature layer with the new combined dataset
"""
#TODO: major weakness/pain point, when need to add/revise/delete field etc you have to manually change in multiple spots. Make code more flexible to solve this issue.

# IMPORTS
from collections import namedtuple
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
    CSV_MDGOV = Variable(r"Docs\20180614_ElectedOfficialsMarylandGovernment.csv")
    CSV_USGOV = Variable(r"Docs\20180614_ElectedOfficialsUSGovernment.csv")
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
    SQL_SELECT_OUTPUT_DATA = Variable("""SELECT MDGOV.*, BRIDGE.Row_ID, USGOV.* FROM MDGOV, BRIDGE, USGOV WHERE MDGOV.MD_District = BRIDGE.MD_District AND BRIDGE.US_District = USGOV.US_District""")

    # VARIABLES - OTHER
    test_database = r"Docs\testdb.db"           # TESTING
    csv_namedtuples_list = [CSV_BRIDGE, CSV_MDGOV, CSV_USGOV]
    class_types_namedtuples_list = [Bridge_Class, MDGov_Class, USGov_Class]
    csv_classobject_pairing = dict(zip(csv_namedtuples_list, class_types_namedtuples_list))
    sql_create_namedtuples_list = [SQL_CREATE_BRIDGE, SQL_CREATE_MDGOV, SQL_CREATE_USGOV]
    sql_insert_namedtuples_list = [SQL_INSERT_BRIDGE, SQL_INSERT_MDGOV, SQL_INSERT_USGOV]
    csv_sqlinsert_pairing = dict(zip(csv_namedtuples_list, sql_insert_namedtuples_list))

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

    # FUNCTIONALITY
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

    # SQL Commit and Close things out
    commit_to_database(conn)
    close_database_connection(conn)

    # Update data in hosted feature layer
    # Make connection with AGOL

if __name__ == "__main__":
    main()
