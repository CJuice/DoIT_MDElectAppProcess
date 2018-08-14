"""
TODO

"""
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

class Data_Class():
    # Parent
    def __init__(self, district, rep_1, rep_1_manual, rep_1_party, sen_1, sen_1_manual, sen_1_party):
        self.district = district
        self.representative_1 = rep_1
        self.representative_1_manual = rep_1_manual
        self.representative_1_party = rep_1_party
        self.senator_1 = sen_1
        self.senator_1_md_manual_online = sen_1_manual
        self.senator_1_party = sen_1_party


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

class MD_Data_Class(Data_Class):
    # Child
    def __init__(self, data_dict):
        self.representative_2 = data_dict["State_Representative_2"]
        self.representative_2_md_manual_online = data_dict["State_Representative_2_Maryland_Manual_Online"]
        self.representative_2_party = data_dict["State_Representative_2_Party"]
        self.representative_3 = data_dict["State_Representative_3"]
        self.representative_3_md_manual_online = data_dict["State_Representative_3_Maryland_Manual_Online"]
        self.representative_3_party = data_dict["State_Representative_3_Party"]
        super().__init__(district=data_dict["MD_District"],
                         rep_1=data_dict["State_Representative_1"],
                         rep_1_manual=data_dict["State_Representative_1_Maryland_Manual_Online"],
                         rep_1_party=data_dict["State_Representative_1_Party"],
                         sen_1=data_dict["State_Senator"],
                         sen_1_manual=data_dict["State_Senator_Maryland_Manual_Online"],
                         sen_1_party=data_dict["State_Senator_Party"])


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

class US_Data_Class(Data_Class):
    # Child
    def __init__(self, data_dict):
        self.label = data_dict["Label"]
        self.name = data_dict["Name"]
        self.senator_2 = data_dict["US_Senator_2"]
        self.senator_2_md_manual_online = data_dict["US_Senator_2_Maryland_Manual_Online"]
        self.senator_2_party = data_dict["US_Senator_2_Party"]
        super().__init__(district=data_dict["US_District"],
                         rep_1=data_dict["US_Representatives"],
                         rep_1_manual=data_dict["US_Representatives_Maryland_Manual_Online"],
                         rep_1_party=data_dict["US_Representatives_Party"],
                         sen_1=data_dict["US_Senator_1"],
                         sen_1_manual=data_dict["US_Senator_1_Maryland_Manual_Online"],
                         sen_1_party=data_dict["US_Senator_1_Party"])

class Util_Class():
    """
    TODO
    """
    def __init__(self):
        pass

    @staticmethod
    def clean_url_slashes(url):
        return url.replace("\\", "/")
    @staticmethod
    def close_database_connection(connection):
        """
        Close the sqlite3 database connection.

        :param connection: sqlite3 connection to be closed
        :return: Nothin
        """
        connection.close()
        return

    @staticmethod
    def clean_and_split(line):
        """
        Strip line string and split on commas into a list, return list.

        :param line: record string from csv dataset
        :return: cleaned and split line as list
        """
        return (line.strip()).split(",")

    @staticmethod
    def commit_to_database(connection):
        """
        Make a commit to sqlite3 database.

        :param connection: sqlite database connection
        :return: Nothing
        """
        connection.commit()
        return

    @staticmethod
    def create_database_connection(database):
        """
        Establish sqlite3 database connection and return connection.

        :param database: database to create
        :return: database connection
        """
        import sqlite3
        if database == ":memory:":
            return sqlite3.connect(database=":memory:")
        else:
            return sqlite3.connect(database=database)

    @staticmethod
    def create_database_cursor(connection):
        """
        Create cursor for database data access and return cursor.

        :param connection: database connection to use
        :return: database cursor
        """
        return connection.cursor()

    @staticmethod
    def create_file_generator(file_path):
        with open(file_path, 'r') as handler:
            for line in handler:
                line = line.strip()
                yield line

    @staticmethod
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

    @staticmethod
    def process_csv_data_to_objects(csv_path, object_type, delimeter=","):
        line_generator = Util_Class.create_file_generator(csv_path)
        headers_list = []
        objects_list = []
        i = 0
        for line in line_generator:
            line = line.strip()
            line_list = line.split(delimeter)
            data_dict = dict(zip(headers_list,line_list))   # first line through will produce empty dictionary
            if i > 0:
                new_obj = object_type(data_dict)
                objects_list.append(new_obj)
                i += 1
            else:
                # capture headers first line through
                headers_list = line_list
                i += 1
        return objects_list

    @staticmethod
    def reverse_dictionary(dictionary):
        """
        Swap the key and value, creating new dictionary, and return new dictionary.
        :param dictionary: in dictionary to be reversed
        :return: reversed dictionary
        """
        return {value: key for key, value in dictionary.items()}