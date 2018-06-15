# IMPORTS
import arcpy
import os

# VARIABLES
fc_name = "ElectedOfficials"
path_master_dataset_csv = r"E:\DoIT_MDElectAppProcess\Docs\test_sql_pull_data.csv"
path_project_gdb = r"E:\DoIT_MDElectAppProcess\Docs\ElectedOfficals\ElectedOfficals.gdb"
record_list_list = []
record_strings_list = []

# FUNCTIONS
def reverse_dictionary(dictionary):
    return {value : key for key, value in dictionary.items()}

# FUNCTIONALITY
assert os.path.exists(path_master_dataset_csv)
assert os.path.exists(path_project_gdb)

# Non-Spatial
# access the master dataset csv file contents
with open(path_master_dataset_csv, 'r') as csv_file_handler:
    for line in csv_file_handler:
        record_strings_list.append(line)

# grab the headers
csv_headers_list = ((record_strings_list[0]).strip()).split(",")

# need a mapping between gis data and csv data
csv_headers_to_fc_field_names_dict = {"MD_District" : "MD_District",
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
fc_field_names_to_csv_headers_dict = reverse_dictionary(csv_headers_to_fc_field_names_dict)

# store the index position of each header
csv_header_index_position_dict = {header : csv_headers_list.index(header) for header in csv_headers_list}


# Spatial
# access feature class
arcpy.env.workspace = path_project_gdb

fc_fields = arcpy.ListFields(fc_name)

# grab the field names
fc_field_names_list = [(fc_field.name).strip() for fc_field in fc_fields]
# print(fc_field_names_list)

# store the index position of each name
fc_field_names_index_position_dict = {name : fc_field_names_list.index(name) for name in fc_field_names_list}
# print(fc_field_names_index_position_dict)

# isolate the fields that have a corresponding header in the csv file
fc_field_names_matching_header_list = [name for name in fc_field_names_list if name in fc_field_names_to_csv_headers_dict.keys()]
fc_field_names_matching_header_index_dictionary = reverse_dictionary(dict(enumerate(fc_field_names_matching_header_list)))
# print(fc_field_names_matching_header_index_dictionary)
# exit()
# print(fc_field_names_matching_header_list)

#______________
# TESTING
# with arcpy.da.SearchCursor(in_table=fc_name, field_names=fc_field_names_matching_header_list) as search_cursor:
#     for row in search_cursor:
#         row[0]
#______________


with arcpy.da.UpdateCursor(in_table=fc_name, field_names=fc_field_names_matching_header_list) as search_cursor:
    for row in search_cursor:
        # print(row)
        current_row_id = row[fc_field_names_matching_header_index_dictionary["Row_ID"]]
        # print(current_row_id)
        # Integrate with other script and sse sql query on in memory database to get the data record that matches the row in the feature class
        # Use the row to update the feature class data

