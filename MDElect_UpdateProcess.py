"""
DRAFT - Combine data from two csv's and update data in AGOL hosted feature layer with the new combined dataset
"""

# IMPORTS
from collections import namedtuple
# VARIABLES - CONSTANTS
Variable = namedtuple("Variable", "value")
CSV_MDGOV = Variable("Docs\\")
CSV_USGOV = Variable("Docs\\")

# VARIABLES - OTHER

# FUNCTIONS

# FUNCTIONALITY
# Access both CSV's
# Set up SQLite3 in memory database
# write both CSV's to database
# Create bridge table that demonstrates relationship between tables
# SQL call to database to join tables and make one master dataset for upload
# Make connection with AGOL
# Update data in hosted feature layer
# Close things out

