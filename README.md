# DoIT_MDElectAppProcess
Two step process serving MD State Archives need to feed changing attributes to a web map and also an SDE feature class.

**Step 1:** Read data in csv files, create sqlite3 database in-memory, overwrite feature class data using database, and 
overwrite hosted feature layer on ArcGIS Online. 
Process looks for csv files of data on elected officials. The Maryland State Archive agency maintains the elected
officials data using two tabs in a Google Sheet. A separate process prior to Step 1 pulls csv's of the twp tabs. One 
tab is MD specific, the other is Federal specific. The process takes the data from those csv's and builds and loads
tables using sqlite3. Due to many-to-many relationships between MD District and US District, a bridge table is 
necessary. A Bridge csv exists. The csv is read and used to create a database table the same as the elected officials 
data. The database is used to build a set of new records. These records, with unique identifier row_id, are used to 
update/overwrite the attributes in a feature class of polygons representinng unique district combination areas. The 
combination areas are unique combinations of the MD Districts and the US Districts layers. An ArcGIS Pro project 
exists. It contains a feature class of the conbinattion polygons. This layer is geometrically identical to the hosted 
feature layer on ArcGIS Online. Once the feature class is updated, the Python API for ArcGIS is used to republish the 
ArcPro project and overwrite the hosted feature layer on ArcGIS Online.

**Step 2:** Read data in csv files and update SDE feature classes
Process looks for csv files of data on elected officials the same as Step 1. A MD Election Boundaries feature class and
a US Government Election Boundaries feature class are accessed and the attributes are updated using the csv data. This 
process keeps the SDE feature classes current with the hosted feature layer on ArcGIS Online. 