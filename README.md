# DoIT_MDElectAppProcess
Read data in csv files and create sqlite3 database in-memory, overwrite feature class data using database, and overwrite hosted feature layer on ArcGIS Online.

Process looks for csv files of data on elected officials. The Maryland State Archive agency maintains the elected
officials data using two tabs in a Google Sheet. We have a process that pulls csv's of the tabs. One tab is MD specific,
the other is Federal specific. The process takes the data from those csv's and builds and loads tables using sqlite3.
Due to many-to-many relationships between MD District and US District, a bridge table is necessary. A Bridge csv exists.
The csv is read and used to create a database table the same as the elected officials data. The database is used to build
a set of new records. These records, with unique identifier row_id, are used to update/overwrite the attributes in a
feature class of polygons representinng unique district combination areas. The combination areas are unique combinations
of the MD Districts and the US Districts layers. An ArcGIS Pro project exists. It contains a feature class of the
conbinattion polygons. This layer is geometrically identical to the hosted feature layer on ArcGIS Online.
Once the feature class is updated, the Python API for ArcGIS is used to republish the ArcPro project and overwrite the
hosted feature layer on ArcGIS Online.