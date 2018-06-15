#https://www.esri.com/arcgis-blog/products/api-python/analytics/updating-your-hosted-feature-services-with-arcgis-pro-and-the-arcgis-api-for-python/
#https://esri.github.io/arcgis-python-api/apidoc/html/index.html
import arcpy
import json
import os, sys
from arcgis.gis import GIS

### Start setting variables
# Set the path to the project
arcpro_project_path = r"E:\DoIT_MDElectAppProcess\Docs\ElectedOfficals\ElectedOfficals.aprx"

# Update the following variables to match:
# Feature service/SD name in arcgis.com, user/password of the owner account
sd_featureservice_name = "Elected_Officials"
portal = "http://maryland.maps.arcgis.com" # Can also reference a local portal
credentials_path = r"Docs\credentials.json"
assert os.path.exists(credentials_path)
with open(credentials_path, 'r') as file_handler:
    file_contents = file_handler.read()
credentials_json = json.loads(file_contents)

agol_username = credentials_json["username"]
agol_password = credentials_json["password"]

# Set sharing options
share_organization = False
share_everyone = False
share_groups = ""

### End setting variables

# Local paths to create temporary content
relative_path = sys.path[0]
sd_draft_filename = os.path.join(relative_path, "Elected_Officals.sddraft")
sd_filename = os.path.join(relative_path, "Elected_Officals.sd")

# Create a new SDDraft and stage to SD
print("Creating SD file")
arcpy.env.overwriteOutput = True
project = arcpy.mp.ArcGISProject(aprx_path=arcpro_project_path)
map = project.listMaps()[0] # Have only one map in aprx. Grabs first map.
arcpy.mp.CreateWebLayerSDDraft(map_or_layers=map,
                               out_sddraft=sd_draft_filename,
                               service_name=sd_featureservice_name,
                               server_type="MY_HOSTED_SERVICES",
                               service_type="FEATURE_ACCESS",
                               folder_name="",
                               overwrite_existing_service=True,
                               copy_data_to_server=True,
                               enable_editing=False,
                               allow_exporting=False,
                               enable_sync=False,
                               summary=None,
                               tags=None,
                               description=None,
                               credits=None,
                               use_limitations=None)
arcpy.StageService_server(in_service_definition_draft=sd_draft_filename,
                          out_service_definition=sd_filename)

print("Connecting to {}".format(portal))
gis = GIS(url=portal,
          username=agol_username,
          password=agol_password,
          key_file=None,
          cert_file=None,
          verify_cert=True,
          set_active=True,
          client_id=None,
          profile=None)

# Find the SD, update it, publish /w overwrite and set sharing and metadata
print("Search for original service definition file (.sd) on portal…")
try:
    agol_sd_item = gis.content.search("{} AND owner:{}".format(sd_featureservice_name, agol_username), item_type="Service Definition")[0]
except:
    # agol_sd_item = gis.content.search("{} AND owner:{}".format(sd_featureservice_name, agol_username), item_type="Service Definition")
    # print(agol_sd_item)
    print("Search not successful. Check that .sd file is present and that the username supplie is the owner of the file.")
    exit()

print("Found SD: {}, ID: {} n Uploading and overwriting…".format(agol_sd_item.title, agol_sd_item.id))
agol_sd_item.update(data=sd_filename)

print("Overwriting existing feature service…")
feature_service = agol_sd_item.publish(overwrite=True)

if share_organization or share_everyone or share_groups:
    print("Setting sharing options…")
    feature_service.share(org=share_organization, everyone=share_everyone, groups=share_groups)

print("Finished updating: {} – ID: {}".format(feature_service.title, feature_service.id))
