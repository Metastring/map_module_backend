import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

############# Database Configuration ###############
# Uses environment variables from .env file, with local defaults
host = os.getenv("DB_HOST", "localhost")
port = int(os.getenv("DB_PORT", "5432"))
username = os.getenv("DB_USERNAME", "postgres")
password = os.getenv("DB_PASSWORD", "2002")
database = os.getenv("DB_NAME", "CML_test")
db_schema = os.getenv("DB_SCHEMA", "cml1")
database_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"


############## GeoServer Configuration ###############
# Uses environment variables from .env file, with local defaults
geoserver_host = os.getenv("GEOSERVER_HOST", "localhost")
geoserver_port = os.getenv("GEOSERVER_PORT", "8080")
geoserver_username = os.getenv("GEOSERVER_USERNAME", "admin")
geoserver_password = os.getenv("GEOSERVER_PASSWORD", "geoserver")
geoserver_data_dir = os.getenv("GEOSERVER_DATA_DIR", "/usr/share/geoserver/geoserver-2.26.1/data_dir/data")

############## Sudo Configuration ###############
sudo_password = os.getenv("SUDO_PASSWORD", "meta")

####################### Dataset Mapping Configuration #########################
# Maps frontend dataset names to actual database table names
DATASET_MAPPING = {
    "gbif": "gbif",
    "kew": "kew_with_geom",
    # Add more mappings as needed
    # "frontend_name": "database_table_name"
}

REVERSE_DATASET_MAPPING = {v: k for k, v in DATASET_MAPPING.items()}
