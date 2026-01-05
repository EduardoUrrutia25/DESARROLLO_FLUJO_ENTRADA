import os
from dotenv import load_dotenv
load_dotenv()

ALL_DB_CONFIGS = {
    'xx': {
        'server': '10.180.11.110',
        'database': 'RetailData_QA',
        'uid': 'retailuser',
        'pwd': 'retail',
        'driver': '{ODBC Driver 17 for SQL Server}',
        #'encoding': 'utf-8',
        'autocommit': True
    },
    'yy': {
        'server': '10.180.11.110',
        'database': 'Aruma_QA2',
        'uid': 'retailuser',
        'pwd': 'retail',
        'driver': '{ODBC Driver 17 for SQL Server}',
        #'encoding': 'utf-8',
        'autocommit': True
    }
}

# -- Conexión ABS ---
AZURE_BLOB_CONNECTION_STRING = f"{os.getenv('ABS_CONNECTION_STRING')}"
#AZURE_BLOB_CONNECTION_STRING = "https://lindpedevst.blob.core.windows.net/rcib-bucket-stg1?si=Oracle-RMS&sv=2024-11-04&sr=c&sig=hXthkwSPIXZrdjRY8FXp%2BfPOLAoj9LuI0vbQyVVlB3U%3D"
#AZURE_CONTAINER_NAME = "lindpedevst"