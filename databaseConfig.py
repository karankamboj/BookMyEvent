import os
from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account
from datetime import datetime, timezone
from enum import Enum
    
def connect_to_spanner_instance(service_account_path, instance_id, database_id):
    # Set the environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
    client = spanner.Client()
    instance = client.instance(instance_id)
    database = instance.database(database_id)
    return database

def getReadDbInstance():
    service_account_path_1_kkamobj = "./keys/spanner-project-439003-fda10b1b5add.json"
    instance_id_1_kkamboj = "spanner-dds"
    database_id_1_kkamboj = "karan-db"

    source_db = connect_to_spanner_instance(service_account_path_1_kkamobj, instance_id_1_kkamboj, database_id_1_kkamboj)
    return source_db

def getWriteDbInstance():
    service_account_path_2_nchaud = "./keys/key_2_nchaud.json"
    instance_id_2_nchaud = "bookmyevent"
    database_id_2_nchaud = "ddsdb"
    source_db = connect_to_spanner_instance(service_account_path_2_nchaud, instance_id_2_nchaud, database_id_2_nchaud)
    return source_db

def connectDatabase(OperationType=None):
    database = getWriteDbInstance()
    return database