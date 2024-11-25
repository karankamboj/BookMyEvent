import os
from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account
from datetime import datetime, timezone
from enum import Enum


class OperationType(Enum):
    READ = "READ"
    WRITE = "WRITE"
    
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

    database = getReadDbInstance()
    return database

def insertDataUtil(database, tableName, columns, dataToInsert):
    try:
        with database.batch() as batch:
            batch.insert(
                table= tableName,
                columns=columns, 
                values=dataToInsert  
            )
        return "Data Inserted Successfuly"
    except Exception as e:
        return f"Data insertion failed because {e}"

def insertData(database, tableName, columns, dataToInsert):
    return insertDataUtil(database, tableName, columns, dataToInsert)
        
def fetchData(database, tableName):
    return fetchDataUtil(database, tableName)

def fetchDataUtil(database, tableName):
    try:
        query = "SELECT * FROM "+tableName
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        return list(results)
    except Exception as e:
        return f"Data Fetch failed because {e}"
    
if __name__=="__main__":
    print("HERE")
    source_db_1 = getReadDbInstance()
    source_db_2 = getWriteDbInstance()

    print(source_db_1)
    print(source_db_2)

    db = connectDatabase()
    columns = ["user_id", "name", "email", "phone_number", "region", "created_at"]
    dataToInsert = [
        (2, 'Alice', 'alice@example.com', '123-456-7890', 'North America', datetime.now(timezone.utc)),
        (3, 'Shan', 'bob@example.com', '987-654-3210', 'Europe', datetime.now(timezone.utc)),
    ]
    print(insertData(db, "users", columns, dataToInsert))
    respose = fetchData(db, "users")
    print(respose)