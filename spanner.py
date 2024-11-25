from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account

import os
from google.cloud import spanner

def connect_to_spanner_instance(service_account_path, instance_id, database_id):
    # Set the environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
    client = spanner.Client()
    instance = client.instance(instance_id)
    database = instance.database(database_id)
    return database
    
def connectDatabase():
   
    database = getnchaudDbInstance()
    return database

def insertData(database, dataToInsert):
    try:
        with database.batch() as batch:
            batch.insert(
                table='test',
                columns=['id'], 
                values=dataToInsert  
            )
        return "Data Inserted Successfuly"
    except Exception as e:
        return f"Data insertion failed because {e}"
        
def fetchData(database):
    try:
        query = "SELECT * FROM users"
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        return list(results)
    except Exception as e:
        return f"Data Fetch failed because {e}"
def getkambojDbInstance():
    service_account_path_1_kkamobj = "./keys/key_1_kkamboj.json"

    instance_id_1_kkamboj = "spanner-dds"
    database_id_1_kkamboj = "karan-db"

    source_db = connect_to_spanner_instance(service_account_path_1_kkamobj, instance_id_1_kkamboj, database_id_1_kkamboj)
    return source_db

def getnchaudDbInstance():
    service_account_path_2_nchaud = "./keys/key_2_nchaud.json"

    instance_id_2_nchaud = "bookmyevent"
    database_id_2_nchaud = "ddsdb"

    source_db = connect_to_spanner_instance(service_account_path_2_nchaud, instance_id_2_nchaud, database_id_2_nchaud)

    return source_db



if __name__ == "__main__":
   
    source_db_2 = getnchaudDbInstance()
    print(insertData(source_db_2, [[19]]))
    respose = fetchData(source_db_2)
    print(respose)
