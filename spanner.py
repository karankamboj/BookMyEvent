import os
from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account
from datetime import datetime, timezone

def connect_to_spanner_instance(service_account_path, instance_id, database_id):
    # Set the environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
    client = spanner.Client()
    instance = client.instance(instance_id)
    database = instance.database(database_id)
    return database

def connectDatabase():
    json_key_path = "C:\\Users\\Acer\\Documents\\Study\\DDS\\BookMyEvent\\keys\\dds-112.json"  # Replace with your key file path
    credentials = service_account.Credentials.from_service_account_file(json_key_path)

    spanner_client = spanner.Client(credentials=credentials)
    
    print(f"Connected to project: {spanner_client.project}")

    instance = spanner_client.instance(instance_id="bookmyevent")
    database = instance.database(database_id="ddsdb")
    return database

def insertData(database, tableName, columns, dataToInsert):
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
        
def fetchData(database, tableName):
    try:
        query = "SELECT * FROM "+tableName
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        return list(results)
    except Exception as e:
        return f"Data Fetch failed because {e}"
    
if __name__=="__main__":
    #Path to service accounts
    service_account_path_1_kkamobj = "/Users/nishtha/Desktop/Courses/CSE512/Project/BookMyEvent/key_1_kkamboj.json"
    service_account_path_2_nchaud = "/Users/nishtha/Desktop/Courses/CSE512/Project/BookMyEvent/key_2_nchaud.json"

    instance_id_1_kkamboj = "spanner-dds"
    database_id_1_kkamboj = "karan-db"

    instance_id_2_nchaud = "bookmyevent"
    database_id_2_nchaud = "ddsdb"

    source_db_1 = connect_to_spanner_instance(service_account_path_1_kkamobj, instance_id_1_kkamboj, database_id_1_kkamboj)
    source_db_2 = connect_to_spanner_instance(service_account_path_2_nchaud, instance_id_2_nchaud, database_id_2_nchaud)

    # replicate_data(source_db, target_db)
    print(source_db_1)
    print(source_db_2)

    db = connectDatabase()
    columns = ["user_id", "name", "email", "phone_number", "region", "created_at"]
    dataToInsert = [
        (2, 'Alice', 'alice@example.com', '123-456-7890', 'North America', datetime.now(timezone.utc)),
        (3, 'Bob', 'bob@example.com', '987-654-3210', 'Europe', datetime.now(timezone.utc)),
    ]
    print(insertData(db, "users", columns, dataToInsert))
    respose = fetchData(db, "users")
    print(respose)