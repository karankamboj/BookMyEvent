from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account
from datetime import datetime, timezone


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
    db = connectDatabase()
    columns = ["user_id", "name", "email", "phone_number", "region", "created_at"]
    dataToInsert = [
        (2, 'Alice', 'alice@example.com', '123-456-7890', 'North America', datetime.now(timezone.utc)),
        (3, 'Bob', 'bob@example.com', '987-654-3210', 'Europe', datetime.now(timezone.utc)),
    ]
    print(insertData(db, "users", columns, dataToInsert))
    respose = fetchData(db, "users")
    print(respose)