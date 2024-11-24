from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account

def connectDatabase():
    json_key_path = "C:\\Users\\Acer\\Documents\\Study\\DDS\\BookMyEvent\\keys\\spanner-project-439003-fda10b1b5add.json"  # Replace with your key file path
    credentials = service_account.Credentials.from_service_account_file(json_key_path)

    spanner_client = spanner.Client(credentials=credentials, project="spanner-project-439003")
    
    print(f"Connected to project: {spanner_client.project}")

    instance = spanner_client.instance('spanner-dds')
    database = instance.database('karan-db')
    return database

def insertData(database, dataToInsert):
    try:
        with database.batch() as batch:
            batch.insert(
                table='events',
                columns=['id'], 
                values=dataToInsert  
            )
        return "Data Inserted Successfuly"
    except Exception as e:
        return f"Data insertion failed because {e}"
        
def fetchData(database):
    try:
        query = "SELECT id FROM events"
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        return list(results)
    except Exception as e:
        return f"Data Fetch failed because {e}"
    
if __name__=="__main__":
    db = connectDatabase()
    print(insertData(db, [[18]]))
    respose = fetchData(db)
    print(respose)