from google.cloud import spanner

def connectDatabase():
    spanner_client = spanner.Client()
    instance = spanner_client.instance('spanner-dds')
    database = instance.database('karan-db')
    return database

def insertData(database):
    try:
        dataToInsert = [(2,), (3,), (4,), (5,)]
        with database.batch() as batch:
            batch.insert(
                table='events',
                columns=['id'], 
                values=dataToInsert  
            )
        print("Data Inserted Successfuly")
    except Exception as e:
        print(f"Data insertion failed because {e}")
        
def fetchData(database):
    try:
        query = "SELECT id FROM events"
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        for row in results:
            print(row)

        print("Data Fetched Successfuly")
    except Exception as e:
        print(f"Data Fetch failed because {e}")
    
if __name__=="__main__":
    db = connectDatabase()
    insertData(db)
    fetchData(db)