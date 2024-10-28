from google.cloud import spanner

def connectDatabase():
    spanner_client = spanner.Client()
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