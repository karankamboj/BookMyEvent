from google.cloud import spanner

def connectDatabase():
    spanner_client = spanner.Client()
    instance = spanner_client.instance('spanner-dds')
    database = instance.database('karan-db')
    return database

def getData(database):
    query = "SELECT id FROM events"
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(query)
    for row in results:
        print(row)
    
if __name__=="__main__":
    db = connectDatabase()
    getData(db)