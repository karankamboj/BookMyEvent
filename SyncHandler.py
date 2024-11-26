import time
import schedule
from datetime import datetime
from google.cloud import spanner
import databaseConfig
from constants import OperationType

def fetchDataFromWriteDB():
    """
    Fetches all the data from the write DB (primary database).
    Returns a list of rows fetched from the table.
    """
    try:
        # Get the write database instance
        database = databaseConfig.connectDatabase(OperationType.WRITE)
        tableName = "users"
        query = f"SELECT * FROM {tableName}"
        
        # Fetch data from the write database
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        return list(results)
    except Exception as e:
        print(f"Error fetching data from write DB: {e}")
        return []
def checkIfRowExistsInReadDB(user_id):
    """
    Checks if a row with the given user_id exists in the read database
    """
    try:
        database = databaseConfig.connectDatabase(OperationType.READ)
        query = f"SELECT COUNT(*) FROM users WHERE user_id = @user_id"
        
        with database.snapshot() as snapshot:
            result = snapshot.execute_sql(query, params={"user_id": user_id})
            
            return len(list(result))  # Return True if row exists, False otherwise
    except Exception as e:
        print(f"Error checking row existence in read DB: {e}")
        return False
def insertDataIntoReadDB(data):
    """
    Inserts data into the read database, checking for duplicates
    """
    new_data = []
    for row in data:
        user_id = row[0]  # Assuming user_id is the first column (primary key)
        
        # Check if row already exists in read DB
        if not checkIfRowExistsInReadDB(user_id):
            new_data.append(row)
        else:
            print(f"Skipping existing row with user_id {user_id}")
    
    # If there is new data, insert it into the read DB
    if new_data:
        try:
            database = databaseConfig.connectDatabase(OperationType.READ)
            with database.batch() as batch:
                batch.insert(
                    table="users",
                    columns=["user_id", "name", "email", "phone_number", "region", "created_at"],
                    values=new_data
                )
            print(f"Successfully inserted {len(new_data)} new rows into read DB.")
        except Exception as e:
            print(f"Error inserting data into read DB: {e}")
    else:
        print("No new data to insert into read DB.")


def syncData():
    """
    Main function to handle data sync from write DB to read-only DB.
    """
    print(f"Syncing data at {datetime.now()}")
    
    # Fetch data from the write DB
    data = fetchDataFromWriteDB()
    
    # If data is fetched, insert it into the read-only DB
    if data:
        insertDataIntoReadDB(data)
    else:
        print("No new data to sync.")

def startSyncJob():
    """
    Schedules the sync task to run every 5 minutes.
    """
    # Schedule the sync to run every 5 minutes
    schedule.every(10).seconds.do(syncData)
    
    print("Sync job started.")
    
    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)  # Sleep to prevent busy-waiting

if __name__ == "__main__":
    # Start the sync process
    startSyncJob()
