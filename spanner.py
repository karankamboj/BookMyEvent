import os
from google.cloud import spanner
from google.auth import default
from google.oauth2 import service_account
from datetime import datetime, timezone
from enum import Enum
import databaseConfig
import syncDatabase


class OperationType(Enum):
    READ = "READ"
    WRITE = "WRITE"
from constants import OperationType


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

def insertData(tableName, columns, dataToInsert):
    database = databaseConfig.connectDatabase(OperationType.WRITE)
    return insertDataUtil(database, tableName, columns, dataToInsert)

        
def fetchData(tableName):
    try:
        #Fetch changes from Read Only Database
        database = databaseConfig.connectDatabase(OperationType.READ)
        return fetchDataUtil(database, tableName)
    except Exception as e:        # Fetch data from write databse when Read Only fails
        database = databaseConfig.connectDatabase(OperationType.WRITE)
        return fetchDataUtil(database, tableName)
        

def fetchDataUtil(database, tableName):
    try:
        query = "SELECT * FROM "+ tableName
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
        return list(results)
    except Exception as e:
        return f"Data Fetch failed because {e}"
    
def updateDataUtil(database, tableName, columns, dataToUpdate, primary_col, primary_val):
    try:
        updates = [
            [primary_val] + dataToUpdate  # Primary key comes first
        ]
        
        with database.batch() as batch:
            batch.update(
                table=tableName,
                columns=[primary_col] + columns,
                values=updates,
            )
        return f"Record updated successfully from table {tableName} where {primary_col} = {primary_val}."

    except Exception as e:
        return f"Data update failed because {e}"
    
def updateData(tableName, columns, dataToUpdate, primary_col, primary_val):
    database = databaseConfig.connectDatabase(OperationType.WRITE)
    return updateDataUtil(database, tableName, columns, dataToUpdate, primary_col, primary_val)

def deleteDataUtil(database, tableName, primary_col, primary_val):
    try:
        with database.batch() as batch:
            batch.delete(
                table=tableName,
                keyset=spanner.KeySet(keys=[[primary_val]]),  # Spanner expects keys as a list of lists
            )

        return f"Record deleted successfully from table {tableName} where {primary_col} = {primary_val}."
    
    except Exception as e:
        return f"Data deletion failed because {e}"

def deleteData(tableName, primary_col, primary_val):
    database = databaseConfig.connectDatabase(OperationType.WRITE)
    return deleteDataUtil(database, tableName, primary_col, primary_val)
  
def test():
    print("HERE")
    source_db_1 = databaseConfig.getReadDbInstance()
    source_db_2 = databaseConfig.getWriteDbInstance()

    print(source_db_1)
    print(source_db_2)

    columns = ["user_id", "name", "email", "phone_number", "region", "created_at"]
    dataToInsert = [
        (2, 'Alice', 'alice@example.com', '123-456-7890', 'North America', datetime.now(timezone.utc)),
        (3, 'Shan', 'bob@example.com', '987-654-3210', 'Europe', datetime.now(timezone.utc)),
    ]
    print(insertData("users", columns, dataToInsert))
    respose = fetchData("users")
    print(respose)
