import time
import schedule
from datetime import datetime
from google.cloud import spanner
import databaseConfig
import elasticSearch

def syncData():
    print(f"Syncing data at {datetime.now()}")
    source_db_1 = databaseConfig.getWriteDbInstance()
    source_db_2 = databaseConfig.getReadDbInstance()
    sync_spanner_databases(source_db_1, source_db_2)

def sync_spanner_databases(source_db, target_db):
    """
    Syncs data from the source Spanner database to the target Spanner database.
    
    Args:
        source_db (spanner.Database): The source Spanner database instance.
        target_db (spanner.Database): The target Spanner database instance.
    """
    def sync_table_data(table_name):  

        with source_db.snapshot() as snapshot:
            column_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            """
            columns = [row[0] for row in snapshot.execute_sql(column_query)]
        
        with source_db.snapshot() as snapshot:
            primary_key_query = f"""
            SELECT column_name
            FROM information_schema.index_columns
            WHERE table_name = '{table_name}' AND index_name = 'PRIMARY_KEY'
            """
            primary_keys = [row[0] for row in snapshot.execute_sql(primary_key_query)]

        # Fetch data from the source table
        with source_db.snapshot() as snapshot:
            data_query = f"SELECT * FROM {table_name}"
            rows = list(snapshot.execute_sql(data_query))
        
        source_rows_dict = [dict(zip(columns, row)) for row in rows]

        # Fetch data from the target table
        with target_db.snapshot() as snapshot:
            data_query = f"SELECT * FROM {table_name}"
            target_rows = list(snapshot.execute_sql(data_query))

        target_rows_dict = [dict(zip(columns, row)) for row in target_rows]
        
        # Convert source and target rows to sets for comparison
        source_primary_keys_set = {tuple(row[pk] for pk in primary_keys) for row in source_rows_dict}
        
        # Determine rows to delete
        rows_to_delete = [row for row in target_rows_dict if tuple(row[pk] for pk in primary_keys) not in source_primary_keys_set]
        print(rows_to_delete)

        # Perform upserts
        if rows:
            data_to_insert = [tuple(row[col] for col in columns) for row in source_rows_dict]
            try:
                with target_db.batch() as batch:
                    batch.insert_or_update(
                        table=table_name,
                        columns=columns,
                        values=data_to_insert,
                    )
                print(f"Inserted/updated {len(data_to_insert)} rows into table {table_name}.")
            except Exception as e:
                print(f"Error inserting/updating rows in {table_name}: {e}")

        # Perform deletions
        if rows_to_delete:
            keys_to_delete = [tuple(row[pk] for pk in primary_keys) for row in rows_to_delete]
            try:
                with target_db.batch() as batch:
                    batch.delete(
                        table=table_name,
                        keyset=spanner.KeySet(keys=keys_to_delete),
                    )
                print(f"Deleted {len(keys_to_delete)} rows from table {table_name}.")
            except Exception as e:
                print(f"Error deleting rows in {table_name}: {e}")

        print(f"Successfully synced table {table_name}.")


    # Fetch table names from the source database
    with source_db.snapshot() as snapshot:
        tables_query = (
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
        )
        tables = snapshot.execute_sql(tables_query)
        table_names = [row[0] for row in tables]

    # Sync data for each table
    for table_name in table_names:
        print(f"Syncing table: {table_name}")
        sync_table_data(table_name)

def syncIndexData():
    print(f"Syncing data at {datetime.now()}")
    source_db = databaseConfig.getWriteDbInstance()
    sync_table_to_elasticsearch(source_db,'event','event_index')

def sync_table_to_elasticsearch(spanner_db, table_name, index_name):

    # Fetch columns and primary key from the table
    with spanner_db.snapshot() as snapshot:
        column_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        columns = [row[0] for row in snapshot.execute_sql(column_query)]

    with spanner_db.snapshot() as snapshot:
        primary_key_query = f"""
        SELECT column_name
        FROM information_schema.index_columns
        WHERE table_name = '{table_name}' AND index_name = 'PRIMARY_KEY'
        """
        primary_keys = [row[0] for row in snapshot.execute_sql(primary_key_query)]

    # Fetch all rows from the table
    with spanner_db.snapshot() as snapshot:
        data_query = f"SELECT * FROM {table_name}"
        rows = list(snapshot.execute_sql(data_query))
    
    # Prepare data for Elasticsearch
    source_rows = [dict(zip(columns, row)) for row in rows]

    # Fetch existing documents from Elasticsearch
    es_docs = {}
    try:
        result = elasticSearch.es.search(index=index_name, size=10000)
        es_docs = {
            hit["_id"]: hit["_source"]
            for hit in result["hits"]["hits"]
        }
        print(es_docs)
    except Exception as e:
        print(f"Error fetching documents from Elasticsearch index '{index_name}': {e}")

    # Determine rows to insert/update
    for row in source_rows:
        doc_id = "_".join(str(row[pk]) for pk in primary_keys)  # Create a unique ID using primary keys
        if doc_id not in es_docs or es_docs[doc_id] != row:
            try:
                elasticSearch.es.index(index=index_name, id=doc_id, document=row)
                print(f"Indexed document with ID {doc_id} in {index_name}")
            except Exception as e:
                print(f"Error indexing document {doc_id} in {index_name}: {e}")

    # Determine rows to delete
    source_primary_keys = {"_".join(str(row[pk]) for pk in primary_keys) for row in source_rows}
    docs_to_delete = [doc_id for doc_id in es_docs if doc_id not in source_primary_keys]

    for doc_id in docs_to_delete:
        try:
            elasticSearch.es.delete(index=index_name, id=doc_id)
            print(f"Deleted document with ID {doc_id} from {index_name}")
        except Exception as e:
            print(f"Error deleting document {doc_id} from {index_name}: {e}")

    print(f"Successfully synced table {table_name} to Elasticsearch index {index_name}.")

# def startIndexSyncJob():

#     schedule.every(5).seconds.do(sync_table_to_elasticsearch(databaseConfig.getReadDbInstance(),'event','event_index'))
#     print("Elasticsearch index sync job started.")
    
#     # Run the scheduler
#     while True:
#         schedule.run_pending()
#         time.sleep(1)  # Sleep to prevent busy-waiting


def startSyncJob():
    """
    Schedules the sync task to run every 5 minutes.
    """
    # Schedule the sync to run every 1 minutes
    schedule.every(5).seconds.do(syncData)
    schedule.every(5).seconds.do(syncIndexData)
    
    print("Sync job started.")
    
    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)  # Sleep to prevent busy-waiting