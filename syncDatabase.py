from google.cloud import spanner

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
        print(f"Columns for {table_name}: {columns}")


        # Fetch data from the source table
        with source_db.snapshot() as snapshot:
            data_query = f"SELECT * FROM {table_name}"
            rows = list(snapshot.execute_sql(data_query))
            print(f"Fetched {len(rows)} rows from {table_name}")

        
        if rows:
            for row in rows:
                row_dict = dict(zip(columns, row))
                data_to_insert = [tuple(row_dict[col] for col in columns)]  # Create a tuple for each row
                try:
                    with target_db.batch() as batch:
                        batch.insert(
                            table=table_name,
                            columns=columns,
                            values=data_to_insert,
                        )
                except Exception as e:
                    print(e)
                print(f"Inserted row into {table_name}: {row_dict}")
            print(f"Successfully synced {len(rows)} rows to table {table_name}.")
        else:
            print(f"No rows to sync for table {table_name}.")


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
