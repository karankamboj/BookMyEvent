from elasticsearch import Elasticsearch
from google.cloud import spanner
import databaseConfig
import os

username = os.getenv("ELASTICSEARCH_USERNAME")
password = os.getenv("ELASTICSEARCH_PASSWORD")

# Initialize Elasticsearch client
es = Elasticsearch('https://localhost:9200', basic_auth=(username, password), verify_certs=False)  # Replace with your Elasticsearch URL

# Sync Spanner data to Elasticsearch
def index_spanner_data():
    database = databaseConfig.getReadDbInstance()

    with database.snapshot() as snapshot:
        # Query all events
        query = "SELECT * FROM event"
        results = snapshot.execute_sql(query)

        for row in results:
            doc = {
                "event_id": row[0],
                "name": row[1],
                "location_id": row[2],
                "date_time": row[3],
                "category": row[4],
                "total_tickets": row[5],
                "available_tickets": row[6],
            }            
            # Index data in Elasticsearch
            es.index(index="event_index", id=row[0], body=doc)

if __name__ == "__main__":
    index_spanner_data()
