from flask import Flask, request, jsonify
import threading
import spanner
from multiprocessing import Process
import syncDatabase
import elasticSearch

app = Flask(__name__)

def run_sync_task():
    """
    Runs the sync task in the background.
    This will keep syncing data from the write DB to the read-only DB every 5 minutes.
    """
    syncDatabase.startSyncJob()  # Starts the sync job which runs every 5 minutes
    # syncDatabase.startIndexSyncJob()
    
def before_first_request():
    """
    This function is called before the first request to the Flask app.
    We will start the sync task here in a background thread.
    """
    print("Starting sync task in background...")
    sync_thread = threading.Thread(target=run_sync_task)
    sync_thread.daemon = True  # Ensure that the thread exits when the main program exits
    sync_thread.start()

@app.route("/")
def hello():
    return "Hello, World!"

@app.route('/fetch', methods=['GET'])
def fetchData():
    table_name = request.args.get('table_name')
    return spanner.fetchData(table_name)

def validateInsertRequest(request_data):
    if not request_data:
        raise Exception({"error": "Invalid JSON payload"})

    # Extract required fields
    table_name = request_data.get("table_name")
    columns = request_data.get("columns")
    values = request_data.get("values")
    
     # Validate the input
    if not table_name or not columns or not values:
        raise Exception({"error": "table_name, columns, and values are required"})
    return table_name, columns, values

@app.route('/insert', methods=['POST'])
def insertData():

    # Parse request JSON
    request_data = request.get_json()
    table_name, columns, values = validateInsertRequest(request_data)
    
    result = spanner.insertData(table_name, columns, values)
    return jsonify({"message": result}), 200 if "successfully" in result.lower() else 500


def validateUpdateRequest(request_data):
    if not request_data:
        raise Exception({"error": "Invalid JSON payload"})
    
    # Extract required fields
    table_name = request_data.get("table_name")
    columns = request_data.get("columns")
    values = request_data.get("values")
    primary_col = request_data.get("primary_column")
    primary_val = request_data.get("primary_value")
    
    # Validate the input
    if not table_name or not columns or not values or not primary_col or not primary_val:
        raise Exception({"error": "table_name, columns, values, primary_column, and primary_value are required"})
    
    return table_name, columns, values, primary_col, primary_val

@app.route('/update', methods=['PUT'])
def updateData():
    # Parse request JSON
    request_data = request.get_json()
    table_name, columns, values, primary_col, primary_val = validateUpdateRequest(request_data)

    # Update the data in the database
    result = spanner.updateData(table_name, columns, values, primary_col, primary_val)
    return jsonify({"message": result}), 200 if "successfully" in result.lower() else 500


def validateDeleteRequest(request_data):
    if not request_data:
        raise Exception({"error": "Invalid JSON payload"})
    
    # Extract required fields
    table_name = request_data.get("table_name")
    primary_col = request_data.get("primary_column")
    primary_val = request_data.get("primary_value")

    # Validate the input
    if not table_name or not primary_col or not primary_val:
        raise Exception({"error": "table_name, primary_column, and primary_value are required"})

    return table_name, primary_col, primary_val

@app.route('/delete', methods=['DELETE'])
def deleteData():
    # Parse request JSON
    request_data = request.get_json()
    table_name, primary_col, primary_val = validateDeleteRequest(request_data)

    # Delete the data from the database
    result = spanner.deleteData(table_name, primary_col, primary_val)

    return jsonify({"message": result}), 200 if "successfully" in result.lower() else 500


def validateSearchRequest(request_data):
    if not request_data:
        raise Exception({"error": "Invalid JSON payload"})
    
    # Extract required fields
    query = request_data.get("query")
    location_id = request_data.get("location_id")
    category = request_data.get("category")
    min_tickets = request_data.get("min_tickets", 0)
    max_tickets = request_data.get("max_tickets", None)
    date_time = request_data.get("date_time")


    # Validate the input
    if not query:
        raise Exception({"error": "at least query param is required! Additional filters; location_id, category, min_tickets, max_tickets, and date_time can be passed."})

    return query, location_id, category, min_tickets, max_tickets, date_time

@app.route('/search', methods=['GET'])
def search_events():
    try:
        request_data = request.get_json()
        search_query, location_id, category, min_tickets, max_tickets, date_time = validateSearchRequest(request_data)

        print(f"search_query:{search_query}")

        # Build Elasticsearch query
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {"multi_match": {"query": search_query, "fields": ["name", "category"]}}
                    ],
                    "filter": []
                }
            }
        }

        # Add filters if provided
        if location_id:
            query_body["query"]["bool"]["filter"].append({"term": {"location_id": location_id}})
        if category:
            query_body["query"]["bool"]["filter"].append({"term": {"category": category}})
        if min_tickets:
            query_body["query"]["bool"]["filter"].append({
                "range": {"available_tickets": {"gte": int(min_tickets)}}
            })
        if max_tickets:
            query_body["query"]["bool"]["filter"].append({
                "range": {"available_tickets": {"lte": int(max_tickets)}}
            })
        if date_time:
            query_body["query"]["bool"]["filter"].append({
                "range": {"date_time": {"gte": date_time}}
            })

        # Execute search
        response = elasticSearch.es.search(index="event_index", body=query_body)

        # Extract results
        results = [hit["_source"] for hit in response["hits"]["hits"]]
        return jsonify({"results": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
def run_flask_app(port):
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    # # Enable App Auto Restart : Debug mod
    # app.run(debug=True)
    before_first_request()

    # Create separate processes for each Flask instance on different ports
    process1 = Process(target=run_flask_app, args=(5001,))
    process2 = Process(target=run_flask_app, args=(5002,))
    process3 = Process(target=run_flask_app, args=(5003,))

    # Start all processes
    process1.start()
    process2.start()
    process3.start()

    # Join all processes to ensure they keep running
    process1.join()
    process2.join()
    process3.join()