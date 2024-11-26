from flask import Flask, request, jsonify
import threading
import spanner
from multiprocessing import Process
import syncDatabase

app = Flask(__name__)

def run_sync_task():
    """
    Runs the sync task in the background.
    This will keep syncing data from the write DB to the read-only DB every 5 minutes.
    """
    syncDatabase.startSyncJob()  # Starts the sync job which runs every 5 minutes
    
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