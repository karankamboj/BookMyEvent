from flask import Flask, request, jsonify
import threading
import spanner
<<<<<<< Updated upstream
=======
from multiprocessing import Process
import SyncHandler
>>>>>>> Stashed changes

app = Flask(__name__)

def run_sync_task():
    """
    Runs the sync task in the background.
    This will keep syncing data from the write DB to the read-only DB every 5 minutes.
    """
    SyncHandler.startSyncJob()  # Starts the sync job which runs every 5 minutes

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
    db = spanner.connectDatabase()
    return spanner.fetchData(db)

@app.route('/insert', methods=['POST'])
def insertData():

    # Get data from body of request
    data = request.get_json()
    print(data)

    if not data or not isinstance(data, list) or not all(isinstance(item, list) for item in data):
        return jsonify({"error": "Invalid data format. Expected a list of lists."}), 400
    
    db = spanner.connectDatabase()
    return spanner.insertData(db, data)

<<<<<<< Updated upstream
if __name__ == "__main__":
    app.run()
=======
def run_flask_app(port):
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    before_first_request()
    # Enable App Auto Restart : Debug mod
    app.run()

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
>>>>>>> Stashed changes
