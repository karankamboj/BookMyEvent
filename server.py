from flask import Flask, request, jsonify
import spanner
from multiprocessing import Process

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello, World!"

@app.route('/fetch', methods=['GET'])
def fetchData():
    table_name = request.args.get('table_name')
    return spanner.fetchData(table_name)

@app.route('/insert', methods=['POST'])
def insertData():

    # Parse request JSON
    request_data = request.get_json()
    if not request_data:
        return jsonify({"error": "Invalid JSON payload"}), 400

    # Extract required fields
    table_name = request_data.get("table_name")
    columns = request_data.get("columns")
    values = request_data.get("values")
    
     # Validate the input
    if not table_name or not columns or not values:
        return jsonify({"error": "table_name, columns, and values are required"}), 400
    
    result = spanner.insertData(table_name, columns, values)
    return jsonify({"message": result}), 200 if "successfully" in result.lower() else 500

def run_flask_app(port):
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    # # Enable App Auto Restart : Debug mod
    # app.run(debug=True)

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