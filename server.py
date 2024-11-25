from flask import Flask, request, jsonify
import spanner

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello, World!"

@app.route('/fetch', methods=['GET'])
def fetchData():
    table_name = request.args.get('table_name')
    db = spanner.connectDatabase()
    return spanner.fetchData(db, table_name)

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
    
    db = spanner.connectDatabase()
    result = spanner.insertData(db, table_name, columns, values)
    return jsonify({"message": result}), 200 if "successfully" in result.lower() else 500

if __name__ == "__main__":
    app.run()