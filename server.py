from flask import Flask, request, jsonify
import spanner

app = Flask(__name__)

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

if __name__ == "__main__":
    app.run()