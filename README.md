# BookMyEvent

## Starting up Flask server
#### Ensure Python is installed. Then, install:
pip install flask
pip install flask_cors
pip install google-cloud-spanner
pip install spanner
pip install elasticsearch
pip install schedule

## Starting up Elasticserach server

### Download ElasticSearch and unzip by following the link.
https://www.elastic.co/downloads/elasticsearch

### Start Elaticsearch server
cd "your_elaticsearch_directory"

#### For Linux/MacOS
bin/elasticsearch 

#### For Windows
bin\elasticsearch.bat 

### Set Environment Variables to Authenticate Elasticsearch

#### For Linux/MacOS
export ELASTICSEARCH_USERNAME="your_username"<br>
export ELASTICSEARCH_PASSWORD="your_password"

#### For Windows
set ELASTICSEARCH_USERNAME=your_username<br>
set ELASTICSEARCH_PASSWORD=your_password

## Running the flask app
### Clone the github repository 
git clone "https://github.com/karankamboj/BookMyEvent.git"

### Run server.py
Navigate to reporsitory folder and run:<br>
python server.py

### Open BookMyEvent UI
Open public/index.html file

## Alternatively, use Postman to send REST request
### Install Postman
https://www.postman.com/downloads/

### Sample REST requests
#### Fetch
curl --location 'http://127.0.0.1:5002/fetch?table_name=event'

#### Update
curl --location --request PUT 'http://127.0.0.1:5001/update' \
--header 'Content-Type: application/json' \
--data '{
    "table_name": "event",
    "columns": ["name"],
    "values": ["Diljit Concert Updated"],
    "primary_column":"event_id",
    "primary_value":"2"
}'

#### Insert
curl --location 'http://127.0.0.1:80/insert' \
--header 'Content-Type: application/json' \
--data '{
    "table_name": "users",
    "columns": ["user_id", "name", "email", "phone_number", "region", "created_at"],
    "values": [
          [13, "Shan", "bob@example.com", "987-654-3210", "Europe", "'"$created_at_2"'"]
    ]
}'
         
##### Delete
curl --location --request DELETE 'http://127.0.0.1:5001/delete' \
--header 'Content-Type: application/json' \
--data '{
    "table_name": "event",
    "primary_column":"event_id",
    "primary_value":"1"
}'

#### Search
curl --location 'http://127.0.0.1:5001/search' \
--header 'Content-Type: application/json' \
--data '{
    "query": "Music",
    "location_id": 1
}'


## To run the Locust performance tests:

### 1. Install Locust:
   pip install locust

### 2. Run Locust:
   locust 

### 3. Open Locust web interface:
   http://localhost:8089

### 4. Configure test parameters:
   - Number of total users to simulate
   - Spawn rate (users per second)
   - Host URL
