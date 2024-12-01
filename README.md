# BookMyEvent

## Starting up Flask server
### Ensure Python is installed. Then, install Flask:
pip install flask

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

### Navigate to reporsitory folder and run the server.py file
python server.py
