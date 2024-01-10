This Python script fetches comments from a YouTube video using the YouTube Data API, paginates through the comments,
and stores them in a MongoDB database. It maintains a global counter for comment numbers, extracts comment text, and 
inserts the data into MongoDB. The script is designed for flexibility with different video IDs and names. 
It's important to set up a valid YouTube API key (DEVELOPER_KEY) and ensure MongoDB is running on localhost:27017.



This Python script connects to a MongoDB database, retrieves a random document from a collection named 'landslide', 
and sends that document to a Kafka topic named 'demo_test' using a Kafka producer. The script runs in an infinite loop,
handling errors by printing error messages and retrying every 5 seconds. 
It's designed to continuously stream data from MongoDB to Kafka for potential consumption by other applications subscribing 
to the 'demo_test' topic.


#This Python script consumes messages from a Kafka topic ('demo_test'), processes each message,
and writes the data to both Amazon S3 and HDFS. It uses the KafkaConsumer to fetch messages,
S3FileSystem to interact with Amazon S3, and InsecureClient for HDFS. For each Kafka message received,
it writes the data to unique S3 and HDFS locations, 
with error handling and a 1-second delay between processing messages to avoid excessive consumption. 
The script essentially acts as a connector, enabling the storage of Kafka messages in distributed storage systems.



This script sets up an Apache Airflow DAG for sentiment analysis on YouTube comments. 
It uses PySpark and NLTK's VADER for analysis, processes data from HDFS, and writes results to both CSV and AWS Redshift. 
The Airflow DAG is scheduled to run weekly.
