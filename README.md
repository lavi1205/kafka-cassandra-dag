# Airflow DAG: End-to-End Data Processing Pipeline

This DAG orchestrates an end-to-end data processing pipeline using various tools like Kafka, Cassandra, and Airflow's PythonOperator. The pipeline fetches data from an API, formats the data, sends it to a Kafka topic, and stores it in a Cassandra database. This README provides an in-depth explanation of each component in the pipeline.

## Prerequisites

Before running this DAG, ensure you have the following components installed and configured:

- **Apache Airflow:** DAG scheduling and orchestration.
- **Kafka:** A distributed event streaming platform for sending and receiving data.
- **Cassandra:** A NoSQL database for storing structured data.
- **Boto3:** AWS SDK for Python to interact with Amazon S3 (though not used directly in this example).
- **Python Libraries:** `requests`, `pandas`, `kafka-python`, and `cassandra-driver`.

## DAG Overview

- **DAG Name:** `e2e_dags`
- **Owner:** Thinh Pham
- **Start Date:** January 1, 2024
- **Schedule Interval:** None (manual execution)
- **Retries:** 1

This DAG is designed for data processing. It fetches data from an external API, formats the data, sends it to a Kafka topic, and stores it in a Cassandra table.

## Task Breakdown

### 1. **Start Task**
   - **Task ID:** `start`
   - **Operator:** `DummyOperator`
   - **Purpose:** Acts as the starting point for the DAG. It does not perform any actions but is used to define the flow of tasks.

### 2. **Fetch API Data**
   - **Task ID:** `fetch_api`
   - **Operator:** `PythonOperator`
   - **Python Function:** `fetch_api`
   - **Purpose:** 
     - Fetches data from a specified API endpoint (`https://randomuser.me/api/`).
     - Extracts the first result from the API response and passes it to the next task via XCom.
   - **Key Points:** 
     - Error handling is implemented to catch and print any request exceptions.

### 3. **Format Data**
   - **Task ID:** `format_data`
   - **Operator:** `PythonOperator`
   - **Python Function:** `format_data`
   - **Purpose:** 
     - Extracts the raw data from the previous task using XCom.
     - Formats the data into a structured dictionary.
     - The dictionary contains user details such as name, address, email, and more.
   - **Key Points:** 
     - Data formatting is done based on the JSON structure received from the API.
     - The formatted data is passed to subsequent tasks via XCom.

### 4. **Kafka Operations Task Group**
   - **Task Group Name:** `kafka_operations`
   - **Purpose:** This task group handles all Kafka-related operations, including topic creation and data sending.

#### a. **Create Kafka Topic**
   - **Task ID:** `create_kafka_topic`
   - **Operator:** `PythonOperator`
   - **Python Function:** `create_kafka_topic`
   - **Purpose:** 
     - Creates a Kafka topic named `user_data_topic` if it does not already exist.
     - This topic is used to send and receive user data.
   - **Key Points:** 
     - The topic is created using the KafkaAdminClient.
     - If the topic already exists, the task will print a message and proceed without error.

#### b. **Send Data to Kafka**
   - **Task ID:** `send_data_to_kafka`
   - **Operator:** `PythonOperator`
   - **Python Function:** `send_data_to_kafka`
   - **Purpose:** 
     - Sends the formatted user data to the Kafka topic (`user_data_topic`).
     - Data is sent continuously for one minute with a delay of 5 seconds between each message.
   - **Key Points:** 
     - Data is serialized to JSON format before sending.
     - The KafkaProducer is used to send messages to the topic.

### 5. **Cassandra Operations Task Group**
   - **Task Group Name:** `cassandra_operations`
   - **Purpose:** This task group handles operations related to Cassandra, including table creation and data insertion.

#### a. **Create Cassandra Table**
   - **Task ID:** `create_cassandra_table`
   - **Operator:** `PythonOperator`
   - **Python Function:** `create_cassandra_table`
   - **Purpose:** 
     - Creates a keyspace (`airflow_keyspace`) and a table (`user_data`) in Cassandra.
     - The table schema is designed to store user data fetched from the API.
   - **Key Points:** 
     - The task first drops the old table if it exists and then creates a new table with the desired schema.

#### b. **Save Data to Cassandra**
   - **Task ID:** `save_to_cassandra`
   - **Operator:** `PythonOperator`
   - **Python Function:** `save_to_cassandra`
   - **Purpose:** 
     - Inserts the formatted user data into the Cassandra table (`user_data`).
     - Ensures that the `postcode` field is stored as an integer.
   - **Key Points:** 
     - The data is converted to a Pandas DataFrame before insertion.
     - The `postcode` field is explicitly converted to an integer to avoid data type issues.

### 6. **End Task**
   - **Task ID:** `end`
   - **Operator:** `DummyOperator`
   - **Purpose:** Marks the end of the DAG. Similar to the start task, it does not perform any actions but is used to signify the completion of the pipeline.

## Task Dependencies

The task dependencies are set up as follows:

```
start_task >> fetch_task >> format_task >> kafka_group >> cassandra_operations >> end_task
```

- **Start Task:** Triggers the execution of the pipeline.
- **Fetch Task:** Fetches data from the API and passes it to the next task.
- **Format Task:** Formats the fetched data.
- **Kafka Group:** Handles Kafka topic creation and data sending.
- **Cassandra Operations:** Creates a Cassandra table and saves the formatted data.
- **End Task:** Marks the completion of the pipeline.

## How to Run the DAG

1. **Ensure all dependencies are installed:**
   - Install the required Python libraries.
   - Set up Kafka and Cassandra instances.

2. **Add the DAG file to Airflow:**
   - Place the DAG Python script in your Airflow DAGs directory.

3. **Trigger the DAG:**
   - You can trigger the DAG manually via the Airflow web interface or CLI.

4. **Monitor the DAG:**
   - Monitor the execution and logs through the Airflow web interface.

## Conclusion

This DAG provides a robust framework for fetching, processing, and storing data using Airflow, Kafka, and Cassandra. It is highly customizable and can be extended to include more complex data processing tasks as needed.