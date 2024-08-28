from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import boto3
import json, requests
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import time
import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider

s3 = boto3.client('s3')

# Define the default arguments
default_args = {
    'owner': 'thinh pham', 
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'schedule_interval': 'None'
}

# Define the DAG
dag = DAG(
    'e2e_dags',
    default_args=default_args,
    description='DAG using for data processing',
    catchup=False,  # Set to False to avoid backfilling
)

def fetch_api(url):
    try:
        data = requests.get(url).json()
        result = data['results'][0]
        print('Fetching API Success')
        return result
    except requests.exceptions.RequestException as err:
        print("Request error. Exception: ", err)

def format_data(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='fetch_api', key='return_value')
    print(f'Result get from xcom_pull {result}')
    data = {}
    street_number = result['location']['street']['number']
    street_name = result['location']['street']['name']
    city = result['location']['city']
    state = result['location']['state']
    country = result['location']['country']
    postcode = result['location']['postcode']
    address = f'{street_number} {street_name}, {city}, {state}, {country}'
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['gender'] = result['gender']
    data['address'] = address
    data['postcode'] = postcode
    data['email'] = result['email']
    data['dob'] = result['dob']['date']
    data['registered_data'] = result['registered']['date']
    data['phone'] = result['phone']
    data['picture'] = result['picture']['medium']
    return data

def create_kafka_topic(topic_name):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers='broker:9092', client_id='airflow_client')
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        existing_topics = admin_client.list_topics()
        
        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        else:
            print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Failed to create Kafka topic. Exception: {e}")

def send_data_to_kafka(topic_name, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='format_data', key='return_value')
    
    producer = KafkaProducer(bootstrap_servers='broker:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    start_time = time.time()
    while time.time() - start_time < 60:  # Send data for 1 minute
        producer.send(topic_name, value=data)
        print(f"Data sent to Kafka topic '{topic_name}': {data}")
        time.sleep(5)  # Simulate some delay between messages
    
    producer.flush()
    producer.close()
    print(f"Finished sending data to Kafka topic '{topic_name}'.")

# Function to check and create Cassandra table
def create_cassandra_table():
    protocol_version = 4
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra' )
    cluster = Cluster(['cassandra'],port=9042, auth_provider = auth_provider, protocol_version = protocol_version)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS airflow_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('airflow_keyspace')
   # Drop the old table if it exists
    session.execute("""
    DROP TABLE IF EXISTS user_data;
    """)
    
    # Create a new table with the desired schema
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_data (
            id UUID PRIMARY KEY,
            first_name text,
            last_name text,
            gender text,
            address text,
            postcode int,
            email text,
            dob text
        )
    """)
    print("Cassandra table created or verified successfully.")

# Function to save data to Cassandra
def save_to_cassandra(**kwargs):
    ti = kwargs['ti']
    formatted_data = ti.xcom_pull(task_ids='format_data', key='return_value')

    # Convert the dictionary to a DataFrame
    df = pd.DataFrame([formatted_data])

    # Ensure that 'postcode' is always an integer
    df['postcode'] = pd.to_numeric(df['postcode'], errors='coerce').fillna(0).astype(int)

    print(f"DataFrame after convert: \n{df}")

    # Connect to Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect('airflow_keyspace')

    # Insert data into the Cassandra table
    for _, row in df.iterrows():
        session.execute("""
            INSERT INTO user_data (id, first_name, last_name, gender, address, postcode, email, dob)
            VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s)
        """, (row['first_name'], row['last_name'], row['gender'], row['address'], row['postcode'], row['email'], row['dob']))

    print("Data inserted into Cassandra successfully.")

# Define the task group for Cassandra operations
with TaskGroup("cassandra_operations", dag=dag) as cassandra_operations:
    create_table = PythonOperator(
        task_id='create_cassandra_table',
        python_callable=create_cassandra_table,
        dag=dag,
    )

    save_data = PythonOperator(
        task_id='save_to_cassandra',
        python_callable=save_to_cassandra,
        dag=dag,
        provide_context=True,
    )

    create_table >> save_data

# Define the tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

fetch_task = PythonOperator(
    task_id="fetch_api", 
    python_callable=fetch_api, 
    dag=dag,
    op_kwargs={'url': 'https://randomuser.me/api/'}
)

format_task = PythonOperator(
    task_id="format_data", 
    python_callable=format_data, 
    dag=dag
)

# Create a TaskGroup for Kafka operations
with TaskGroup("kafka_operations", dag=dag) as kafka_group:
    create_topic_task = PythonOperator(
        task_id="create_kafka_topic", 
        python_callable=create_kafka_topic, 
        dag=dag,
        op_kwargs={'topic_name': 'user_data_topic'}  # Choose a topic name
    )

    send_data_task = PythonOperator(
        task_id="send_data_to_kafka", 
        python_callable=send_data_to_kafka, 
        dag=dag,
        op_kwargs={'topic_name': 'user_data_topic'}
    )
    create_topic_task >> send_data_task


# Define task dependencies
start_task >> fetch_task >> format_task >> kafka_group >> cassandra_operations >> end_task
