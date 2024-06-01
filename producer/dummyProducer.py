import csv
from kafka import KafkaProducer
import json
import os

# Function to read data from a file
def read_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) == 2:  # Expecting two columns: MAC address and RSSI value
                mac_address, rssi = row
                data.append({'mac_address': mac_address, 'rssi': rssi})
    return data

# Function to send data to Kafka
def send_to_kafka(data, kafka_topic, kafka_server):
    producer = KafkaProducer(bootstrap_servers=kafka_server,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for record in data:
        producer.send(kafka_topic, record)
    producer.flush()

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'macs.csv')

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"The file {file_path} does not exist in the directory {os.getcwd()}")
        return
    
    kafka_topic = 'macs'  # Update this to your Kafka topic
    kafka_server = 'contabo_server:9092'  # Update this to your Kafka server

    data = read_data(file_path)
    send_to_kafka(data, kafka_topic, kafka_server)

if __name__ == '__main__':
    main()
    print("Data sent to Kafka successfully.")
