import json
import configparser
import logging
import logging.config
from datetime import datetime
from kafka import KafkaConsumer


DEFAULT_CONFIG_SECTION='DEFAULT'
config_file_path='/home/ebedber/bgf_test/data_export/config.ini'


def load_logging_config(config):
    log_config_file_path = str(config[DEFAULT_CONFIG_SECTION]['logger_config_file_path'])
    with open(log_config_file_path, 'r') as config_file:
        logging_config = json.load(config_file)
    logging.config.dictConfig(logging_config)
    global logger
    logger = logging.getLogger()


def is_json(obj):
    try:
        json_obj=json.loads(obj)
        return True, json_obj
    except ValueError:
        return False


def save_last_run(config, date):
    default_config = config[DEFAULT_CONFIG_SECTION]

    default_config['timestamp'] = str(date)

    with open(config_file_path, 'w') as config_file:
        config.write(config_file)


def load_last_run(config):
    timestamp_str = str(config[DEFAULT_CONFIG_SECTION]['timestamp'])
    return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")


def read_json_from_kafka(topic, config, bootstrap_servers='localhost:9092'):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

    last_run_time = load_last_run(config)
    if last_run_time:
        start_timestamp = last_run_time
    else:
        start_timestamp = datetime(2023,5,1)

    end_timestamp = datetime.now()
    logger.info(f'[PROCESSING] Starting export from {start_timestamp} to {end_timestamp}')

    output_file_path = str(config[DEFAULT_CONFIG_SECTION]['output_file_path'])
    with open(output_file_path,'a') as file:
        messages = consumer.poll(timeout_ms=6000)  # Adjust the timeout as needed
        invalid_event_count = valid_event_count = 0

        for topic_partition, records in messages.items():
            for record in records:
                message_time = datetime.fromtimestamp(record.timestamp / 1000.0)

                if start_timestamp <= message_time <= end_timestamp:
                    is_json_tuple=is_json(record.value)
                    if isinstance(is_json_tuple, tuple) and is_json_tuple[0]:
                        json_data = is_json_tuple[1]
                        json.dump(json_data, file)
                        file.write('\n')
                        valid_event_count+=1
                        # print(f"Received json message at {message_time}: {record.value}")
                    else:
                        invalid_event_count+=1


    logger.info(f'[PROCESSING] Processing finished: Invalid event count: {invalid_event_count} | valid event count" {valid_event_count}')
    consumer.close()
    save_last_run(config, end_timestamp)


def export_data_from_kafka(config):
    topic=str(config[DEFAULT_CONFIG_SECTION]['topic'])
    kafka_server=str(config[DEFAULT_CONFIG_SECTION]['bootstrap_server'])
    read_json_from_kafka(topic, config, kafka_server)


if __name__ == '__main__':
    config=configparser.ConfigParser()
    config.read(config_file_path)
    load_logging_config(config)

    try:
        export_data_from_kafka(config)
    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
