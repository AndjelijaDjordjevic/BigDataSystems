import sys
from glob import glob
from kafka import KafkaProducer


def main(input_path, bootstrap_server, topic_name):

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=lambda x: x.encode("utf-8"),
                                   api_version=(0, 1, 0))

    for file_path in glob(input_path + "/*.csv"):
        with open(file_path, 'r') as opened_file:
            for line in opened_file:
                info = line.strip()
                split_info = info.split(',')
                n = len(split_info)
                if n == 0:
                    continue
                if "time" not in split_info[n-1].lower():
                    kafka_producer.send(topic_name, value=info)
                    kafka_producer.flush()
                    print(info)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("\nBad command syntax. The correct parameter syntax is:")
        print("input_path bootstrap_server topic_name\n")
    else:
        input_path = sys.argv[1]
        bootstrap_server = sys.argv[2]
        topic_name = sys.argv[3]
        main(input_path, bootstrap_server, topic_name)
