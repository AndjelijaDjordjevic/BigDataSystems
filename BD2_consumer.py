import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster


def analysis(input_data, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max, cassandra_session):
    data_split = input_data.map(lambda x: x[1].split(','))
    data_formated = data_split.map(lambda x: list(map(lambda y: float(y), x[:])))

    data_filtered = data_formated.filter(lambda x: latitude_min < x[0] < latitude_max and longitude_min < x[1] < longitude_max
                                                 and time_min < x[5] < time_max)
    data_filtered.cache()

    count = data_filtered.count()
    print("Total number of elements: " + str(count) + ".")
    filtered_walking = data_filtered.filter(lambda x: x[2] == 2.0)

    walking = filtered_walking.count()
    print("Walking time " + str(walking) + " sec.")

    temp_min = data_filtered.map(lambda x: x[4]).min()
    temp_max = data_filtered.map(lambda x: x[4]).max()
    temp_mean = data_filtered.map(lambda x: x[4]).mean()
    temp_std = data_filtered.map(lambda x: x[4]).stdev()
    print("Temperature min=" + str(temp_min) + " max=" + str(temp_max) + " mean=" + str(temp_mean) + " std=" +
          str(temp_std))

    pressure_min = data_filtered.map(lambda x: x[3]).min()
    pressure_max = data_filtered.map(lambda x: x[3]).max()
    pressure_mean = data_filtered.map(lambda x: x[3]).mean()
    pressure_std = data_filtered.map(lambda x: x[3]).stdev()
    print("Pressure min=" + str(pressure_min) + " max=" + str(pressure_max) + " mean=" + str(pressure_mean) +
          " std=" + str(pressure_std))

    cassandra_session.execute("INSERT INTO cass.Analysis_info (attribute, timestamp, elements, min, max, mean, std, "
                              "value) VALUES (%s, toTimeStamp(now()), %s, %s, %s, %s, %s, %s)", ("walking", count, 0, 0,
                                                                                                 0, 0, walking))

    cassandra_session.execute("INSERT INTO cass.Analysis_info (attribute, timestamp, elements, min, max, mean, std,"
                              " value) VALUES (%s, toTimeStamp(now()), %s, %s, %s, %s, %s, %s)", ("temperature", count,
                                                                                                  temp_min, temp_max,
                                                                                                  temp_mean, temp_std,
                                                                                                  0))

    cassandra_session.execute("INSERT INTO cass.Analysis_info (attribute, timestamp, elements, min, max, mean, std,"
                              " value) VALUES (%s, toTimeStamp(now()), %s, %s, %s, %s, %s, %s)", ("pressure", count,
                                                                                                  pressure_min,
                                                                                                  pressure_max,
                                                                                                  pressure_mean,
                                                                                                  pressure_std, 0))


def main(bootstrap_server, topic_name, time_interval, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max):

    cassandra_cluster = Cluster()
    cassandra_session = cassandra_cluster.connect('cass', wait_for_all_pools=True)
    configuration = SparkConf().setAppName("BigDataProj2")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")

    streaming_context = StreamingContext(context, time_interval)
    stream = KafkaUtils.createDirectStream(streaming_context, [topic_name], {"metadata.broker.list": bootstrap_server})
    stream.foreachRDD(lambda input_data: analysis(input_data, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max,
                                                  cassandra_session))

    streaming_context.start()
    streaming_context.awaitTermination()


if __name__ == "__main__":
    if len(sys.argv) != 10:
        print("\nBad command syntax. The correct parameter syntax is:")
        print("bootstrap_server, topic_name, time_interval, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max\n")
    else:
        good = True
        try:
            bootstrap_server = sys.argv[1]
            topic_name = sys.argv[2]
            time_interval = float(sys.argv[3])
            latitude_min = float(sys.argv[4])
            latitude_max = float(sys.argv[5])
            longitude_min = float(sys.argv[6])
            longitude_max = float(sys.argv[7])
            time_min = float(sys.argv[8])
            time_max = float(sys.argv[9])
        except ValueError:
            print("\nInvalid parameters.\n")
            good = False
        if good:
            main(bootstrap_server, topic_name, time_interval, latitude_min, latitude_max, longitude_min, longitude_max, time_min, time_max)
