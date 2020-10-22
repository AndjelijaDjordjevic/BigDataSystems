from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['0.0.0.0'])
session = cluster.connect()
session.execute("CREATE KEYSPACE cass WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
session.execute('USE cass')
session.execute("CREATE TABLE cass.Analysis_Info(attribute text, timestamp timestamp, elements int, min float, max float, mean float, std float, value float, PRIMARY KEY (attribute, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);");