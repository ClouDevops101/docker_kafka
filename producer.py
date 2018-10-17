from kafka import SimpleProducer, KafkaClient
import avro.schema
import io, random
from avro.io import DatumWriter
from time import gmtime, strftime
 
# To send messages synchronously
kafka = KafkaClient('172.20.10.2:9092')
producer = SimpleProducer(kafka)

# Kafka topic
topic = "furtive"
 
# Path to user.avsc avro schema
schema_path="user.avsc"
schema = avro.schema.parse(open(schema_path).read())
ts = strftime("%Y-%m-%d %H:%M:%S", gmtime()) 
mesg ="hello"
for i in xrange(10):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"date": ts , "name": "123", "favorite_color": "111", "favorite_number": random.randint(20,100)}, encoder)
    raw_bytes = bytes_writer.getvalue()
    
    producer.send_messages(topic, raw_bytes)
    #producer.send_messages(topic, mesg)
