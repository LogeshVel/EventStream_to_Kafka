from aiokafka import AIOKafkaConsumer
import asyncio, json
import subprocess, os, sys
from prettytable import PrettyTable

t = PrettyTable()

KAFKA_SERVER = 'localhost:9092'

async def consume_msg(topic, group_id):
    try:
        t.field_names = ["Topic", "Partition", "Offset", "Key", "Timestamp ms", "Consumer Group"]
        if group_id == None:
            consumer = AIOKafkaConsumer(topic, value_deserializer=lambda x: json.loads(x), bootstrap_servers=KAFKA_SERVER, auto_offset_reset="earliest")
            print(f"Started Consuming message for topic : {topic}")
        else:
            consumer = AIOKafkaConsumer(topic, group_id=group_id, value_deserializer=lambda x: json.loads(x), bootstrap_servers=KAFKA_SERVER)
            print(f"Started Consuming message for topic : {topic}, Consumer Group : {group_id}")
    
        await consumer.start()
        async for msg in consumer:
            t.add_row([msg.topic, msg.partition, msg.offset, msg.key, msg.timestamp, consumer._group_id])
            subprocess.call("clear" if os.name == "posix" else "cls")
            print(t)
            print("\nLast Message")
            print(json.dumps(msg.value, indent=2))
            
    except KeyboardInterrupt as kint:
        print("Stop Consuming")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            topic = sys.argv[1]
            group_id = None
            
        elif len(sys.argv) == 3:
            topic = sys.argv[1]
            group_id = sys.argv[2]
            
        elif len(sys.argv) < 3:
            print("usage: consumer.py <topic> <group_id>")
            sys.exit(1)
        asyncio.run(consume_msg(topic, group_id))
    except KeyboardInterrupt as kint:
        print("Stopping Consumer")
    print("Done Consuming")

