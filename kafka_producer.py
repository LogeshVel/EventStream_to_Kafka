# Produce the Kafak message from the EventStream 
from sseclient import SSEClient
import json
from aiokafka import AIOKafkaProducer
import asyncio

event_response = SSEClient("https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_TOPIC = "wikimedia.recentchange"
KAFKA_SERVER = 'localhost:9092'

async def produce_msg():
    print("Started Producing, by getting the EventStreams")
    try:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        await producer.start()
        for event in event_response:
            if event.data is None or event.data == "":
                print("No data got from EventStream")
                continue
            try:
                json_data = json.loads(event.data)
                json_event_id = json.loads(event.id)
            except json.decoder.JSONDecodeError as jse:
                print("Event response data is not JSON decodeable.")
                continue
            produce_data = {"uri": json_data["meta"].get("uri",""),"id": json_data.get("id",""), "type":json_data.get("type",""), "title": json_data.get("title",""), "user": json_data.get("user",""), "bot": json_data.get("bot",""), "minor": json_data.get("minor",""), "server_name": json_data.get("server_name",""), "event_ids": json_event_id}
            print(produce_data)
            
            await producer.send_and_wait(KAFKA_TOPIC, produce_data)
    except KeyboardInterrupt as kint:
        print("Stop producing")
    finally:
        await producer.stop()

asyncio.run(produce_msg())

print("Done producing")


