import os
import json
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType
import paho.mqtt.client as mqtt
import threading


KAFKA_HOSTS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
DB1_TOPIC_NAME = os.getenv("DB1_TOPIC_NAME")
DB2_TOPIC_NAME = os.getenv("DB2_TOPIC_NAME")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

# MQTT setup
mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
mqtt_client.loop_start()

# Kafka client setup
kafka_client = KafkaClient(hosts=KAFKA_HOSTS)

print(f"✅ Connected to Kafka: {KAFKA_HOSTS}")
print(f"📡 DB1 Topic: {DB1_TOPIC_NAME}")
print(f"📡 DB2 Topic: {DB2_TOPIC_NAME}")
print(f"📤 Forwarding to MQTT topic: {MQTT_TOPIC}")

def consume_topic(topic_name, db_identifier):
    """Consumer function for a specific topic"""
    try:
        topic = kafka_client.topics[topic_name.encode('utf-8')]
        consumer = topic.get_simple_consumer(
            consumer_group=f"ignition-json-consumer-{db_identifier.lower()}".encode('utf-8'),
            auto_offset_reset=OffsetType.LATEST,
            reset_offset_on_start=True,
            auto_commit_enable=True
        )
        
        print(f"✅ [{db_identifier}] Started consuming from topic: {topic_name}")
        
        for message in consumer:
            if message:
                try:
                    raw = json.loads(message.value.decode('utf-8'))
                    
                    # Add database identifier to the payload
                    enhanced_payload = {
                        "source_db": db_identifier,
                        "topic": topic_name,
                        "timestamp": int(time.time() * 1000),
                        "data": raw
                    }
                    
                    payload = json.dumps(enhanced_payload)
                    mqtt_client.publish(MQTT_TOPIC, payload)
                    print(f"📤 [{db_identifier}] Sent message to MQTT")
                except Exception as e:
                    print(f"⚠️ [{db_identifier}] Error processing message: {e}")
                    
    except Exception as e:
        print(f"❌ [{db_identifier}] Error in consumer: {e}")
    finally:
        if 'consumer' in locals():
            consumer.stop()

# Create threads for both database topics
threads = []

if DB1_TOPIC_NAME:
    db1_thread = threading.Thread(target=consume_topic, args=(DB1_TOPIC_NAME, "DB1"))
    db1_thread.daemon = True
    threads.append(db1_thread)

if DB2_TOPIC_NAME:
    db2_thread = threading.Thread(target=consume_topic, args=(DB2_TOPIC_NAME, "DB2"))
    db2_thread.daemon = True
    threads.append(db2_thread)

try:
    # Start all consumer threads
    for thread in threads:
        thread.start()
    
    print(f"🚀 Started {len(threads)} consumer threads")
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
        
except KeyboardInterrupt:
    print("🛑 Interrupted by user")
finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("✅ Cleanup completed")
