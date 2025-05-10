import os
from pykafka import KafkaClient
from pykafka.common import OffsetType
import paho.mqtt.client as mqtt
import json
import ssl
import time

# Config from environment
KAFKA_HOSTS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MQTT_BROKER = os.getenv("MQTT_BROKER", "e2cf4dc097804f4889e07d9a50208781.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "anish")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Developer1")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "streamsql/data")
TOPIC_NAME = os.getenv("TOPIC_NAME", "TuringMachine.dbo.Customers")

print(f"🔄 Kafka Bootstrap Servers: {KAFKA_HOSTS}"
      f"\n🔄 MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}"
      f"\n🔄 MQTT Topic: {MQTT_TOPIC}"
      f"\n🔄 Kafka Topic: {TOPIC_NAME}"
      f"\n🔄 MQTT Username: {MQTT_USERNAME}"
      f"\n🔄 MQTT Password: {MQTT_PASSWORD}"
      f"\n🔄 MQTT TLS Version: {ssl.PROTOCOL_TLS}"
      )

# MQTT Configuration
# MQTT_BROKER = 'e2cf4dc097804f4889e07d9a50208781.s1.eu.hivemq.cloud'
# MQTT_PORT = 8883
# MQTT_USERNAME = 'anish'
# MQTT_PASSWORD = 'Developer1'
# MQTT_TOPIC = 'streamsql/data'

# MQTT Callbacks
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"🔗 Connected to MQTT Broker with reason code: {reason_code}")

def on_publish(client, userdata, mid, reason_code, properties):
    print(f"📤 Message published with ID: {mid}")

# Set up MQTT client
mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)
mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish

print("🔄 Connecting to MQTT broker...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
mqtt_client.loop_start()

# Wait for MQTT connection
time.sleep(1)

# Connect to Kafka broker
print(f"🔄 Connecting to Kafka broker at {KAFKA_HOSTS}...")
client = KafkaClient(hosts=KAFKA_HOSTS)

# Choose your topic
# topic_name = "TuringMachine.dbo.Customers"  # Replace with your Kafka topic name
topic_name = TOPIC_NAME
print(f"📋 Available topics: {[t.decode('utf-8') for t in client.topics.keys()]}")
topic = client.topics[topic_name.encode('utf-8')]

# Create a consumer
consumer = topic.get_simple_consumer(
    consumer_group=b"mqtt-bridge-consumer",
    auto_offset_reset=OffsetType.EARLIEST,  # Or OffsetType.LATEST
    reset_offset_on_start=True,
    auto_commit_enable=True
)

print(f"✅ Consuming from Kafka topic '{topic_name}' and forwarding to MQTT topic '{MQTT_TOPIC}'")
print("⏱️ Waiting for messages...")

# Consume messages
try:
    for message in consumer:
        if message is not None:
            # Get message value and try to parse as JSON
            try:
                # Try to decode and parse as JSON
                message_value = message.value.decode('utf-8')
                json_data = json.loads(message_value)
                
                # Pretty print for debugging
                pretty_json = json.dumps(json_data, indent=2)
                print(f"\n📥 Received message from Kafka:")
                print(f"📌 Offset: {message.offset}")
                print(f"📄 Payload:\n{pretty_json}")
                
                # Publish to MQTT
                mqtt_result = mqtt_client.publish(MQTT_TOPIC, message_value)
                print(f"📤 Published to MQTT topic '{MQTT_TOPIC}' with result: {mqtt_result.rc}")
                
            except json.JSONDecodeError:
                # Not JSON, print as-is
                print(f"📥 Received non-JSON message - Offset: {message.offset}, Value: {message.value}")
                mqtt_client.publish(MQTT_TOPIC, message.value)
                
            except Exception as e:
                print(f"⚠️ Error processing message: {e}")
except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")
    consumer.stop()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("👋 Shutdown complete!")
except Exception as e:
    print(f"❌ Error: {e}")
    consumer.stop()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()