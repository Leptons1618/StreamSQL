#!/usr/bin/env python3
"""
StreamSQL Topic Consumer Example
Demonstrates how to consume from specific topics created by StreamSQL
Each table gets its own topic, allowing selective consumption
"""

import os
import json
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType
import argparse

class StreamSQLConsumer:
    def __init__(self, kafka_hosts="localhost:9092"):
        self.kafka_hosts = kafka_hosts
        self.kafka_client = KafkaClient(hosts=kafka_hosts)
        
    def list_available_topics(self):
        """List all available topics in Kafka"""
        print("📡 Available Topics in Kafka:")
        print("=" * 60)
        
        topics = []
        for topic_bytes in self.kafka_client.topics.keys():
            topic_name = topic_bytes.decode('utf-8')
            
            # Skip internal topics
            if not topic_name.startswith('_') and 'connect' not in topic_name.lower() and 'history' not in topic_name.lower():
                topics.append(topic_name)
        
        if not topics:
            print("No StreamSQL topics found")
            return []
        
        for i, topic in enumerate(sorted(topics), 1):
            print(f"{i:2d}. {topic}")
            
        print("=" * 60)
        return sorted(topics)
    
    def consume_from_topic(self, topic_name, consumer_group=None, show_snapshots=False):
        """Consume messages from a specific topic"""
        if not consumer_group:
            consumer_group = f"example-consumer-{int(time.time())}"
            
        print(f"🚀 Starting consumer for topic: {topic_name}")
        print(f"👥 Consumer group: {consumer_group}")
        print(f"📸 Show snapshots: {show_snapshots}")
        print("-" * 60)
        
        try:
            topic = self.kafka_client.topics[topic_name.encode('utf-8')]
            consumer = topic.get_simple_consumer(
                consumer_group=consumer_group.encode('utf-8'),
                auto_offset_reset=OffsetType.LATEST,
                reset_offset_on_start=True,
                auto_commit_enable=True
            )
            
            print("✅ Consumer started successfully. Waiting for messages...")
            print("   Press Ctrl+C to stop")
            print("-" * 60)
            
            message_count = 0
            for message in consumer:
                if message and message.value:
                    try:
                        message_count += 1
                        payload = json.loads(message.value.decode('utf-8'))
                        
                        # Extract key information
                        operation = payload.get('op', 'unknown')
                        source = payload.get('source', {})
                        before_data = payload.get('before')
                        after_data = payload.get('after')
                        timestamp = payload.get('ts_ms', 0)
                        
                        # Skip snapshots if not requested
                        if operation == 'r' and not show_snapshots:
                            continue
                            
                        # Format timestamp
                        if timestamp:
                            ts_formatted = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
                        else:
                            ts_formatted = 'unknown'
                        
                        # Display message info
                        print(f"📨 Message #{message_count}")
                        print(f"   Operation: {self.get_operation_name(operation)}")
                        print(f"   Timestamp: {ts_formatted}")
                        print(f"   Source: {source.get('name', 'unknown')}.{source.get('db', 'unknown')}.{source.get('table', 'unknown')}")
                        
                        # Show data based on operation
                        if operation in ['c', 'r']:  # Create/Read
                            if after_data:
                                print(f"   Data: {json.dumps(after_data, indent=6)}")
                        elif operation == 'u':  # Update
                            print(f"   Before: {json.dumps(before_data, indent=10) if before_data else 'null'}")
                            print(f"   After:  {json.dumps(after_data, indent=10) if after_data else 'null'}")
                        elif operation == 'd':  # Delete
                            if before_data:
                                print(f"   Deleted: {json.dumps(before_data, indent=12)}")
                        
                        print("-" * 60)
                        
                    except json.JSONDecodeError as e:
                        print(f"❌ JSON decode error: {e}")
                    except Exception as e:
                        print(f"⚠️ Error processing message: {e}")
                        
        except KeyboardInterrupt:
            print("\n🛑 Consumer stopped by user")
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            if 'consumer' in locals():
                try:
                    consumer.stop()
                    print("✅ Consumer stopped cleanly")
                except:
                    pass
    
    def get_operation_name(self, op_code):
        """Convert operation code to human-readable name"""
        operations = {
            'c': 'CREATE/INSERT',
            'u': 'UPDATE', 
            'd': 'DELETE',
            'r': 'READ/SNAPSHOT'
        }
        return operations.get(op_code, f'UNKNOWN({op_code})')
    
    def consume_multiple_topics(self, topic_patterns, consumer_group=None):
        """Consume from multiple topics matching patterns"""
        all_topics = [topic.decode('utf-8') for topic in self.kafka_client.topics.keys()]
        
        matching_topics = []
        for pattern in topic_patterns:
            for topic in all_topics:
                if pattern.lower() in topic.lower() and topic not in matching_topics:
                    matching_topics.append(topic)
        
        if not matching_topics:
            print(f"❌ No topics found matching patterns: {topic_patterns}")
            return
        
        print(f"🎯 Found {len(matching_topics)} matching topics:")
        for topic in matching_topics:
            print(f"   - {topic}")
        
        # For this example, we'll consume from the first matching topic
        # In a real application, you'd want to create separate threads for each
        if matching_topics:
            self.consume_from_topic(matching_topics[0], consumer_group)


def main():
    parser = argparse.ArgumentParser(description='StreamSQL Topic Consumer Example')
    parser.add_argument('--kafka-hosts', default='localhost:9092', 
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--list-topics', action='store_true',
                       help='List all available topics and exit')
    parser.add_argument('--topic', type=str,
                       help='Specific topic to consume from')
    parser.add_argument('--pattern', type=str, nargs='+',
                       help='Topic patterns to match (e.g., "Customers" "Products")')
    parser.add_argument('--consumer-group', type=str,
                       help='Consumer group name (default: auto-generated)')
    parser.add_argument('--show-snapshots', action='store_true',
                       help='Show snapshot/read operations (default: false)')
    
    args = parser.parse_args()
    
    print("🔗 StreamSQL Topic Consumer Example")
    print("=" * 60)
    
    consumer = StreamSQLConsumer(args.kafka_hosts)
    
    if args.list_topics:
        consumer.list_available_topics()
        return
    
    # List topics first
    topics = consumer.list_available_topics()
    
    if args.topic:
        # Consume from specific topic
        if args.topic in topics:
            consumer.consume_from_topic(args.topic, args.consumer_group, args.show_snapshots)
        else:
            print(f"❌ Topic '{args.topic}' not found")
    elif args.pattern:
        # Consume from topics matching patterns
        consumer.consume_multiple_topics(args.pattern, args.consumer_group)
    else:
        # Interactive mode
        if not topics:
            print("❌ No topics available")
            return
            
        print("\n🎯 Interactive Mode:")
        print("Enter the number of the topic you want to consume from:")
        
        try:
            choice = int(input("Topic number: ")) - 1
            if 0 <= choice < len(topics):
                selected_topic = topics[choice]
                consumer.consume_from_topic(selected_topic, args.consumer_group, args.show_snapshots)
            else:
                print("❌ Invalid topic number")
        except ValueError:
            print("❌ Please enter a valid number")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()
