#!/usr/bin/env python3
"""
StreamSQL Management Script
Provides utilities for managing the StreamSQL pipeline
"""

import os
import json
import requests
import time
import argparse
from pykafka import KafkaClient
from pykafka.common import OffsetType

class StreamSQLManager:
    def __init__(self, kafka_hosts="localhost:9092", connect_url="http://localhost:8083"):
        self.kafka_hosts = kafka_hosts
        self.connect_url = connect_url
        
    def list_connectors(self):
        """List all Kafka Connect connectors"""
        try:
            response = requests.get(f"{self.connect_url}/connectors", timeout=10)
            if response.status_code == 200:
                connectors = response.json()
                
                print("🔗 Kafka Connect Connectors:")
                print("=" * 60)
                
                if not connectors:
                    print("No connectors found")
                    return
                
                for connector in connectors:
                    # Get connector status
                    status_response = requests.get(f"{self.connect_url}/connectors/{connector}/status")
                    if status_response.status_code == 200:
                        status_data = status_response.json()
                        state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                        
                        # Color code status
                        if state == 'RUNNING':
                            status_icon = "✅"
                        elif state == 'FAILED':
                            status_icon = "❌"
                        else:
                            status_icon = "⚠️"
                        
                        print(f"{status_icon} {connector} - {state}")
                        
                        # Show task status
                        tasks = status_data.get('tasks', [])
                        for i, task in enumerate(tasks):
                            task_state = task.get('state', 'UNKNOWN')
                            print(f"   Task {i}: {task_state}")
                    else:
                        print(f"❓ {connector} - Status unknown")
                
                print("=" * 60)
            else:
                print(f"❌ Failed to connect to Kafka Connect: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error connecting to Kafka Connect: {e}")
    
    def delete_connector(self, connector_name):
        """Delete a specific connector"""
        try:
            response = requests.delete(f"{self.connect_url}/connectors/{connector_name}")
            if response.status_code == 204:
                print(f"✅ Deleted connector: {connector_name}")
            elif response.status_code == 404:
                print(f"⚠️ Connector not found: {connector_name}")
            else:
                print(f"❌ Failed to delete connector {connector_name}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error deleting connector: {e}")
    
    def restart_connector(self, connector_name):
        """Restart a specific connector"""
        try:
            response = requests.post(f"{self.connect_url}/connectors/{connector_name}/restart")
            if response.status_code == 204:
                print(f"✅ Restarted connector: {connector_name}")
            elif response.status_code == 404:
                print(f"⚠️ Connector not found: {connector_name}")
            else:
                print(f"❌ Failed to restart connector {connector_name}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error restarting connector: {e}")
    
    def list_topics(self):
        """List all Kafka topics"""
        try:
            kafka_client = KafkaClient(hosts=self.kafka_hosts)
            
            print("📡 Kafka Topics:")
            print("=" * 60)
            
            topics = []
            for topic_bytes in kafka_client.topics.keys():
                topic_name = topic_bytes.decode('utf-8')
                topics.append(topic_name)
            
            # Separate StreamSQL topics from system topics
            streamsql_topics = []
            system_topics = []
            
            for topic in sorted(topics):
                if topic.startswith('_') or 'connect' in topic.lower() or 'history' in topic.lower():
                    system_topics.append(topic)
                else:
                    streamsql_topics.append(topic)
            
            print("📊 StreamSQL Data Topics:")
            if streamsql_topics:
                for topic in streamsql_topics:
                    print(f"   📋 {topic}")
            else:
                print("   No StreamSQL topics found")
            
            print("\n🔧 System Topics:")
            for topic in system_topics:
                print(f"   ⚙️ {topic}")
            
            print("=" * 60)
            print(f"Total topics: {len(topics)} ({len(streamsql_topics)} data + {len(system_topics)} system)")
            
        except Exception as e:
            print(f"❌ Error listing topics: {e}")
    
    def check_topic_health(self, topic_name):
        """Check health of a specific topic"""
        try:
            kafka_client = KafkaClient(hosts=self.kafka_hosts)
            
            if topic_name.encode('utf-8') not in kafka_client.topics:
                print(f"❌ Topic '{topic_name}' not found")
                return
            
            topic = kafka_client.topics[topic_name.encode('utf-8')]
            
            print(f"🔍 Topic Health Check: {topic_name}")
            print("=" * 60)
            
            # Get topic metadata
            partitions = topic.partitions
            print(f"Partitions: {len(partitions)}")
            
            for partition_id, partition in partitions.items():
                print(f"  Partition {partition_id}:")
                print(f"    Leader: {partition.leader}")
                print(f"    Replicas: {len(partition.replicas)}")
                
                # Get latest offset
                try:
                    latest_offset = partition.latest_available_offset()
                    print(f"    Latest offset: {latest_offset}")
                except:
                    print(f"    Latest offset: Unable to fetch")
            
            print("=" * 60)
            
        except Exception as e:
            print(f"❌ Error checking topic health: {e}")
    
    def monitor_topic_activity(self, topic_name, duration=30):
        """Monitor message activity on a topic"""
        try:
            kafka_client = KafkaClient(hosts=self.kafka_hosts)
            
            if topic_name.encode('utf-8') not in kafka_client.topics:
                print(f"❌ Topic '{topic_name}' not found")
                return
            
            topic = kafka_client.topics[topic_name.encode('utf-8')]
            consumer = topic.get_simple_consumer(
                consumer_group=f"monitor-{int(time.time())}".encode('utf-8'),
                auto_offset_reset=OffsetType.LATEST,
                reset_offset_on_start=True
            )
            
            print(f"📊 Monitoring topic '{topic_name}' for {duration} seconds...")
            print("=" * 60)
            
            start_time = time.time()
            message_count = 0
            
            try:
                for message in consumer:
                    if time.time() - start_time > duration:
                        break
                        
                    if message and message.value:
                        message_count += 1
                        try:
                            payload = json.loads(message.value.decode('utf-8'))
                            operation = payload.get('op', 'unknown')
                            source = payload.get('source', {})
                            
                            print(f"📨 Message #{message_count}: {operation} on {source.get('table', 'unknown')}")
                        except:
                            print(f"📨 Message #{message_count}: Unable to parse")
                    
                    # Small delay to avoid overwhelming output
                    time.sleep(0.1)
                    
            except KeyboardInterrupt:
                print("\n🛑 Monitoring stopped by user")
            
            print("=" * 60)
            print(f"Total messages received: {message_count}")
            
            consumer.stop()
            
        except Exception as e:
            print(f"❌ Error monitoring topic: {e}")
    
    def cleanup_old_connectors(self):
        """Delete all StreamSQL connectors"""
        try:
            response = requests.get(f"{self.connect_url}/connectors", timeout=10)
            if response.status_code == 200:
                connectors = response.json()
                
                streamsql_connectors = [c for c in connectors if 'mssql-source-connector' in c]
                
                if not streamsql_connectors:
                    print("✅ No StreamSQL connectors to clean up")
                    return
                
                print(f"🧹 Found {len(streamsql_connectors)} StreamSQL connectors to delete:")
                for connector in streamsql_connectors:
                    print(f"   - {connector}")
                
                confirm = input("\nAre you sure you want to delete all these connectors? (y/N): ")
                if confirm.lower() == 'y':
                    for connector in streamsql_connectors:
                        self.delete_connector(connector)
                        time.sleep(1)
                    print("✅ Cleanup completed")
                else:
                    print("❌ Cleanup cancelled")
            else:
                print(f"❌ Failed to list connectors: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error during cleanup: {e}")


def main():
    parser = argparse.ArgumentParser(description='StreamSQL Management Script')
    parser.add_argument('--kafka-hosts', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--connect-url', default='http://localhost:8083',
                       help='Kafka Connect URL (default: http://localhost:8083)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List connectors
    subparsers.add_parser('list-connectors', help='List all Kafka Connect connectors')
    
    # Delete connector
    delete_parser = subparsers.add_parser('delete-connector', help='Delete a specific connector')
    delete_parser.add_argument('name', help='Connector name to delete')
    
    # Restart connector
    restart_parser = subparsers.add_parser('restart-connector', help='Restart a specific connector')
    restart_parser.add_argument('name', help='Connector name to restart')
    
    # List topics
    subparsers.add_parser('list-topics', help='List all Kafka topics')
    
    # Check topic health
    health_parser = subparsers.add_parser('check-topic', help='Check health of a specific topic')
    health_parser.add_argument('name', help='Topic name to check')
    
    # Monitor topic
    monitor_parser = subparsers.add_parser('monitor-topic', help='Monitor message activity on a topic')
    monitor_parser.add_argument('name', help='Topic name to monitor')
    monitor_parser.add_argument('--duration', type=int, default=30, help='Monitor duration in seconds')
    
    # Cleanup
    subparsers.add_parser('cleanup', help='Delete all StreamSQL connectors')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = StreamSQLManager(args.kafka_hosts, args.connect_url)
    
    if args.command == 'list-connectors':
        manager.list_connectors()
    elif args.command == 'delete-connector':
        manager.delete_connector(args.name)
    elif args.command == 'restart-connector':
        manager.restart_connector(args.name)
    elif args.command == 'list-topics':
        manager.list_topics()
    elif args.command == 'check-topic':
        manager.check_topic_health(args.name)
    elif args.command == 'monitor-topic':
        manager.monitor_topic_activity(args.name, args.duration)
    elif args.command == 'cleanup':
        manager.cleanup_old_connectors()


if __name__ == "__main__":
    main()
