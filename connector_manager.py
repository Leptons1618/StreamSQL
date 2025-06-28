#!/usr/bin/env python3
"""
Dynamic Connector Manager for StreamSQL
Automatically creates Kafka Connect connectors based on environment variables
Supports multiple databases and tables with individual topics
"""

import os
import json
import time
import requests
import sys
from typing import Dict, List, Tuple

class ConnectorManager:
    def __init__(self):
        self.kafka_connect_url = "http://kafka-connect:8083"
        self.max_retries = 30
        self.retry_interval = 10
        
    def wait_for_kafka_connect(self):
        """Wait for Kafka Connect to be available"""
        print("🔄 Waiting for Kafka Connect to be available...")
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(f"{self.kafka_connect_url}/", timeout=5)
                if response.status_code == 200:
                    print("✅ Kafka Connect is available")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            print(f"⏳ Attempt {attempt + 1}/{self.max_retries} - Kafka Connect not ready, waiting {self.retry_interval}s...")
            time.sleep(self.retry_interval)
        
        print("❌ Kafka Connect failed to become available")
        return False
    
    def parse_database_configs(self) -> List[Dict]:
        """Parse environment variables to extract database configurations"""
        databases = []
        db_index = 1
        
        while True:
            db_prefix = f"DB{db_index}"
            hostname = os.getenv(f"{db_prefix}_HOSTNAME")
            
            if not hostname:
                break
                
            # Get all required database configuration
            config = {
                'db_index': db_index,
                'hostname': hostname,
                'port': os.getenv(f"{db_prefix}_PORT", "1433"),
                'user': os.getenv(f"{db_prefix}_USER"),
                'password': os.getenv(f"{db_prefix}_PASSWORD"),
                'database': os.getenv(f"{db_prefix}_NAME"),
                'server_name': os.getenv(f"{db_prefix}_SERVER_NAME"),
                'table_list': os.getenv(f"{db_prefix}_TABLE_INCLUDE_LIST", ""),
                'history_topic': os.getenv(f"{db_prefix}_HISTORY_TOPIC", f"dbhistory.sql-server-cdc-db{db_index}")
            }
            
            # Validate required fields
            required_fields = ['hostname', 'user', 'password', 'database', 'server_name']
            missing_fields = [field for field in required_fields if not config[field]]
            
            if missing_fields:
                print(f"⚠️  Skipping DB{db_index} due to missing fields: {missing_fields}")
                db_index += 1
                continue
            
            databases.append(config)
            print(f"📋 Found configuration for DB{db_index}: {config['server_name']}.{config['database']}")
            
            db_index += 1
        
        return databases
    
    def parse_table_configs(self, table_list: str) -> List[str]:
        """Parse table list and return individual tables"""
        if not table_list:
            return []
        
        # Split by comma and clean up whitespace
        tables = [table.strip() for table in table_list.split(',') if table.strip()]
        return tables
    
    def create_connector_config(self, db_config: Dict, table: str) -> Dict:
        """Create connector configuration for a specific database and table"""
        db_index = db_config['db_index']
        table_safe = table.replace('.', '_').replace(' ', '_')
        
        connector_name = f"mssql-source-connector-db{db_index}-{table_safe}"
        
        config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
                "database.hostname": db_config['hostname'],
                "database.port": db_config['port'],
                "database.user": db_config['user'],
                "database.password": db_config['password'],
                "database.dbname": db_config['database'],
                "database.server.name": f"{db_config['server_name']}-{table_safe}",
                "table.include.list": table,
                "database.history.kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                "database.history.kafka.topic": f"{db_config['history_topic']}-{table_safe}",
                "database.history.kafka.recovery.poll.interval.ms": os.getenv("RECOVERY_POLL_INTERVAL_MS", "5000"),
                "database.history.kafka.recovery.attempts": os.getenv("RECOVERY_ATTEMPTS", "4"),
                "tasks.max": os.getenv("TASKS_MAX", "1"),
                "snapshot.mode": os.getenv("SNAPSHOT_MODE", "initial"),
                "topic.creation.default.replication.factor": int(os.getenv("TOPIC_CREATION_REPLICATION_FACTOR", "1")),
                "topic.creation.default.partitions": int(os.getenv("TOPIC_CREATION_PARTITIONS", "1")),
                "topic.creation.enable": os.getenv("TOPIC_CREATION_ENABLE", "true").lower() == "true",
                "database.encrypt": "false",
                "database.trustServerCertificate": "true"
            }
        }
        
        return config
    
    def create_connector(self, connector_config: Dict) -> bool:
        """Create a connector in Kafka Connect"""
        connector_name = connector_config['name']
        
        try:
            # Check if connector already exists
            response = requests.get(f"{self.kafka_connect_url}/connectors/{connector_name}")
            if response.status_code == 200:
                print(f"⚠️  Connector {connector_name} already exists, deleting...")
                delete_response = requests.delete(f"{self.kafka_connect_url}/connectors/{connector_name}")
                if delete_response.status_code in [204, 404]:
                    print(f"✅ Deleted existing connector {connector_name}")
                    time.sleep(5)  # Wait a bit before recreating
                else:
                    print(f"❌ Failed to delete connector {connector_name}: {delete_response.status_code}")
                    return False
            
            # Create the connector
            response = requests.post(
                f"{self.kafka_connect_url}/connectors",
                headers={"Content-Type": "application/json"},
                json=connector_config,
                timeout=30
            )
            
            if response.status_code in [201, 409]:  # 201 = created, 409 = already exists
                print(f"✅ Successfully created connector: {connector_name}")
                return True
            else:
                print(f"❌ Failed to create connector {connector_name}: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error creating connector {connector_name}: {e}")
            return False
    
    def get_topic_name(self, db_config: Dict, table: str) -> str:
        """Generate topic name for a database table"""
        table_safe = table.replace('.', '_').replace(' ', '_')
        return f"{db_config['server_name']}-{table_safe}.{table}"
    
    def run(self):
        """Main execution method"""
        print("🚀 StreamSQL Dynamic Connector Manager started")
        
        # Wait for Kafka Connect
        if not self.wait_for_kafka_connect():
            sys.exit(1)
        
        # Parse database configurations
        databases = self.parse_database_configs()
        
        if not databases:
            print("❌ No database configurations found. Check your environment variables.")
            sys.exit(1)
        
        print(f"📊 Found {len(databases)} database(s) to configure")
        
        # Create connectors for each database and table
        total_connectors = 0
        successful_connectors = 0
        
        for db_config in databases:
            print(f"\n🔧 Processing database: {db_config['server_name']}")
            
            tables = self.parse_table_configs(db_config['table_list'])
            if not tables:
                print(f"⚠️  No tables configured for DB{db_config['db_index']}")
                continue
            
            print(f"📋 Tables to monitor: {tables}")
            
            for table in tables:
                print(f"\n  🔗 Creating connector for table: {table}")
                
                # Create connector configuration
                connector_config = self.create_connector_config(db_config, table)
                
                # Get topic name for reference
                topic_name = self.get_topic_name(db_config, table)
                print(f"  📡 Topic will be: {topic_name}")
                
                # Create the connector
                if self.create_connector(connector_config):
                    successful_connectors += 1
                    
                total_connectors += 1
                
                # Small delay between connector creations
                time.sleep(2)
        
        print(f"\n📈 Summary:")
        print(f"   Total connectors attempted: {total_connectors}")
        print(f"   Successfully created: {successful_connectors}")
        print(f"   Failed: {total_connectors - successful_connectors}")
        
        if successful_connectors > 0:
            print("✅ Connector setup completed successfully")
        else:
            print("❌ No connectors were created successfully")
            sys.exit(1)


if __name__ == "__main__":
    manager = ConnectorManager()
    manager.run()
