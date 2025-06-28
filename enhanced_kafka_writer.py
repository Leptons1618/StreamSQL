#!/usr/bin/env python3
"""
Enhanced Kafka to Database Writer for StreamSQL
Dynamically discovers and consumes from multiple topics based on environment configuration
Supports multiple databases and tables with individual topic handling
"""

import os
import json
import time
import re
from datetime import datetime
from urllib.parse import quote_plus
from pykafka import KafkaClient
from pykafka.common import OffsetType
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedKafkaToDatabaseWriter:
    def __init__(self):
        self.setup_database_connection()
        self.setup_kafka_client()
        self.create_cdc_changes_table()
        self.database_configs = self.parse_database_configs()
        
    def setup_database_connection(self):
        """Setup database connection to target database"""
        try:
            # Use DB1 as target database by default, or use TARGET_DB_ variables if available
            target_hostname = os.getenv("TARGET_DB_HOSTNAME") or os.getenv("DB1_HOSTNAME")
            target_port = os.getenv("TARGET_DB_PORT") or os.getenv("DB1_PORT", "1433")
            target_user = os.getenv("TARGET_DB_USER") or os.getenv("DB1_USER")
            target_password = os.getenv("TARGET_DB_PASSWORD") or os.getenv("DB1_PASSWORD")
            target_database = os.getenv("TARGET_DB_NAME") or os.getenv("DB1_NAME")
            
            if not all([target_hostname, target_user, target_password, target_database]):
                raise ValueError("Missing target database configuration")
            
            # URL encode credentials
            encoded_user = quote_plus(target_user)
            encoded_password = quote_plus(target_password)
            
            connection_string = (
                f"mssql+pyodbc://{encoded_user}:{encoded_password}@"
                f"{target_hostname}:{target_port}/{target_database}"
                f"?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
            )
            
            self.engine = create_engine(connection_string, pool_pre_ping=True, echo=False)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info(f"✅ Connected to target database: {target_hostname}")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to target database: {e}")
            raise
            
    def setup_kafka_client(self):
        """Setup Kafka client"""
        try:
            kafka_hosts = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
            self.kafka_client = KafkaClient(hosts=kafka_hosts)
            logger.info(f"✅ Connected to Kafka: {kafka_hosts}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
            
    def create_cdc_changes_table(self):
        """Create CDC changes table if it doesn't exist"""
        try:
            cdc_table = os.getenv("CDC_CHANGES_TABLE", "dbo.CDCChanges")
            table_name = cdc_table.split('.')[-1]
            
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            BEGIN
                CREATE TABLE {cdc_table} (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    SourceDatabase NVARCHAR(100) NOT NULL,
                    SourceTable NVARCHAR(100) NOT NULL,
                    SourceServer NVARCHAR(100) NOT NULL,
                    TopicName NVARCHAR(200) NOT NULL,
                    Operation NVARCHAR(10) NOT NULL,
                    RecordId NVARCHAR(100),
                    ChangeTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
                    KafkaTimestamp BIGINT,
                    BeforeData NVARCHAR(MAX),
                    AfterData NVARCHAR(MAX),
                    FullPayload NVARCHAR(MAX),
                    ProcessedAt DATETIME2 NOT NULL DEFAULT GETDATE()
                );
                
                CREATE INDEX IX_CDCChanges_SourceTable_ChangeTimestamp 
                ON {cdc_table} (SourceTable, ChangeTimestamp);
                
                CREATE INDEX IX_CDCChanges_Operation_ChangeTimestamp 
                ON {cdc_table} (Operation, ChangeTimestamp);
                
                CREATE INDEX IX_CDCChanges_TopicName_ChangeTimestamp 
                ON {cdc_table} (TopicName, ChangeTimestamp);
            END
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                
            logger.info(f"✅ CDC Changes table ensured: {cdc_table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to create CDC changes table: {e}")
            logger.warning("⚠️ Continuing without table creation - make sure table exists manually")
            
    def parse_database_configs(self):
        """Parse environment variables to extract database configurations"""
        databases = {}
        db_index = 1
        
        while True:
            db_prefix = f"DB{db_index}"
            hostname = os.getenv(f"{db_prefix}_HOSTNAME")
            
            if not hostname:
                break
                
            config = {
                'hostname': hostname,
                'database': os.getenv(f"{db_prefix}_NAME"),
                'server_name': os.getenv(f"{db_prefix}_SERVER_NAME"),
                'table_list': os.getenv(f"{db_prefix}_TABLE_INCLUDE_LIST", "")
            }
            
            databases[f"DB{db_index}"] = config
            logger.info(f"📋 Registered DB{db_index}: {config['server_name']}.{config['database']}")
            
            db_index += 1
        
        return databases
    
    def discover_topics(self):
        """Discover available topics that match our database patterns"""
        topics = []
        
        try:
            # Get all topics from Kafka
            kafka_topics = list(self.kafka_client.topics.keys())
            
            for topic_bytes in kafka_topics:
                topic_name = topic_bytes.decode('utf-8')
                
                # Skip internal Kafka topics
                if topic_name.startswith('_') or 'connect' in topic_name.lower() or 'history' in topic_name.lower():
                    continue
                
                # Check if this topic matches any of our database configurations
                for db_id, db_config in self.database_configs.items():
                    server_name = db_config['server_name']
                    
                    # Match topics that start with our server name
                    if topic_name.startswith(server_name):
                        topic_info = {
                            'name': topic_name,
                            'db_id': db_id,
                            'server_name': server_name,
                            'database': db_config['database']
                        }
                        
                        # Try to extract table name from topic
                        # Topic format: server-table.schema.table
                        if '.' in topic_name:
                            parts = topic_name.split('.')
                            if len(parts) >= 2:
                                topic_info['table'] = f"{parts[-2]}.{parts[-1]}"
                            else:
                                topic_info['table'] = parts[-1]
                        else:
                            topic_info['table'] = 'unknown'
                        
                        topics.append(topic_info)
                        logger.info(f"📡 Discovered topic: {topic_name} -> {db_id}.{topic_info['table']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to discover topics: {e}")
        
        return topics
    
    def extract_cdc_data(self, message_value, topic_info):
        """Extract CDC data from Debezium message"""
        try:
            payload = message_value
            
            operation = payload.get('op', 'unknown')
            before_data = payload.get('before')
            after_data = payload.get('after')
            source_info = payload.get('source', {})
            timestamp_ms = payload.get('ts_ms', 0)
            
            # Extract record ID
            record_id = None
            for data in [after_data, before_data]:
                if data:
                    for pk_field in ['CustomerID', 'Id', 'ID', 'id', 'CustomerId', 'EmployeeID', 'ProductID']:
                        if pk_field in data:
                            record_id = str(data[pk_field])
                            break
                    if record_id:
                        break
                        
            return {
                'operation': operation,
                'before_data': before_data,
                'after_data': after_data,
                'source_table': source_info.get('table', topic_info['table']),
                'source_db': source_info.get('db', topic_info['database']),
                'server_name': source_info.get('name', topic_info['server_name']),
                'timestamp_ms': timestamp_ms,
                'record_id': record_id,
                'topic_name': topic_info['name']
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to extract CDC data: {e}")
            return None
            
    def write_change_to_database(self, cdc_data, topic_info, full_payload):
        """Write CDC change to database"""
        try:
            cdc_table = os.getenv("CDC_CHANGES_TABLE", "dbo.CDCChanges")
            
            insert_sql = f"""
            INSERT INTO {cdc_table} (
                SourceDatabase, SourceTable, SourceServer, TopicName, Operation, RecordId, 
                KafkaTimestamp, BeforeData, AfterData, FullPayload
            ) VALUES (:source_database, :source_table, :source_server, :topic_name, :operation, :record_id, 
                     :kafka_timestamp, :before_data, :after_data, :full_payload)
            """
            
            params = {
                'source_database': f"{topic_info['db_id']} ({cdc_data['source_db']})",
                'source_table': cdc_data['source_table'],
                'source_server': cdc_data['server_name'],
                'topic_name': cdc_data['topic_name'],
                'operation': cdc_data['operation'],
                'record_id': cdc_data['record_id'],
                'kafka_timestamp': cdc_data['timestamp_ms'],
                'before_data': json.dumps(cdc_data['before_data']) if cdc_data['before_data'] else None,
                'after_data': json.dumps(cdc_data['after_data']) if cdc_data['after_data'] else None,
                'full_payload': json.dumps(full_payload)
            }
            
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), params)
                conn.commit()
                
            logger.info(f"✅ Recorded {cdc_data['operation']} operation for {topic_info['db_id']}.{cdc_data['source_table']} (ID: {cdc_data['record_id']}) from topic {cdc_data['topic_name']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to write change to database: {e}")
            
    def consume_topic(self, topic_info):
        """Consumer function for a specific topic"""
        topic_name = topic_info['name']
        db_id = topic_info['db_id']
        
        try:
            logger.info(f"🔍 [{db_id}] Starting consumer for topic: {topic_name}")
            
            topic = self.kafka_client.topics[topic_name.encode('utf-8')]
            consumer = topic.get_simple_consumer(
                consumer_group=f"enhanced-db-writer-{db_id.lower()}-{topic_info['table'].replace('.', '-')}".encode('utf-8'),
                auto_offset_reset=OffsetType.LATEST,
                reset_offset_on_start=True,
                auto_commit_enable=True
            )
            
            logger.info(f"✅ [{db_id}] Started consuming from topic: {topic_name}")
            
            message_count = 0
            for message in consumer:
                if message and message.value:
                    try:
                        message_count += 1
                        message_value = json.loads(message.value.decode('utf-8'))
                        
                        # Extract CDC data
                        cdc_data = self.extract_cdc_data(message_value, topic_info)
                        if not cdc_data:
                            logger.warning(f"⚠️ [{db_id}] Failed to extract CDC data from message")
                            continue
                            
                        logger.info(f"📊 [{db_id}] Extracted CDC: {cdc_data['operation']} on {cdc_data['source_table']} (ID: {cdc_data['record_id']}) from topic {topic_name}")
                        
                        # Skip read operations (initial snapshot) for logging
                        if cdc_data['operation'] == 'r':
                            logger.debug(f"🔍 [{db_id}] Skipping read/snapshot operation for record {cdc_data['record_id']}")
                            continue
                            
                        # Write to database
                        self.write_change_to_database(cdc_data, topic_info, message_value)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ [{db_id}] JSON decode error: {e}")
                    except Exception as e:
                        logger.error(f"⚠️ [{db_id}] Error processing message #{message_count}: {e}")
                        
        except Exception as e:
            logger.error(f"❌ [{db_id}] Error in consumer for topic {topic_name}: {e}")
        finally:
            if 'consumer' in locals():
                try:
                    consumer.stop()
                except:
                    pass
                    
    def run(self):
        """Main run method"""
        logger.info("🚀 Enhanced Kafka to Database Writer started")
        
        if not self.database_configs:
            logger.error("❌ No database configurations found")
            return
        
        # Discover topics
        logger.info("🔍 Discovering available topics...")
        topics = self.discover_topics()
        
        if not topics:
            logger.error("❌ No matching topics found")
            return
        
        logger.info(f"📡 Found {len(topics)} topics to consume")
        for topic in topics:
            logger.info(f"   - {topic['name']} ({topic['db_id']}.{topic['table']})")
        
        # Create consumer threads for each topic
        threads = []
        
        for topic_info in topics:
            thread = threading.Thread(
                target=self.consume_topic, 
                args=(topic_info,),
                name=f"Consumer-{topic_info['db_id']}-{topic_info['table']}"
            )
            thread.daemon = True
            threads.append(thread)
            
        if not threads:
            logger.error("❌ No consumer threads created")
            return
            
        try:
            # Start all consumer threads
            for thread in threads:
                thread.start()
                time.sleep(1)  # Small delay between thread starts
                
            logger.info(f"🚀 Started {len(threads)} consumer threads")
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
                
        except KeyboardInterrupt:
            logger.info("🛑 Interrupted by user")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        finally:
            self.cleanup()
            
    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'engine'):
                self.engine.dispose()
            logger.info("🧹 Cleanup completed")
        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")


if __name__ == "__main__":
    writer = EnhancedKafkaToDatabaseWriter()
    writer.run()
