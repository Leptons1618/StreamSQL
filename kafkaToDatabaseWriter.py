import os
import json
import time
from datetime import datetime
from urllib.parse import quote_plus
from pykafka import KafkaClient
from pykafka.common import OffsetType
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Text
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_HOSTS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
DB1_TOPIC_NAME = os.getenv("DB1_TOPIC_NAME")
DB2_TOPIC_NAME = os.getenv("DB2_TOPIC_NAME")

# Target database connection (DB1)
TARGET_DB_HOSTNAME = os.getenv("TARGET_DB_HOSTNAME")
TARGET_DB_PORT = os.getenv("TARGET_DB_PORT", "1433")
TARGET_DB_USER = os.getenv("TARGET_DB_USER")
TARGET_DB_PASSWORD = os.getenv("TARGET_DB_PASSWORD")
TARGET_DB_NAME = os.getenv("TARGET_DB_NAME")
CDC_CHANGES_TABLE = os.getenv("CDC_CHANGES_TABLE", "dbo.CDCChanges")

class KafkaToDatabaseWriter:
    def __init__(self):
        self.setup_database_connection()
        self.setup_kafka_client()
        self.create_cdc_changes_table()
        
    def setup_database_connection(self):
        """Setup database connection to DB1"""
        try:
            # URL encode the password and username to handle special characters
            encoded_user = quote_plus(TARGET_DB_USER)
            encoded_password = quote_plus(TARGET_DB_PASSWORD)
            
            # First, try with SQLalchemy
            connection_string = (
                f"mssql+pyodbc://{encoded_user}:{encoded_password}@"
                f"{TARGET_DB_HOSTNAME}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"
                f"?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
            )
            
            self.engine = create_engine(connection_string, pool_pre_ping=True, echo=False)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info(f"✅ Connected to target database: {TARGET_DB_HOSTNAME}")
            
        except Exception as sqlalchemy_error:
            logger.warning(f"⚠️ SQLAlchemy connection failed: {sqlalchemy_error}")
            
            # Fallback to direct pyodbc connection
            try:
                logger.info("🔄 Trying direct pyodbc connection...")
                
                direct_connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={TARGET_DB_HOSTNAME},{TARGET_DB_PORT};"
                    f"DATABASE={TARGET_DB_NAME};"
                    f"UID={TARGET_DB_USER};"
                    f"PWD={TARGET_DB_PASSWORD};"
                    f"TrustServerCertificate=yes;"
                )
                
                # Test direct connection
                conn = pyodbc.connect(direct_connection_string)
                conn.close()
                
                # Create SQLAlchemy engine with URL encoded connection string
                encoded_conn_str = quote_plus(direct_connection_string)
                self.engine = create_engine(
                    f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}",
                    pool_pre_ping=True
                )
                
                logger.info(f"✅ Connected to target database via direct pyodbc: {TARGET_DB_HOSTNAME}")
                
            except Exception as e:
                logger.error(f"❌ All connection methods failed: {e}")
                raise
            
    def setup_kafka_client(self):
        """Setup Kafka client"""
        try:
            self.kafka_client = KafkaClient(hosts=KAFKA_HOSTS)
            logger.info(f"✅ Connected to Kafka: {KAFKA_HOSTS}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
            
    def create_cdc_changes_table(self):
        """Create CDC changes table if it doesn't exist"""
        try:
            table_name = CDC_CHANGES_TABLE.split('.')[-1]
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            BEGIN
                CREATE TABLE {CDC_CHANGES_TABLE} (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    SourceDatabase NVARCHAR(50) NOT NULL,
                    SourceTable NVARCHAR(100) NOT NULL,
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
                ON {CDC_CHANGES_TABLE} (SourceTable, ChangeTimestamp);
                
                CREATE INDEX IX_CDCChanges_Operation_ChangeTimestamp 
                ON {CDC_CHANGES_TABLE} (Operation, ChangeTimestamp);
            END
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                
            logger.info(f"✅ CDC Changes table ensured: {CDC_CHANGES_TABLE}")
            
        except Exception as e:
            logger.error(f"❌ Failed to create CDC changes table: {e}")
            # Don't raise the exception - continue without the table creation
            logger.warning("⚠️ Continuing without table creation - make sure table exists manually")
            
    def extract_cdc_data(self, message_value):
        """Extract CDC data from Debezium message"""
        try:
            # The message_value IS the payload, not containing a payload
            payload = message_value
            
            operation = payload.get('op', 'unknown')  # c=create, u=update, d=delete, r=read
            before_data = payload.get('before')
            after_data = payload.get('after')
            source_info = payload.get('source', {})
            timestamp_ms = payload.get('ts_ms', 0)
            
            # Extract record ID (try to find primary key)
            record_id = None
            if after_data:
                # Try common primary key names
                for pk_field in ['CustomerID', 'Id', 'ID', 'id', 'CustomerId']:
                    if pk_field in after_data:
                        record_id = str(after_data[pk_field])
                        break
            elif before_data:
                for pk_field in ['CustomerID', 'Id', 'ID', 'id', 'CustomerId']:
                    if pk_field in before_data:
                        record_id = str(before_data[pk_field])
                        break
                        
            return {
                'operation': operation,
                'before_data': before_data,
                'after_data': after_data,
                'source_table': source_info.get('table', 'unknown'),
                'source_db': source_info.get('db', 'unknown'),
                'server_name': source_info.get('name', 'unknown'),
                'timestamp_ms': timestamp_ms,
                'record_id': record_id
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to extract CDC data: {e}")
            logger.error(f"❌ Message structure: {json.dumps(message_value, indent=2) if isinstance(message_value, dict) else str(message_value)}")
            return None
            
    def write_change_to_database(self, cdc_data, source_db_identifier, full_payload):
        """Write CDC change to database"""
        try:
            insert_sql = f"""
            INSERT INTO {CDC_CHANGES_TABLE} (
                SourceDatabase, SourceTable, Operation, RecordId, 
                KafkaTimestamp, BeforeData, AfterData, FullPayload
            ) VALUES (:source_database, :source_table, :operation, :record_id, 
                     :kafka_timestamp, :before_data, :after_data, :full_payload)
            """
            
            # Prepare data
            source_database = f"{source_db_identifier} ({cdc_data['server_name']})"
            source_table = cdc_data['source_table']
            operation = cdc_data['operation']
            record_id = cdc_data['record_id']
            kafka_timestamp = cdc_data['timestamp_ms']
            before_data = json.dumps(cdc_data['before_data']) if cdc_data['before_data'] else None
            after_data = json.dumps(cdc_data['after_data']) if cdc_data['after_data'] else None
            full_payload_json = json.dumps(full_payload)
            
            # Parameters dictionary
            params = {
                'source_database': source_database,
                'source_table': source_table,
                'operation': operation,
                'record_id': record_id,
                'kafka_timestamp': kafka_timestamp,
                'before_data': before_data,
                'after_data': after_data,
                'full_payload': full_payload_json
            }
            
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), params)
                conn.commit()
                
            logger.info(f"✅ Recorded {operation} operation for {source_database}.{source_table} (ID: {record_id})")
            
        except Exception as e:
            logger.error(f"❌ Failed to write change to database: {e}")
            logger.error(f"❌ CDC Data: {cdc_data}")
            logger.error(f"❌ Source DB: {source_db_identifier}")
            
    def consume_topic(self, topic_name, db_identifier):
        """Consumer function for a specific topic"""
        try:
            if not topic_name:
                logger.warning(f"⚠️ No topic name provided for {db_identifier}")
                return
                
            logger.info(f"🔍 [{db_identifier}] Attempting to connect to topic: {topic_name}")
            
            topic = self.kafka_client.topics[topic_name.encode('utf-8')]
            consumer = topic.get_simple_consumer(
                consumer_group=f"db-writer-{db_identifier.lower()}".encode('utf-8'),
                auto_offset_reset=OffsetType.LATEST,
                reset_offset_on_start=True,
                auto_commit_enable=True
            )
            
            logger.info(f"✅ [{db_identifier}] Started consuming from topic: {topic_name}")
            
            message_count = 0
            for message in consumer:
                if message and message.value:
                    try:
                        message_count += 1
                        logger.debug(f"📨 [{db_identifier}] Processing message #{message_count}")
                        
                        message_value = json.loads(message.value.decode('utf-8'))
                        
                        # Log the raw message for debugging
                        logger.debug(f"📨 [{db_identifier}] Raw message: {json.dumps(message_value, indent=2)}")
                        
                        # Extract CDC data
                        cdc_data = self.extract_cdc_data(message_value)
                        if not cdc_data:
                            logger.warning(f"⚠️ [{db_identifier}] Failed to extract CDC data from message")
                            continue
                            
                        logger.info(f"📊 [{db_identifier}] Extracted CDC: {cdc_data['operation']} on {cdc_data['source_db']}.{cdc_data['source_table']} (ID: {cdc_data['record_id']}) from {cdc_data['server_name']}")
                        
                        # Skip read operations (initial snapshot) - but log them
                        if cdc_data['operation'] == 'r':
                            logger.info(f"🔍 [{db_identifier}] Skipping read/snapshot operation for record {cdc_data['record_id']}")
                            continue
                            
                        # Write to database
                        self.write_change_to_database(cdc_data, db_identifier, message_value)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ [{db_identifier}] JSON decode error: {e}")
                    except Exception as e:
                        logger.error(f"⚠️ [{db_identifier}] Error processing message #{message_count}: {e}")
                        logger.error(f"⚠️ [{db_identifier}] Message content: {message.value.decode('utf-8') if message.value else 'None'}")
                        
        except Exception as e:
            logger.error(f"❌ [{db_identifier}] Error in consumer: {e}")
        finally:
            if 'consumer' in locals():
                try:
                    consumer.stop()
                except:
                    pass
                    
    def run(self):
        """Main run method"""
        logger.info("🚀 Kafka to Database Writer started")
        logger.info(f"📡 DB1 Topic: {DB1_TOPIC_NAME}")
        logger.info(f"📡 DB2 Topic: {DB2_TOPIC_NAME}")
        logger.info(f"💾 Target Database: {TARGET_DB_HOSTNAME}")
        logger.info(f"📋 CDC Changes Table: {CDC_CHANGES_TABLE}")
        
        # Create threads for both database topics
        threads = []
        
        if DB1_TOPIC_NAME:
            db1_thread = threading.Thread(target=self.consume_topic, args=(DB1_TOPIC_NAME, "DB1"))
            db1_thread.daemon = True
            threads.append(db1_thread)
            
        if DB2_TOPIC_NAME:
            db2_thread = threading.Thread(target=self.consume_topic, args=(DB2_TOPIC_NAME, "DB2"))
            db2_thread.daemon = True
            threads.append(db2_thread)
            
        if not threads:
            logger.error("❌ No topics configured for consumption")
            return
            
        try:
            # Start all consumer threads
            for thread in threads:
                thread.start()
                
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
    writer = KafkaToDatabaseWriter()
    writer.run()
