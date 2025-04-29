import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_keyspace(session):
    session.execute(""" 
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class':'SimpleStrategy', 'replication_factor':'1'}
    """)
    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users(
            id UUID PRIMARY KEY,
            name TEXT,
            username TEXT,
            email TEXT,
            address TEXT,
            phone TEXT,
            website TEXT,
            company TEXT
        )
    """)
    print("Table created successfully")

def insert_data(session, **kwargs):
    print("inserting data...")
    
    id = kwargs.get('id')
    name = kwargs.get('name')
    username = kwargs.get('username')
    email = kwargs.get('email')
    address = kwargs.get('address')
    phone = kwargs.get('phone')
    website = kwargs.get('website')
    company = kwargs.get('company')
    
    try:
        # Execute the INSERT query
        session.execute("""
            INSERT INTO spark_streams.created_users (id, name, username, email, address, phone, website, company)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, name, username, email, address, phone, website, company))
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error inserting data: {e}")

def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra_2.13:3.4.1",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create Spark connection due to {e}")
        return None

def create_cassandra_connection():    
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
    except Exception as e:
        logging.error(f"Couldn't create Cassandra connection due to {e}")
        return None
    return session

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame cannot be created because {e}")
    
    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('username', StringType(), True),
        StructField('email', StringType(), True),
        StructField('address', StringType(), True),
        StructField('phone', StringType(), True),
        StructField('website', StringType(), True),
        StructField('company', StringType(), True),
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    print(sel)

if __name__ == '__main__':
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            # Sample data to insert
            # insert_data(session, id="1234-5678-9101", name="John Doe", username="johndoe", email="johndoe@example.com",
            #             address="123 Main St", phone="555-1234", website="www.johndoe.com", company="Doe Corp")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option("checkpointLocation", '/temp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option("table", 'created_users')
                                .start())

            streaming_query.awaitTermination()
