from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------------------------------------------------
# BRONZE: Simulate Mobile App Telemetry (Kafka Ingestion)
# -------------------------------------------------------------------------
@dp.table(
    comment="Raw streaming mobile app events from 'Kafka'",
    table_properties={"quality": "bronze"}
)
def mobile_events_bronze():
    # 1. Define lists of fake data to pick from randomly
    event_types = ["app_open", "login", "view_balance", "transfer_init", "logout", "error"]
    os_types = ["iOS", "Android"]
    
    # 2. Generate the rate stream (Heartbeat)
    return (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)  # Adjust volume here
        .load()
        .withColumn("timestamp", col("timestamp"))
        
        # 3. Create Synthetic Data Logic
        .withColumn("random_user_id", (rand() * 1000).cast("int")) 
        .withColumn("event_index", (rand() * len(event_types)).cast("int"))
        .withColumn("os_index", (rand() * len(os_types)).cast("int"))
        
        # 4. Pack it into a JSON string to simulate a raw Kafka payload
        # This matches the 'value' column you'd get from Kafka
        .select(
            col("timestamp").alias("kafka_ingest_time"),
            lit("mobile_telemetry_topic").alias("topic"),
            to_json(struct(
                concat(lit("user_"), col("random_user_id")).alias("user_id"),
                uuid().alias("session_id"),
                # Pick random item from lists using element_at (arrays are 1-based in Spark SQL)
                element_at(array([lit(x) for x in event_types]), col("event_index") + 1).alias("event_name"),
                element_at(array([lit(x) for x in os_types]), col("os_index") + 1).alias("device_os"),
                lit("v2.4.1").alias("app_version"),
                col("timestamp").alias("event_time")
            )).cast("binary").alias("value") # Kafka values are binary
        )
    )

# -------------------------------------------------------------------------
# SILVER: Parse & Structure User Sessions
# -------------------------------------------------------------------------
@dp.table(
    comment="Cleaned mobile events parsed from JSON",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("valid_user", "user_id IS NOT NULL")
def mobile_events_silver():
    # Define the schema of the JSON payload we created above
    json_schema = "user_id STRING, session_id STRING, event_name STRING, device_os STRING, app_version STRING, event_time TIMESTAMP"
    
    return (
        dp.read_stream("mobile_events_bronze")
        .withColumn("payload_str", col("value").cast("string"))
        .withColumn("json", from_json("payload_str", json_schema))
        .select(
            col("kafka_ingest_time"),
            col("json.user_id"),
            col("json.session_id"),
            col("json.event_name"),
            col("json.device_os"),
            col("json.app_version"),
            col("json.event_time")
        )
    )

# -------------------------------------------------------------------------
# GOLD: Daily Active Users (Real-time Aggregation)
# -------------------------------------------------------------------------
# Optional: Shows how streaming aggregation works alongside your batch tables
@dp.table(
    comment="Real-time count of events by type"
)
def mobile_events_summary_gold():
    return (
        dp.read_stream("mobile_events_silver")
        .groupBy(
            window("event_time", "1 hour"),
            "event_name", 
            "device_os"
        )
        .count()
    )