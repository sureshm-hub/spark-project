# Avro
Avro is a row-based storage format optimized for **write-heavy** workloads and streaming. 
It supports strong schema evolution, making it ideal for Kafka pipelines, event data, and data exchange between systems.
Use Avro for ingestion and streaming pipelines.

# Parquet
Parquet is a columnar storage format optimized for **read-heavy** analytics. 
It provides better compression and query performance (especially for Spark, Hive, Snowflake) because it reads only the required columns.
Use Parquet for analytics, reporting, and data lakehouse storage.