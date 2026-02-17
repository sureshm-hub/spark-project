# Databricks Interview Cheat Sheet

## Architecture & Basics

1.  Databricks is a unified data analytics platform built on Apache Spark.
2.  Lakehouse architecture combines data lake flexibility with warehouse reliability.
3.  Delta Lake adds ACID transactions to cloud object storage.
4.  Delta solves data corruption and schema drift issues.
5.  DBFS is a distributed file abstraction over cloud storage.
6.  Unity Catalog centralizes governance and metadata management.
6.  What are Workspaces?
    * Collaborative environments for notebooks, jobs, clusters, and data artifacts.
7.  A Metastore stores table metadata and schemas.

------------------------------------------------------------------------

## Delta Lake

8.  ACID guarantees ensure Atomicity, Consistency, Isolation, Durability.
9.  \_delta_log tracks all table changes.
10. Time Travel allows querying historical table versions.
    * version: SELECT * FROM table_name VERSION AS OF 1
    * Version numbers in a Delta table are automatically provided
    * as of:  SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01'
    * Restore: RESTORE TABLE your_table TO VERSION AS OF 1
    * A cloned Delta table gets its own new history starting at version 0, independent of the source table's versioning
    * eg: table changes that causes Deltas on a table
    
      | Operation               | Scope            |
      |--------------------------|------------------|
      | `df.write.mode("append")`    | Table           |
      | `df.write.mode("overwrite")` | Table / Partition |
      | `INSERT (SQL)`               | Table / Partition |
      | `UPDATE (SQL)`               | Row             |
      | `DELETE (SQL)`               | Row             |
      | `MERGE (SQL)`                | Row             |
    * Insert into as of ("Fixing accidental deletes to a table")
    ```sql 
      INSERT INTO my_table
      SELECT * FROM my_table TIMESTAMP AS OF date_sub(current_date(), 1)
      WHERE userId = 222
    ```
11. OPTIMIZE compacts small files.
12. Z-ORDER improves selective query performance.
    * **Implementation:** OPTIMIZE commands with a ZORDER BY clause
    * typically used for high-cardinality columns
    * **limitations:** resource-intensive operation 
    * Updates and inserts can degrade overtime
13. VACUUM removes old unreferenced files.
    * retention period ( 7 days)
15. What is Schema Enforcement?
    Rejecting writes that don’t match the expected schema.
16. What is Schema Evolution?
    Automatically adapting table schema during writes when enabled.
    **Methods to Enable Schema Evolution:**
    - DataFrame Write (.option): Use .option("mergeSchema", "true") during .write or .writeStream operations to add 
      new columns, which is ideal for Spark batch or streaming.
    - SQL Merge (WITH SCHEMA EVOLUTION): Use MERGE WITH SCHEMA EVOLUTION in SQL syntax for MERGE operations to 
      automatically adapt the table structure.
    - Session-Wide Configuration (AutoMerge): Set the Spark configuration 
       spark.databricks.delta.schema.autoMerge.enabled = true to enable automatic schema merging for all write 
      operations in a Spark session.
    - Table Properties (ALTER TABLE): Enable auto-merge for a specific table using 
      ALTER TABLE table_name SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true').
    **Key Considerations:**
    - Automatic Merging: When enabled, new columns present in the source data but missing in the target table are 
      automatically added.
    - Merge Priority: Options specified within a query (mergeSchema) take precedence over the Spark session
      configuration (autoMerge).
14. MERGE INTO performs atomic upserts
    * **Merge Syntax**
    ```sql  
        MERGE INTO target
        USING source
        ON source.key = target.key
        WHEN MATCHED
          UPDATE SET *
        WHEN NOT MATCHED
          INSERT *
        WHEN NOT MATCHED BY SOURCE
          DELETE
    ```
    * **Merge Dedup**
    ```sql  
        MERGE INTO target
        USING source
        ON source.key = target.key
        WHEN NOT MATCHED
          INSERT *
    ```
    * **SCD Type 2 (insert new change record):**
      * **Goal: keep full history of customer address changes.** 
         * If a customer’s current address changes:
           1. Expire the current row **(current=false, endDate = incoming.effectiveDate)**
           2. Insert a new current row with the new address **(current=true, endDate=null)**
         * If it’s a **new customer, just insert a current row.**
      * **_Assume:_**
         * customers has: (customerId, address, current, effectiveDate, endDate)
         * updates has: (customerId, address, effectiveDate)
      * **_Impl:_**
        * when merge key matches expire current record only 2 attribs set (set current = FALSE & endDate = s.
          effectiveDate)
        * when **merge key is null** or is a new record with no merge key Insert new records
    ```sql
    MERGE INTO customers c
    USING (
    -- Row type A: normal rows (match on customerId) -> used to EXPIRE current record if merge key  + INSERT if no 
    -- merge key
    SELECT customerId AS mergeKey, customerId, address, effectiveDate
    FROM updates
    
    UNION ALL
    
    -- Row type B: only for "changed address" customers -> used to INSERT new current record  by forcing 
    -- NULL mergeKey 
    SELECT NULL AS mergeKey, u.customerId, u.address, u.effectiveDate
    FROM updates u
    JOIN customers c
    ON c.customerId = u.customerId
    AND c.current = TRUE
    WHERE u.address <> c.address
    ) s
    ON c.customerId = s.mergeKey
    
    WHEN MATCHED
    AND c.current = TRUE
    AND c.address <> s.address
    THEN UPDATE SET
    c.current = FALSE,
    c.endDate = s.effectiveDate
    
    WHEN NOT MATCHED
    THEN INSERT (customerId, address, current, effectiveDate, endDate)
    VALUES (s.customerId, s.address, TRUE, s.effectiveDate, NULL);
    ```
    * **CDC to Delta Table**
      * Collapse multiple CDC events per key to the latest one
      * MERGE that latest state into the Delta table
        * Merge rules:
            - If key exists in target and source says deleted=true → DELETE
            - If key exists in target and not deleted → UPDATE value
            - If key doesn’t exist in target and not deleted → INSERT
      * **_Assume:_**
        * Target: delta_table(key, value)
        * CDC feed: changes(key, time, newValue, deleted)
    ```sql
    WITH latest AS (
    SELECT key, time, newValue, deleted
    FROM (
    SELECT
    key, time, newValue, deleted,
    ROW_NUMBER() OVER (PARTITION BY key ORDER BY time DESC) AS rn
    FROM changes
    )
    WHERE rn = 1
    )
    MERGE INTO delta_table t
    USING latest s
    ON t.key = s.key
    WHEN MATCHED AND s.deleted = true THEN DELETE
    WHEN MATCHED AND s.deleted = false THEN UPDATE SET t.value = s.newValue
    WHEN NOT MATCHED AND s.deleted = false THEN
    INSERT (key, value) VALUES (s.key, s.newValue);
    ```
15. Change Data Feed tracks row-level changes using **table_changes** functions
    ```sql
        CREATE TABLE my_table (id INT, name STRING)
        TBLPROPERTIES (delta.enableChangeDataFeed = true)

        ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    
        SELECT * FROM table_changes('my_table', start_version, end_version)

        df = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", version)
             .load("path/to/table")
    ```
    **Schema for Change Data Feed**
    - data columns from the schema of the Delta table 
    - **metadata** columns: 
      * _change_type (_insert, update_preimage , update_postimage, delete_), 
      * _commit_version,
      * _commit_timestamp
    - **Finding Specific Change Values:**
      - Filter for updates on _change_type to update_preimage and update_postimage.
      - Compare the rows "same original primary key and _commit_version": 
        - update_preimage row contains the "old" values, and  update_postimage row contains the "new" values.
      - Perform analysis: Join or compare the pre- and post-images to pinpoint exact column-level differences, 
        - for example, by selecting columns where the values differ between the pre- and post-image rows.
------------------------------------------------------------------------

## Performance & Optimization

16. Photon is a vectorized C++ engine for fast SQL queries.
17. AQE dynamically optimizes execution at runtime.
18. Small file problem is caused by frequent tiny writes.
19. Fix small files using OPTIMIZE or proper partitioning.
20. Data Skew is uneven distribution of keys.
21. Fix skew using salting or repartitioning.
22. Broadcast Join avoids shuffle for small datasets.
23. Repartition reshuffles; Coalesce reduces partitions without full shuffle.

------------------------------------------------------------------------

## Data Engineering

24. Auto Loader ingests files incrementally from cloud storage.
25. File notification mode uses cloud events instead of directory scans.
26. Structured Streaming runs micro-batch processing.
27. Checkpointing stores offsets and state for recovery.
28. Exactly-once guarantees no duplicate processing.
29. Bronze = raw data.
30. Silver = cleaned data.
31. Gold = aggregated business-ready data.

------------------------------------------------------------------------

## Security & Governance

32. Row-level security restricts row access.
33. Column-level security restricts column access.
34. Table ACLs define table permissions.
35. Data lineage tracks data origin and transformations.

------------------------------------------------------------------------

## Infrastructure

36. All-purpose clusters are interactive; job clusters are scheduled.
37. Autoscaling adjusts cluster size dynamically.
38. Cluster policies enforce configuration rules.
39. Reduce cost by right-sizing clusters and optimizing workloads.

------------------------------------------------------------------------

## L5 Rapid Fire

40. Delta over Parquet because of ACID and time travel.
41. Avoid partitioning on high-cardinality columns.
42. Biggest anti-pattern is over-partitioning.
43. Design scalable pipelines using incremental processing.
44. Handle late data using watermarks and MERGE logic.
45. Ensure reliability via checkpointing and monitoring.