What is Shuffle?
    - Certain operations, such as join() and groupByKey(), require Spark to perform a shuffle
    - Shuffles is a costly data redistribution across executors:
        Disk I/O
        Network I/O
        CPU & Memory load
    - Storing the intermediate data, it can exhaust space on the executor's local disk, which causes the Spark job to fail.

How?
    - shuffle is split into 2 phases:
    -   map-side (writing intermediate data to local disk, often with pre-aggregation): writing of sorted, partitioned data to disk by mappers
        - When map tasks finish, they don't just hold data in memory; they organize it by the target partition/key and write it to local disk files (one file per reducer).
        - Key Actions:
            In-memory buffer: Data is buffered in memory and sorted by key.
            Spilling: If buffer fills, data is spilled to disk.
            Partitioning: Data is partitioned by hash (e.g., hash(key) % num_partitions).
            Map-side pre-aggregation: Operations like reduceByKey aggregate data before writing to disk, reducing shuffle volume.
            Result: Intermediate files (shuffle files) containing key-value pairs are created on the local disks of the map executors.
    -   reduce-side (fetching, merging, and processing shuffled data): is the reading, merging, and processing of these files by reducers
        - Key Actions:
            Fetch: Reducers fetch relevant data blocks from various map-side shuffle files.
            Merge & Sort: Reducers merge the incoming data streams, sorting them by key.
            Aggregation: The final processing (e.g., summing, counting) happens on this merged, sorted data.
            Result: The final processed data, grouped by key, ready for the next stage or output.

Partitions
    -   File read: partitions are tied to: file size & maxPartitionBytes.
            numInputPartitions ≈ ceil(totalSize / maxPartitionBytes)
            ex: orders.csv ~ 250MB with spark.sql.files.maxPartitionBytes = 128MB  -> 2 partitions
                dataset.rdd().getNumPartitions()
    -   In-memory local collection: partitions are tied to your explicit repartition calls 
            (or spark.default.parallelism / spark.sql.shuffle.partitions depending on the op).
            What happens:
                1.  Spark starts the query with 16 planned shuffle partitions (from spark.sql.shuffle.partitions).
                2.  At runtime, AQE looks at actual data size after map side, and may merge small partitions.
                    *   Example: 16 initial partitions → AQE decides only 3 or 4 are necessary to hit ~64MB each.
                3.  In the SQL tab / DAG:
                    *   You’ll see a note about “AdaptiveSparkPlan”.
                    *   In the Stages view, shuffle stages might have fewer tasks than 16, despite the static setting.
    -   repartition:
            ```java
                        example: Dataset<Row> ordersRepart  = orders.repartition(32); // wide shuffle
                        System.out.println("After repartition(32): " + orders.rdd().getNumPartitions());
            ```
            There will be a stage where repartition(32) happens:
            *   Shuffle write: many output partitions.
            *   Number of tasks: 32.
    -   coalesce:
            ```java
                        // Now maybe we want to reduce tiny tasks (common after filters):
                        Dataset<Row> ordersCoalesced = ordersRepart.coalesce(8); // narrow shuffle (usually)
                        System.out.println("After coalesce(8): " +ordersCoalesced.rdd().getNumPartitions());
            ```
            *   Often remains a narrow dependency (just reassigning partition IDs), so it may not show as a separate shuffle.
            *   But the resulting RDD/DataFrame now reports 8 partitions when you call .rdd().getNumPartitions(). 
    -   Executors: Executors are the JVMs that run tasks. Each executor has:
        ```conf
            spark.executor.memory (heap for JVM)
            spark.executor.cores (# tasks it can run at once)
        ```
        Why partitions matter for memory:
            Each task processes 1 partition at a time. For a given executor:
                Max concurrent tasks ≈ executor_cores
                So max in-flight partitions per executor = executor_cores.
        Each partition should be small enough that one task’s working set fits in heap comfortably:
          That’s why we talk about 64–256MB target sizes a lot.
                Too few partitions: Some cores idle, slow job.
                Too many tiny partitions: Huge scheduling overhead, lots of task launch time.
        AQE tries to merge them.
        ```conf In a real cluster you’d tune:
            spark.executor.instances
            spark.executor.cores
            spark.executor.memory
            spark.sql.shuffle.partitions
            spark.sql.files.maxPartitionBytes
            spark.sql.adaptive.* (for AQE)
        ```
    - Overcome data skew
        On the Stage tab in the Spark UI, examine the Event Timeline page. You can see an uneven distribution of tasks
        Another important page is Summary Metrics, which shows statistics for Spark tasks with percentiles for Duration,
        GC Time, Spill (memory), Spill (disk), and so on
        Use keys with a large range of values for the join keys. In a shuffle join, partitions are determined for each
        hash value of a key. 
        If a join key's cardinality is too low, the hash function is more likely to do a bad job of distributing your 
        data across partitions. Therefore, if your application and business logic support it, 
        consider using a higher cardinality key or a composite key.
        ```java 
            # Use Single Primary Key
            df_joined = df1_select.join(df2_select, ["primary_key"]) 
            # Use Composite Key
            df_joined = df1_select.join(df2_select, ["primary_key","secondary_key"])
        ```

Joins
    -   Shuffle join:
        Shuffle Hash Join: 
        The shuffle hash join joins two tables without sorting and distributes the join between the two tables. 
        It's suitable for joins of small tables that can be stored in the Spark executor's memory.
        Sort-merge join: 
        distributes the two tables to be joined by key and sorts them before joining. 
        It's suitable for joins of large tables.
    -   Broadcast hash join:
        A broadcast hash join pushes the smaller RDD or table to each of the worker nodes. 
        Then it does a map-side combine with each partition of the larger RDD or table.
    - Bucketing:
        The sort-merge join requires two phases, shuffle and sort, and then merge. These two phases can overload the 
        Spark executor and cause OOM and performance issues when some of the executors are merging and others are 
        sorting simultaneously. 
        In such cases, it might be possible to efficiently join by using bucketing.
        Bucketing will pre-shuffle and pre-sort your input on join keys, and then write that sorted data to an
        intermediary table. 
        The cost of the shuffle and sort steps can be reduced when joining large tables by defining the sorted
        intermediary tables in advance.
        Use bucketing only if you have:
            Hive metastore (Spark + Hive catalog)
            Large dimension/fact stable tables
            Repeatable joins where pre-shuffling is worth the cost
            Long-lived tables (warehouse/lakehouse)
            NOT recommended for ad-hoc Spark jobs or dynamic pipelines.
        you can still manually coalesce & repartition:
            Use this instead (recommended for non-Hive Spark apps):
            ```java
                Dataset<Row> left  = df1.repartition(200, col("key"));
                Dataset<Row> right = df2.repartition(200, col("key"));
                left.join(right, "key");
            ```

Example: proj.hobby.bigdata.rdd.tx.ShuffleDemo
    -   Read the orders data and repartition into 4 (4 partitions = 4 tasks)
    -   Read the customers data and repartition into 4 (4 partitions = 4 tasks)
    -   On OrdersData Apply transformations (filter and withColumn flow through without shuffling)
    -   join with Customers data on customer_id
    Stage 1: Map Side (Shuffle Write). Here’s what actually happens in the first stage:
            Hash the join key to determine target partitions: partition_id = hash(customer_id) % spark.sql.shuffle.partitions
            Sort and write shuffle data to local disk on each executor
            Note this data doesn't stay in memory, it is written to disk
            The file structure:
               spark.local.dir/blockmgr-{uuid}/
                 shuffle_0_0.data    ← All partitions from map task 0, concatenated
                 shuffle_0_0.index   ← Byte offsets for quick lookup
                 shuffle_0_1.data    ← All partitions from map task 1
                 shuffle_0_1.index
                 shuffle_0_2.data    ← All partitions from map task 2
                 shuffle_0_2.index
            If spark.sql.shuffle.partitions = 8 and you have 4 input partitions:
                Each of the 4 tasks writes 2 files (index and data files)
                The .index file stores byte offsets so reduce tasks can quickly find their data
                Total: 8 shuffle files from the orders side alone
                The same process happens for customers (1 partition × 2= 2 more files).
            Note: Each .data file contains output for all 8 target partitions, concatenated together.
            This shuffle write marks the end of Stage 1.
    Stage 2: Reduce Side (Shuffle Read). Now Stage 2 begins:
            Shuffle read: Spark creates 8 tasks (one per shuffle partition)
            Each task reads its assigned partition from "Map Side Shuffle"/previous tasks
            If files are on remote executors, data transfers over the network (Network I/O costs)
            Files are read from spark.local.dir into memory (Disk I/O costs)
            Sort-merge join:
                Both datasets are sorted by customer_id
                Matching keys are merged
                All rows with the same customer_id are guaranteed to be in the same partition
            Next operations flow through until another shuffle or action

Takeaway:
    1. Shuffle Files Live on Disk (spark.local.dir)  & Not Memory, then read back into memory. This provides:
        Fault tolerance (tasks can retry without recomputing)
        Memory relief (data doesn’t all fit in RAM)
        Persistence across stage boundaries
    2. The Map/Reduce Split is Literal: shuffle files on disk are the physical boundary between stages.
        Stage 1 (map): Process data → write shuffle files → stage ends
        Stage 2 (reduce): Read shuffle files → continue processing
        The files persist on disk until the application completes.
    3. Each Shuffle Gets a Unique ID
        If your job has multiple shuffles:
        result =
                orders
                .join(customers, "customer_id")      # shuffle_0
                .groupBy("country").agg(sum("amount_with_tax")) # shuffle_1
                .orderBy("total")                      # shuffle_2
        You’ll see:
            spark.local.dir/blockmgr-abc123/
              ├── shuffle_0_0.data    ← All in same directory
              ├── shuffle_0_0.index
              ├── shuffle_0_1.data
              ├── shuffle_0_1.index
              ├── shuffle_1_0.data
              ├── shuffle_1_0.index
              ├── shuffle_1_1.data
              ├── shuffle_1_1.index
              ├── shuffle_2_0.data
              ├── shuffle_2_0.index
              └── ...
            Each shuffle operation creates its own set of files.
    4. Number of Tasks = Number of Partitions
        In shuffle write stage: Tasks = input partitions (4 in our example)
        In shuffle read stage: Tasks = spark.sql.shuffle.partitions (default 200)
        Tuning Guideline:
            Too few (e.g., 4 on an 8-core cluster) = cores sit idle
            Too many (e.g., 1000 on an 8-core cluster) = task overhead dominates
        Rule of thumb: 2–4 x your number of cores
        # For 8 cores:
        spark.conf.set("spark.sql.shuffle.partitions", "32")

Optimization & Monitoring:
    Configure adequate disk space for Executors & Monitor disk usage
        - If spark.local.dir fills up, executors crash (Shuffle Disk Space Matters for Large jobs  & can generate GB's of shuffle files).
    Consider broadcast joins for small tables
    Spark UI metrics: “Shuffle Read” vs “Shuffle Read (local)”
    Minimizing shuffle size and count (filter early, project only needed columns)
    ```java    
        // Default
        df_joined  = df1.join(df2, ["product_id"])

        // Use Pushdown
        df1_select = df1.select("product_id","product_title","star_rating").filter(col("star_rating")>=4.0)
        df2_select = df2.select("product_id","category_id")
        df_joined  = df1_select.join(df2_select, ["product_id"])
    ```

Resources:
    https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html
    https://medium.com/@sairam94.a/what-i-learned-about-spark-shuffles-after-8-years-of-writing-production-jobs-33d454c92150
