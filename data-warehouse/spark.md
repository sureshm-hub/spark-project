# Spark Transformations – One‑Line Interview‑Ready Answers

## Core Spark Transformations – Fundamentals
- **Transformation**: A lazy operation that defines how data is transformed, not executed.
- **Transformation vs Action**: Transformations build lineage; actions trigger execution.
- **Lazy or eager**: Transformations are lazy to enable optimization and avoid unnecessary work.
- **Internal behavior**: Spark builds a logical DAG until an action is called.
- **Failure timing**: Errors surface only during action execution.

## Narrow vs Wide Transformations
- **Narrow transformation**: Each output partition depends on a single input partition.
- **Wide transformation**: Output partitions depend on multiple input partitions.
- **Why wide is expensive**: It requires a shuffle across the network.
- **Examples**: Narrow – map, filter; Wide – groupBy, join.
- **Shuffle impact**: Wide transformations trigger shuffle read/write.
- **Spark UI identification**: 
  * Stages page -> Stage boundaries indicate wide transformations. Each Stage has DAG Visualization expansion as well
  * ### Shuffle Boundary (Map-side vs Reduce-side)
    - **Map-side processing** happens locally on executors before data is shuffled, using partial aggregation (e.g., `reduceByKey`) to reduce shuffle volume.
    - **Reduce-side processing** occurs after a shuffle, where all values for a key are brought together on a node for final computation.
    - **Map-side joins (Broadcast Joins)** avoid shuffles by broadcasting small tables to all executors
    - **Reduce-side joins (Sort-Merge Joins)** handle large datasets by shuffling data to co-locate keys
- **AQE role**: Dynamically optimizes shuffle partitions and join strategies.
  * coalesce smaller partitions to reduce small task overhead
  * after shuffle analyze partition size and splits to handle data skew
  * Dynamic Partition pruning during joins  
    * static ->  WHERE date = '2026-01-01'
    * Dynamic JOIN with a table filtered by region='West'
    * e.g., When joining a 1TB sales_transactions table (partitioned by date) with a small store_details table, DPP 
      ensures only partitions containing the relevant store_id (e.g., in 'WEST' region) are read, rather than 
      scanning the entire 1TB
  * Dynamically Switching Join Strategies e.g., switch from a Sort-Merge Join to a more efficient Broadcast Hash Join

## RDD vs DataFrame vs Dataset
- **RDD-only transformations**: mapPartitions, reduceByKey (pair RDD's), flatMap (available for dataset also).
  * An RDD is a collection of partitions, not a single partition itself
- **Key difference**: DataFrames are schema-aware and optimized.
- **Why DataFrames are faster**: Catalyst and Tungsten optimize execution.
- **Catalyst optimizer**: Produces efficient logical and physical plans.
- **Tungsten Engine Features**: As the "executor," Tungsten focuses on maximizing CPU and memory
- **Mixing APIs**: Possible, but converting breaks optimizations.
- **Calling df.rdd**: Loses Catalyst optimizations. (is a transformation as lazy evaluation)
- **Dataset vs DataFrame**
  - Dataset<Row> 
    - Alias: DataFrame
    - Generic Row objects
    - Typing: Dynamically typed, only errors at runtime
    - API Style	Untyped, expression-based
    - Convert a DataFrame to a typed Dataset using methods like **.as[T]()**
  - Dataset<Type>
    - Typed Dataset
    - Static (compile-time safety)
    - Specific JVM objects
    - Typed, object-oriented
    - Convert a typed Dataset to a DataFrame using **.toDF()**

## Common DataFrame Transformations
- **select vs withColumn**: select projects columns; withColumn adds or replaces.
- **withColumn type**: Narrow transformation.
- **Repeated withColumn issue**: Creates deep, inefficient DAGs.
- **filter vs where**: No difference; where is an alias.
- **drop() behavior**: Removes column metadata.
- **distinct() internals**: Shuffle plus aggregation.
- **dropDuplicates vs distinct**: dropDuplicates can target specific columns.

## Aggregations & Grouping
- **groupBy internals**: Shuffle followed by aggregation.
- **Why shuffle occurs**: Rows must regroup by key.
- **groupBy vs window**: groupBy reduces rows; windows preserve rows.
- **When to use window**: When row-level context is needed.
- **reduceByKey vs groupByKey**: reduceByKey aggregates before shuffle.
- **Why reduceByKey is better**: Less shuffle data.

## Joins
- **Join internals**: Shuffle or broadcast followed by key matching.
- **Broadcast vs shuffle join**: Broadcast avoids shuffle.
- **Join strategy decision**: Based on size and configuration.
- **SortMergeJoin**: Sorts both sides then merges.
- **Skew cause**: Uneven key distribution.
- **Salting**: Spreads skewed keys artificially.
- **AQE benefit**: Changes join strategy at runtime.
- **Join always wide**: Yes, unless broadcast.

## Partitioning & Repartitioning
- **repartition vs coalesce**: repartition shuffles; coalesce usually doesn’t.
- **repartition type**: Wide transformation.
- **Why repartition shuffles**: Redistributes data across executors.
- **When to use coalesce**: When reducing partitions safely.
- **Partition impact**: Affects parallelism and shuffle cost.
- **Ideal partition count**: 2–4× total CPU cores 
  * maximize resource utilization, ensure balanced workloads, and optimize task scheduling.
  * 1 vcpu = 1 core
  * 1 core = 1 hyper thread (or 2 cores = 1 physical cpu)
  * if hyper threading not involved 1 vcpu = 1 core = 1 physical cpu
  * 1 master and 5 core nodes r7a 8X Large - 32 vCPU, 256 GB-> 192 vCPU + 1.5 TB

## Set & Row-Level Transformations
- **union vs unionByName**: union matches position; unionByName matches names.
- **union internals**: Concatenates partitions without shuffle.
- **union type**: Narrow transformation.
- **explode vs flatMap**: explode is SQL & Dataset level; flatMap is RDD & Dataset level.
- **explode effect**: Multiplies rows.
- **explode risk**: Data explosion and OOM.

## Caching, Lineage & Checkpointing
- **Is cache a transformation**: No, it’s a persistence hint
  * it is not a transformation as it doesn't generate a new dataset but also doesn't trigger action immediately
- **cache vs persist**: persist supports multiple storage levels.
- **Caching benefit**: Avoids recomputation.
- **Executor failure**: Cached data may be recomputed.
  - 1 spark executor is 1 jvm launched on a worker node
- **Lineage role**: Enables fault-tolerant recomputation.
- **Checkpoint usage**: Cuts long lineage chains.

## Optimization & Internals
- **Stage creation**: DAG is split at shuffle boundaries.
- **Stage boundaries**: Defined by wide transformations.
- **Predicate pushdown**: Filters applied at the data source. e.g., Query: ... WHERE age > 30;
- **Column pruning**: Reads only required columns.
- **Avoiding UDFs**: They block Catalyst optimization.
- **Python UDF cost**: Serialization and Python worker overhead.

## Scenario & Debugging
- **distinct slowdown**: Full shuffle and aggregation.
- **groupBy OOM**: Data skew or insufficient memory.
- **Pipeline refactor**: Filter early and reduce shuffles.
- **Debugging slowness**: Use Spark UI stage and task metrics.
- **Reduce shuffle size**: Pre-aggregate and repartition smartly.
- **Order matters**: Early filters reduce downstream data.

## Sizing Cluster & Spark Applications
- configure --num-executors, --executor-cores, and --executor-memory
- Hadoop Overhead:
  - Hadoop/YARN Daemons: 1 core per node + 7% of executor memory (e.g., executor is 20 GB YARN request: 21.4 GB) 
  - ClusterManager: Special Node powerful enough to run cluster
  - AM: manages resources and coordinates tasks across cluster
  - HDFS: Disk size
  - 1 core per node goes for background process + 1 GB for OS memory 
- Sample Configurations:
  - e.g., cluster: 10 Node cluster with 1 Node  16 cores + 64 GB
  - if 1 executor per core => 160 executors => task parallelism overhead
  - if 1 executor per node => 10 executor & 1 executor has 64 GB => GC overhead
  - middle ground: minimize task parallelism & gc overhead
    - 1 executor per 5 core 
    - number of executors = 150/5 = 30
      - 10 cores total - 1 per core for background process
    - AM runs in its own YARN container (not as executor) but ~ executor capacity leaving 29 executors for spark app
    - 3 executors per node
    - memory per executor = 63 GB/3 = 21 GB. After factoring memory overhead(caching) 7%, set executor memory as 18 GB
  - Final Configuration: --num-executors=29 --executor-cores=5 --executor-memory=18G
  - so how many tasks can run in parallel & what size of data can they process?
    - 29 executors * 5 tasks per executor = 145 tasks
    - Recommended partitions = 2–4 × 145 = 290–580 partitions
    - If each partition is ~256 MB, order of input/shuffle data = 74–149 GB
  - Can a 1 TB job run on this cluster?
    - absolutely! 75-150 GB is neatly sized partitions for single pass
    - for 1 TB we have 4k-5k partitions of 200-256 MB size
    - so for the 4k-5k partitions to process there will be many more waves of tasks ~ 35 waves of tasks to finish a 
      full stage shuffle
    - The job will take proportionally longer, but a valid configuration for 1 TB as long as:
      - Per-partition size stays in a safe range (100–256 MB)
      - skewed keys do not cause single partitions to blow up in memory
      - it can fail or crawl is when the working set (especially shuffle + skew + wide aggregations + joins) 
         becomes too large for your executor memory/disk/I/O
      - For heavy shuffle (**big groupBy, sort, wide joins**), 1 TB can still be doable, but you’ll likely see:
        - shuffle spill to disk 
        - long straggler tasks if skew
        - possible OOM / executor lost if partitions are too big or overhead is under-sized
      - The real limiter isn’t 1 TB, it’s shuffle & skew
        - 1 TB groupBy / distinct / global sort => huge shuffle, spill, and **potentially multi-hour stages.**
        - Large join where both sides are big and not well-partitioned -> **massive shuffle read/write.**
        - Skewed keys -> a few reducers get gigantic partitions -> those **tasks OOM or run forever.**
      - adjustments to config
        - Use AQE (Spark 3+)
        - Guard against skew:
          - spark.sql.adaptive.skewJoin.enabled=true
          - or salting / pre-agg where needed
        - Increase memory overhead spark.executor.memoryOverheadFactor closer to 0.10–0.20
        - Ensure local disk is fast and ample (spill + shuffle): NVMe/SSD matters a lot. 

## Spark/YARN components:
- Cluster Manager
  - resource allocation
  - Dedicated Master Node
- ApplicationMaster
  - resource negotiation
  - A Worker Node (Cluster Mode)
  - Manages Task Execution
- Driver
  - Task Scheduling/Execution
  - Inside AM or Client Machine
  - Creates Tasks/Logic
- **Interaction Scenario**
  - Driver (Client) tells the Cluster Manager to start the application.
  - Cluster Manager starts the Application Master.
  - Application Master asks Cluster Manager for resources (Executors).
  - Driver tells Executors what tasks to run.