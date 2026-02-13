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
- **What is RDD?**: RDD is spark's data abstraction concept represents a fault tolerant collection of partitions.
  - they are immutable, lazily evaluated & distributed across executor nodes for parallel processing
- **RDD-only transformations**: mapPartitions, reduceByKey (pair RDD's), flatMap (available for dataset also).
  * An RDD is a collection of partitions, not a single partition itself
- **Key difference**: DataFrames are schema-aware and optimized.
- **Why DataFrames are faster**: Catalyst and Tungsten optimize execution.
- **Catalyst optimizer**: Produces efficient logical and physical plans (ex: predicate pushdown, column pruning, 
  join optimzation)
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

## DAG, Caching, Lineage & Checkpointing
- **What is DAG**: DAG is a logical graph that represents spark application transformations on RDDs or DataFrames, 
  where each vertex is an RDD and edges represent transformations.
- **Is cache a transformation**: No, it’s a persistence hint
  * it is not a transformation as it doesn't generate a new dataset but also doesn't trigger action immediately
- **cache vs persist**: persist supports multiple storage levels.
- **Caching benefit**: Avoids recomputation.
- **Executor failure**: Cached data may be recomputed.
  - 1 spark executor is 1 jvm launched on a worker node
- **Lineage role**: Enables fault-tolerant recomputation.
- **Checkpoint usage**: Cuts long lineage chains.
- **What is Lineage**: Data lineage refers to the metadata that records the history of transformations that were 
  applied to create an RDD, allowing Spark to recompute lost rdd's in case of failures, making it fault-tolerant.

## Optimization & Internals
- **Stage creation**: DAG is split at shuffle boundaries.
- **Stage boundaries**: Defined by wide transformations.
- **Predicate pushdown (Catalyst)**: Filters applied at the data source. e.g., Query: ... WHERE age > 30;
- **Column pruning (Catalyst)**: Reads only required columns.
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

## Lambda & Kappa Architecture
  - Streaming systems are fundamentally trying to answer one hard question:
    - How do I get **low-latency results and correct, recomputable results at scale**?
    - **Lambda and Kappa** are two different answers to that same problem.
  - **Lambda Architecture (Two parallel paths):**
    - One path optimized for accuracy
    - One path optimized for latency 
    - Then merge them at query time
  - The 3 layers:
    - Batch Layer (Accuracy)
      - Processes all historical data
      - Recomputes results from scratch
      - Source of truth
      - Tech:
        - Spark batch on EMR / Databricks
        - Daily / hourly runs (T+1, T+2)
    - Speed Layer (Low latency)
      - Processes only new events 
      - Near-real-time updates
      - Eventually **overwritten by batch results**
      - Tech Stack:
        - Spark Streaming / Structured Streaming
        - Kafka Streams / Flink
        - Stateful streaming jobs
      - Serving Layer:
        - Merges batch + speed views
        - Answers queries
        - Tech:
          - Pre-aggregated tables in Snowflake
          - Elasticsearch
  - **Kappa Architecture**
    - There is only one path: streaming.
      - If you need to recompute? Replay the stream from the beginning.
    - Architecture flow:
      - Immutable event log (Kafka is the system of record)
      - Single streaming pipeline (Same code handles real-time + reprocessing)
      - Serving layer (Same as Lambda)
    - Tech Stack:
      - Structured Streaming reading Kafka 
      - Checkpointed state
      - Reprocessing = reset offsets + re-run job
    - Kappa simplifies Lambda by eliminating the batch layer and treating the event log as the source of truth, relying 
      on stream replay for reprocessing.
  - Where watermarking & state fit
    - Lambda 
      - Watermarking mostly in speed layer 
      - Batch layer "fixes" late data eventually
    - Kappa
      - Watermarking is critical
      - State eviction depends on event-time guarantees 
      - Late data past watermark = dropped unless replayed

## Spark Streaming

## 1. Core Concepts vs Batch

- **Unified API**
  - Structured Streaming uses the same DataFrame/Dataset and SQL APIs as batch; you define a long‑running query over an unbounded table instead of running one‑off jobs.[web:13]
  - Transformations (select, filter, join, agg) are mostly identical; differences are in sources, sinks, triggers, and output modes.[web:13]

- **Micro‑batch and continuous execution**
  - Default engine runs in micro‑batches: each trigger picks new data, builds a micro‑batch, and runs the plan incrementally.[web:13]
  - Continuous processing mode targets very low latency, but with more constraints and less common in production.[web:4]

- **Streaming query lifecycle**
  - You define a streaming DataFrame with `readStream`, write it with `writeStream`, specify output mode, trigger, checkpoint, and call `start()` to run it until stopped.[web:13]
  - The engine tracks offsets/progress and updates results incrementally rather than recomputing from scratch.[web:13]

- **Output modes**
  - Append: only new rows since last trigger are written; appropriate for append‑only queries with no updates (e.g., simple ETL).[web:13]
  - Update: only changed rows are written; useful for aggregations where results evolve over time.[web:13]
  - Complete: the entire result table is written every trigger; used mainly for aggregations to sinks that can handle full rewrites.[web:13]

- **Key mental model shift**
  - Batch: “Run this job over a fixed snapshot and finish.”
  - Streaming: “Maintain this **incremental** query over an unbounded table with specified latency and correctness guarantees.”[web:13]

---

## 2. Stateless vs Stateful Streaming

- **Stateless operations**
  - Operate per record or per batch with no dependence on historical data (e.g., select, filter, simple map).[web:11][web:16]
  - Easy to scale and reason about, no state store growth, and recovery is simpler since only offsets matter.[web:11]

- **Stateful operations**
  - Maintain intermediate state across micro‑batches to support aggregations, windowing, deduplication, and stream–stream joins.[web:11][web:16]
  - State lives in a state store (e.g., RocksDB‑backed or similar), usually on local disk + memory cache, and is tracked per key.[web:11][web:16]

- **Examples of stateful operators**
  - Grouped aggregations (`groupBy` + `agg` on streaming DataFrames).[web:16]
  - Windowed aggregations (`groupBy(window(...), key)`), session windows, and deduplication with event‑time + watermark.[web:16]
  - Stream–stream joins and `mapGroupsWithState` / `flatMapGroupsWithState` for custom per‑key state.[web:16]

- **Challenges and risks**
  - Unbounded state growth can lead to OOM, long GC pauses, large checkpoints, and slow recovery.[web:16]
  - State skew from hot keys (e.g., one customer id with huge volume) can create severe performance hotspots.[web:16]

- **Mitigation strategies**
  - Use watermarks and timeouts to bound how long state is kept.[web:16]
  - Design keys to avoid extreme skew and apply salting where necessary.[web:16]
  - Monitor state metrics such as `stateOperators.numRowsTotal` and checkpoint size to catch issues early.[web:15][web:16]

---

## 3. Event Time, Watermarks, and Late Data

- **Event time vs processing time**
  - Event time: timestamp embedded in the data indicating when the event actually occurred (e.g., click time).[web:13][web:18]
  - Processing time: time when Spark receives and processes the record; affected by network, upstream lags, etc.[web:13]

- **Why event time matters**
  - For analytics like “per 5‑minute window by event time,” you want windows aligned to when events occurred, not when they arrived.[web:13]
  - Late events (arriving after their natural window) must be handled explicitly to avoid wrong results or unbounded state.[web:18]

- **Watermark concept**
  - A watermark is a threshold that says: “Data older than max(event_time_seen) minus delay is considered too late and can be dropped, and state for those windows can be cleaned up.”[web:16][web:18]
  - Configured via `withWatermark(eventTimeColumn, delay)` on streaming DataFrames.[web:16]

- **How watermarks work internally**
  - At the end of each micro‑batch, Spark computes the maximum event time seen so far and subtracts the configured delay to derive the watermark.[web:16][web:15]
  - For each batch, records with event time older than the watermark can be dropped, and state (e.g., old windows) older than watermark is evicted.[web:16][web:15]

- **Multiple streams and global watermark**
  - In stream–stream joins, Spark tracks watermarks per input and chooses a global watermark, typically the minimum across inputs to ensure correctness.[web:13][web:16]
  - This means the overall query advances at the pace of the slowest stream to avoid prematurely dropping data.[web:13]

- **Trade‑offs when choosing watermark delay**
  - Larger delay: better tolerance for late events but more state and slower cleanup.[web:16][web:18]
  - Smaller delay: smaller state and faster cleanup but more late data dropped and potential accuracy loss.[web:16][web:18]

---

## 4. Streaming Joins

- **Stream–static (stream–batch) joins**
  - Stream side is unbounded, static side is a dimension table or reference dataset.[web:13]
  - Often behaves like a stateless join since static data doesn’t change per micro‑batch; implemented as broadcast join or regular join in each batch.[web:13]

- **Stream–stream joins**
  - Both inputs are unbounded streams; Spark must keep unmatched records from both sides in state until they either match or expire.[web:13][web:16]
  - This is always stateful and can be heavy if not properly time‑bounded.[web:16]

- **Time‑bounded join conditions**
  - Use event‑time columns on both sides with conditions like `streamA.time BETWEEN streamB.time - interval AND streamB.time + interval`.[web:16][web:18]
  - Combine with watermarks on both inputs so Spark can evict join state when records are older than watermark + join window.[web:16][web:18]

- **Supported join types (Structured Streaming)**
  - Inner, left/right outer, full outer, and left semi joins are supported with some constraints based on output mode and state semantics.[web:13]
  - Outer joins require careful watermarking and time conditions so that unmatched records can eventually be emitted and state cleared.[web:13][web:16]

- **Why watermarks are critical for joins**
  - Without watermarks and time bounds, state for stream–stream joins grows without limit, risking memory and performance issues.[web:16]
  - Watermarks signal when it is safe to drop old join keys because any matching partner would be considered too late and dropped anyway.[web:16]

---

## 5. Checkpointing, State, and Fault Tolerance

- **Checkpoint directory**
  - Stores offsets, committed progress, streaming query metadata, and state store information necessary for recovery.[web:13]
  - Must be on reliable storage (e.g., HDFS, S3) and remain stable across restarts to ensure correct resume behavior.[web:13]

- **Fault tolerance semantics**
  - Structured Streaming provides end‑to‑end exactly‑once processing for supported sinks, given idempotent or transactional sink behavior.[web:13]
  - On failure and restart with the same checkpoint, Spark replays from last committed offsets and reconstructs state so results are consistent.[web:13]

- **State and checkpoint interaction**
  - Stateful operators persist state snapshots along with offsets; the combination defines the logical point in the stream up to which results are committed.[web:16]
  - Large or skewed state increases checkpoint size and recovery time, so watermarking and state cleanup are essential.[web:16]

- **Operational considerations**
  - Never reuse the same checkpoint directory for different queries; doing so can corrupt state and progress tracking.[web:13]
  - When changing query logic in a state‑incompatible way (e.g., different key semantics), typically start with a fresh checkpoint and reprocess if needed.[web:13]

---

## 6. Triggers, Throughput, and Optimization

- **Triggers overview**
  - Trigger.ProcessingTime(interval): schedules micro‑batches at a fixed interval (e.g., every 10 seconds).[web:17]
  - Trigger.Once / AvailableNow: process all available data in one go then stop; useful for catch‑up or incremental batch workloads over streaming sources.[web:14][web:17]
  - Continuous trigger: runs continuously with very low latency constraints; limited features and less widely used.[web:17]

- **Latency vs throughput trade‑off**
  - Shorter trigger intervals reduce latency but may increase overhead per batch and pressure on cluster resources.[web:17]
  - Longer intervals allow larger micro‑batches and better throughput but higher end‑to‑end latency.[web:17]

- **Input rate and backpressure controls**
  - For Kafka and similar sources, you can control read rate via options like `maxOffsetsPerTrigger` to avoid overloading the job.[web:13]
  - Monitor metrics such as batch duration vs trigger interval to detect when the job is falling behind.[web:15]

- **Optimization techniques**
  - Tune partitions and shuffle: avoid excessive small partitions, but also avoid huge skew; use `repartition` or `coalesce` judiciously.[web:16]
  - Reduce state size: design queries with appropriate watermarks, timeouts, and pruning logic to keep state bounded.[web:16]
  - Leverage foreachBatch for complex sinks: handle writing per micro‑batch so you can batch I/O, manage transactions, or deduplicate before writing.[web:4]

- **Practical tuning loop**
  - Monitor micro‑batch duration, input rates, state size, and watermark progress in the UI and logs.[web:15][web:16]
  - Adjust trigger interval, input rate, partitions, and state configuration iteratively until batch duration consistently stays below trigger interval.[web:15][web:16]

---