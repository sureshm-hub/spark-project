Transformations:
    Narrow
        - work on same partition with no shuffle
        - ex: map, flatMap, filter, mapPartitions, union, coalesce, withColumn
    Wide
        - require data from other partitions and causes shuffle and triggers a new job Stage
        - ex: reduceByKey, groupByKey, distinct, sortByKey, join, intersection, Repartition

Tx's, Action's, DAG's Stages, Jobs:
    DAG:
        A DAG (Directed Acyclic Graph) = all your transformations linked together.
        Spark builds this DAG lazily and optimizes it before running.
        You can see it in the Spark UI (http://localhost:4040).
    How Spark breaks down execution:
        Narrow transformations
            --> Each partition’s output depends only on one parent partition.
            --> All narrow ops stay in the same stage.
        Wide transformations
            --> Require shuffle (data exchange across partitions).
            --> Mark a stage boundary; Spark cuts the DAG and creates a new stage.
        Each stage runs a set of tasks (one per partition).
        Each job is triggered by an action (e.g., collect, take, count, saveAsTextFile).
    Execution hierarchy
        Application
        ├── Job 0, Job 1, Job 2, ...
        │     ├── Stage 0, Stage 1, Stage 2, ...
        │     │     ├── Tasks on partitions
        ✅ Job numbering starts at 0
        ✅ Stage numbering also starts at 0 (not 1)
        In your Spark UI you’ll see Job 0 --> Stage 0, then Job 1 --> Stage 1, Stage 2, etc.
        Each Action = 1 Job
            Within a Job, Spark breaks the DAG into Stages:
            A stage ends whenever a wide transformation (shuffle) occurs.
            Stages are 0-indexed (Stage 0, Stage 1, …)
            Jobs are 0-indexed (Job 0, Job 1, …)

Example: proj.hobby.bigdata.rdd.tx.NarrowWideXformer
    Spark breaks this DAG into stages:
        Narrow-only chains -> stay in the same stage.
        A wide transformation (shuffle) -> cuts and starts a new stage.
        Each stage is executed as tasks on partitions.
    You write transformations; Spark builds + optimizes it, then shows it in the Spark UI (localhost:4040).
    Job 0: loading lines -> normalized words is Narrow chain (no shuffle yet)
        No job has run yet - these are all lazy transformations.
        Spark records them as part of the same Stage 0.
        --> When the first action runs: normalizedWords.take(10); Spark submits Job 0.
        Executes Stage 0 on just enough partitions to return 10 records.
        DAG (UI): textFile --> Filter --> FlatMap --> Map --> Filter --> mapToPair --> Take
    Job 1:
        Why Job 1? take already triggered Job 0
        First Tx in Job 1 is Wide Tx
        reduceByKey --> WIDE (introduces shuffle)
        Causes Spark to split the DAG:
        Stage 0: all narrow ops up to mapToPair(word --> 1)
        Shuffle boundary
        Stage 1: reduceByKey operates on shuffled data
        --> When you run Action: wordCounts.take(10); Spark triggers Job 1 (two stages: 0 & 1).
        DAG (UI): Stage 0 --> Shuffle --> Stage 1 (reduceByKey --> take)
    Job 2:
        groupByKey --> another WIDE transformation
        mapToPair(countToWord) --> groupByKey
        mapToPair = narrow (same stage)
        groupByKey = wide (new shuffle --> new stage)
        --> Action: wordsByFrequency.take(10);  Triggers Job 2:
        Stage 0: recompute upstream wordCounts (if not cached)
        Stage 1: mapToPair(countToWord) (narrow)
        Stage 2: groupByKey (wide --> shuffle)
        DAG (UI): Stage 0 --> Stage 1 --> Stage 2
    Job 3: (Not Triggered)
        coalesce and repartition
        repartition(n) --> wide, causes a shuffle (new stage)
        coalesce(n, false) --> narrow, stays in the same stage
        --> Action on coalesced would trigger a new Job 3: In the example there is no action hence this job is not triggerred
        DAG: narrow chain --> repartition (shuffle) --> coalesce --> action
    Actions: Program Flow
            normalizedWords.take(10);
            wordCounts.take(10);
            wordsByFrequency.take(10);
        Each of these take() calls is an Action, and therefore creates a Job.
        Also:
            repartition(4);
            coalesce(2, false);
        but no Action on them - so those do not create a new Job (unless we later add something like coalesced.count()).

Catalyst(Optimizer), Tungsten(Physical Execution), DAG(Scheduler):
    What really happens when following spark program is run proj.hobby.bigdata.dataset.tx.??:
        Logical Plan (Unresolved) (Spark parses your operations into a tree):
            Project, Filter, Aggregate, etc.
            At first columns, relations may be unresolved ('country, 'amount).
        Catalyst Analyzer:
            Binds columns and tables using catalog/metadata.
            Validates schemas, resolves functions, etc.
            Produces a resolved logical plan.
        Catalyst Optimizer (Rule-based + cost-based transforms):
            Push down filters/projections:
            Constant folding  --> Replace lit(10) + lit(5) with lit(15)
            Reorder joins
            Collapse adjacent operations
            Use stats (if available) for better join order/strategy
            All these rules live in:
                org.apache.spark.sql.catalyst.optimizer.Optimizer.scala
        When the Logical Plan Becomes a DAG
            Once Catalyst finishes optimizing the logical plan:
            Spark’s Query Planner translates it to one or more Physical Plans (with actual execution strategies).
            The chosen physical plan is converted into a DAG of stages (each stage = set of tasks over partitions).
            Only when you call an action (e.g., show(), count(), collect()) does Spark:
                -   Trigger the DAG Scheduler.
                -   Submit the job to executors.
                -   Execute according to the physical plan.
        Physical Planning
            Spark’s Query Planner translates it to one or more Physical Plans (with actual execution strategies)
            Generates one or more candidate physical plans.
            Chooses operators: sort-merge join vs hash join vs broadcast, shuffle vs no shuffle, etc.
            This becomes your DAG of stages/tasks.
        DAG Scheduler (Lazy):
            Only when you call an action (count, show, collect, write, etc):
                Breaks the physical plan into stages:
                    Stage boundary at every shuffle.
                    Within a stage: multiple tasks over partitions.
                Submits to cluster manager (YARN/K8s/Stand-alone), executes.
        Tungsten (Execution Engine)
            Underneath the physical operators:
                Off-heap / on-heap managed memory.
                Cache-friendly binary row format.
                Whole-stage codegen (fuses operators into tight Java bytecode loops).
            This is why your “map/filter/projection” pipelines become crazy-fast tight loops instead of generic object-y code.
    Summary:
        DataFrame API/SQL Query (Application Spark code is "Unresolved" Logical Plan)
        Catalyst Analyzer  ("resolved" logical plan)
        Catalyst Optimizer ( Rule & Cost based Optimized Plan) => Generates 1 or more Physical Plan
        DAG scheduler constructs stages using shuffle boundaries (submits to YARN/K8s/Stand-alone)
        Tungsten executes it efficiently in memory & CPU
    Example Plan:
        ```bash
            1. == Parsed Logical Plan ==
            'Aggregate ['category], ['sum('amount) AS total_sales#30]
            +- 'Filter ('region = "US")
               +- 'UnresolvedRelation [sales]
            2. == Analyzed Logical Plan ==
            category: string, total_sales: double
            Aggregate [category#12], [sum(amount#14) AS total_sales#30]
            +- Filter (region#13 = US)
               +- Relation [region#13, category#12, amount#14] csv
            3. == Optimized Logical Plan ==
            Aggregate [category#12], [sum(amount#14) AS total_sales#30]
            +- Project [category#12, amount#14]
               +- Filter (region#13 = US)
                  +- Relation [region#13, category#12, amount#14] csv
            4. == Physical Plan ==
            *(2) HashAggregate(keys=[category#12], functions=[sum(amount#14)], ...)
        ```

Spark Tuning:
    ```java
        agg.explain();                      // simple logical + physical
        agg.explain("extended");            // all plans
        agg.explain("formatted");           // nicer breakdown (Spark 3+)
        df.queryExecution.executedPlan      // Inspect physical plan
    ```
    Use .select() early to enable column pruning.
    Ensure you’re reading from a columnar source like Parquet/ORC, not CSV.
    Join explosion?
        Add statistics: ANALYZE TABLE table_name COMPUTE STATISTICS;
        Or give hints: /*+ BROADCAST(small_table) */
    Look for:
        Extra Exchange -> shuffle boundaries
                Spark shuffles are visiblie as Exchange operation in the query plan, if operation involves multiple stages of shuffling, multiple exchanges occurr.
                shuffle is aligning datasets bsed on join columns, so dataset partitions having same join columns reside on same worker nodes
        Extra Sort you don’t need
        Bad join strategy (e.g., SortMergeJoin on tiny dim table instead of broadcast)
        Missing filter pushdown (e.g., filter appears above scan instead of inside FileScan)
            Filters may not be pushed down past joins in Spark because the filter depends on a value that is only known after the join is performed,
            or because the filter logic is not compatible with the data source's filtering capabilities.
            Other reasons include schema issues, complex functions, or the filter being applied after a cache.
        In Java: df.queryExecution().logical().toString() (if you access via Scala API interop).
            ```java
                spark.conf().getAll().forEach((k, v) -> System.out.println(k + " = " + v));
                verify:
                    spark.sql.adaptive.enabled=true
                    spark.sql.shuffle.partitions
            ```
            ```SQL
                SET -v;  -- list config
                -- If using spark SQL
                EXPLAIN EXTENDED
                SELECT ...
                FROM ...
                ;
            ```
    Practical Join hints:
        ```java
            Dataset<Row> fact = ...;
            Dataset<Row> dim  = ...;
            Dataset<Row> joined = fact.join(
            functions.broadcast(dim),  // broadcast dim table
            fact.col("dim_id").equalTo(dim.col("id")),
            "left"
            );
        ```
        ```SQL
            SELECT /*+ BROADCAST(dim) */ ...
            FROM fact
            JOIN dim ON ...
        ```
    Common hints:
        BROADCAST(t) => force broadcast hash join.
        SHUFFLE_HASH_JOIN, SHUFFLE_MERGE, MERGE => steer physical join type (only when you know what you’re doing).
            -   BHJ is best when the smaller table is small (< 10 MB)
            -   SMJ is a general-purpose join that is robust for large datasets (High CPU)
            -   SHJ strikes a balance by avoiding the heavy sorting cost when the per-partition memory size is manageable. (Extra Memory on worker node for the Hash Join Table)
            https://www.canadiandataguy.com/p/spark-join-strategies-explained-shuffle
        COALESCE, REPARTITION in API to adjust partitioning before joins.
    Control shuffle & partitioning:
        If you see massive shuffle:
        // Default might be too high or too low for you:
        spark.conf().set("spark.sql.shuffle.partitions", "400"); // tune per cluster & data size
        Pre-distribute fact tables on join keys:
        ```java
            Dataset<Row> df1p = df1.repartition(col("join_key"));
            Dataset<Row> df2p = df2.repartition(col("join_key"));
            ...
            Dataset<Row> joined = df1p.join(df2p, "join_key");
        ```
        Use wisely:
            repartition(col) -> full shuffle, but good for co-locating joins.
            coalesce(n) -> reduce partitions without full shuffle (good post-filter or post-aggregation).
    Cache / persist when reuse is real
        If you use same heavy sub-plan in multiple actions:
        ```java
            Dataset<Row> base = ...expensive read & filters...;
            ...
            base = base.persist(StorageLevel.MEMORY_AND_DISK());
            base.count(); // materialize once
            ..
            // reuse `base` safely below
            Dataset<Row> v1 = base.filter(...);
            Dataset<Row> v2 = base.groupBy(...);
        ```
        Don’t: Cache everything blindly (Tungsten memory is still finite).
        Do: Cache only reused, expensive, and reasonably sized intermediates.
    Help Catalyst & Tungsten by staying in the SQL/DataFrame world
        Prefer built-ins, instead of random opaque UDFs (Avoid killing optimizations )
            - Consider Spark SQL functions first.
            - Prefer Scala/Java UDF over Python UDF in JVM shops.
            - Use Pandas UDF only when running PySpark and working with columnar batches.
        Keep schemas explicit when possible (below skips inference and helps Catalyst reason better):
            ```java
                StructType schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("amount", DataTypes.DoubleType)
                .add("country", DataTypes.StringType);
                ....
                Dataset<Row> df = spark.read()
                .schema(schema)
                .csv(path);
            ```
        If using metastore / Hive-style tables or Delta/Iceberg:
            ```sql
                ANALYZE TABLE my_db.big_fact COMPUTE STATISTICS;
                ANALYZE TABLE my_db.big_fact COMPUTE STATISTICS FOR COLUMNS join_key, dt;
                -- After this, the table level statistics are computed and saved in metastore and we can see them by calling
                DESCRIBE EXTENDED my_db.big_fact
            ```
        Lets Catalyst:
            - Reorder joins
            - Decide which table to broadcast
            - Estimate cardinalities to reduce silly plans

Resources:
    https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/key-topics-apache-spark.html