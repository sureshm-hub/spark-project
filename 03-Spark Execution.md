# Spark DAG & Execution:
    Important components:
        | ------------------- | ------------------------------------------- | -------------------------------------------------------------------------------- |
        | Level               | Component                                   | Role                                                                             |
        | ------------------- | ------------------------------------------- | -------------------------------------------------------------------------------- |
        | **Cluster Manager** | YARN / Kubernetes / Standalone / Mesos      | Allocates resources to Spark applications.                                       |
        | **Driver**          | SparkContext, DAG Scheduler, Task Scheduler | Runs on master node, builds logical & physical plans, and coordinates execution. |
        | **Executor**        | JVM processes on worker nodes               | Executes tasks, holds cache/shuffle data, reports status back to driver.         |
        | **Node**            | Machine/VM/Pod                              | Runs one or more executors depending on configuration.                           |
        | ------------------- | ------------------------------------------- | -------------------------------------------------------------------------------- |
    Driver Responsible for:
        Converting transformations into a DAG (Directed Acyclic Graph).
        Scheduling tasks on executors.
        Tracking progress and collecting results.
        Runs once per job, usually on the master node (or a single pod in Kubernetes).
        Spark offers different deployment modes:
        -   Client Mode: "driver program" runs on the machine where the spark-submit command is executed (the "client" machine)
        -   Cluster Mode:"driver program" runs on one of the worker nodes within the Spark cluster, managed by the cluster manager.
        ```bash tuning config
            --driver-memory 4g
            --driver-cores 2
        ```
        If the driver runs out of memory (e.g., large collect() or huge broadcast vars), Spark crashes.
    * Application Master(AM):
        YARN-specific process responsible for managing one Spark application’s lifecycle within the YARN cluster.
            - In cluster mode, the AM contains the driver - when the driver dies, the job dies.
            - In client mode, the AM only tracks executors, and if your laptop dies, the job stops (driver lost).
            | ---------------- | --------------------------------------------- | --------------------------------- | ---------------------------------------- |
            | Mode             | Where Driver Runs                             | AM Role                           | When to Use                              |
            | ---------------- | --------------------------------------------- | --------------------------------- | ---------------------------------------- |
            | **Cluster mode** | Inside YARN container (managed by AM)         | AM = Driver + Resource Negotiator | Production jobs (EMR, Airflow, etc.)     |
            | **Client mode**  | On local machine (where you ran spark-submit) | AM = Resource Negotiator only     | Interactive use (Spark Shell, notebooks) |
            | ---------------- | --------------------------------------------- | --------------------------------- | ---------------------------------------- |
        ApplicationMaster on Kubernetes or Standalone:
            Kubernetes:
                There is no ApplicationMaster - K8s’ scheduler replaces YARN’s ResourceManager.
                The Driver Pod requests executor pods directly from the Kubernetes API server.
            Standalone:
                The Spark Master and Worker daemons handle scheduling, again no AM.
                So, the AM is YARN-only - not part of Spark’s core design.
        Monitoring and Logs:
            You can view the ApplicationMaster in: YARN ResourceManager UI (:8088)
            It shows one AM per Spark application. Clicking into it lets you:
                - Track logs (stdout/stderr)
                - View driver logs (in cluster mode)
                - Access Spark UI link (usually port 4040 or a proxy link like :18088)
    Executor:
        Each Spark executor runs as a separate Java Virtual Machine (JVM) process
        The executor's JVM is launched on a worker node to execute tasks assigned by the Spark driver
        Why this architecture? Isolation, Resource Management, GC
        Runs multiple tasks in parallel (using cores).
        Stores RDD/DataFrame partitions in memory or disk (for caching/shuffles).
        Executors persist across the entire job (unlike map-reduce tasks which die per stage).
        ```bash tunning config
            --num-executors 10
            --executor-cores 4
            --executor-memory 8g
        ```
    Tasks:
        Smallest unit of work - 1 task per partition.
        Executed by executors, typically Spark aims to run ~1 task per CPU core at a time.
    Partitions:
        Logical divisions of your data - Spark parallelizes by partitions.
        Each partition = 1 task at runtime.
        Too few partitions -> underutilized cluster.
        Too many partitions -> scheduler overhead.
        ```java
            Rules of thumb:
                df.repartition(1000)  // increase parallelism
                df.coalesce(10)       // decrease parallelism (narrow)
        ```
        For large jobs:
            Aim for 2–4x total cores in the cluster for partitions.
            Each partition ~100–200 MB uncompressed.
    Executor Deep Dive:
    Executor Cores:
        Each executor runs multiple tasks in parallel.
        executor-cores = number of concurrent tasks on that executor.
        Common tuning:
            Too high → GC pressure & context switching.
            Too low → underutilized CPUs.
        ```bash
            On 8-core node:
            --executor-cores 5
            # leave 1–2 cores for OS/YARN overhead
        ```
    Executor Memory:
        Controls heap for tasks, caching, and shuffle.
        Memory Type	Description
            - executor-memory:    Total heap size per executor.
            - spark.memory.fraction:  Portion of memory for caching/shuffling (default 0.6).
            - spark.memory.storageFraction:   Fraction of above reserved for cached data (default 0.5).
        ```bash
            Example:
            --executor-memory 8g
            --conf spark.memory.fraction=0.7
        ```
    Number of Executors:
        Defines parallelism capacity.
        Each executor consumes memory and cores → total resources should match cluster capacity.
        Example for an EMR cluster with 3 worker nodes:
            Each node = 16 cores, 64 GB RAM
            Reserve 1 core + 8GB for OS/YARN
        ```bash
                Setting	Value
                executor-cores	5 #  are per node
                executor-memory	10g #  per node
                num-executors 6 # total number of executors for application, across all worker nodes in cluster
                Executors per node	2 #  not specified as a setting
                Total executors	3 nodes × 2 = 6 #  not specified as a setting
                Total cores = 6 × 5 = 30 parallel tasks. #  not specified as a setting
                Cluster Capacity:
                    num_executors = (nodes * (cores_per_node - reserved_cores)) / executor_cores
                    executor_memory = (memory_per_node - reserved_memory) / executors_per_node
        ```
    Full Spark Submit:
        ```java
            spark-submit \
            --class com.example.MyJob \
            --master yarn \
            --deploy-mode cluster \
            --num-executors 6 \
            --executor-cores 5 \
            --executor-memory 10g \
            --driver-memory 4g \
            --conf spark.sql.shuffle.partitions=200 \
            --conf spark.dynamicAllocation.enabled=false \
            myapp.jar
        ```
    Diagnostic Commands:
        | ------------------------------- | ------------------------------------ |
        | Purpose                         | Command                              |
        | ------------------------------- | ------------------------------------ |
        | View Spark UI                   | `http://<driver>:4040`               |
        | Show stage/partition info       | Spark UI → Stages tab                |
        | View job plan                   | `df.explain(true)`                   |
        | Adjust partitioning dynamically | `df.repartition(n)`                  |
        | Show config at runtime          | `spark.conf.getAll.foreach(println)` |
        | Inspect physical plan           | `df.queryExecution.executedPlan`     |
        | ------------------------------- | ------------------------------------ |
    Practical Tuning Heuristics:
        | --------------------- | ----------------------------------------------- |
        | Scenario              | What to Adjust                                  |
        | --------------------- | ----------------------------------------------- |
        | GC pressure           | Reduce executor-cores, increase executor-memory |
        | Cluster underutilized | Increase num-executors or partitions            |
        | Too many small tasks  | Decrease shuffle partitions                     |
        | Long-running stages   | Check skew (`df.groupByKey` hotspots)           |
        | Network bottlenecks   | Enable compression, repartition by key          |
        | Data imbalance        | Use `salting` or `repartitionByRange`           |
        | --------------------- | ----------------------------------------------- |