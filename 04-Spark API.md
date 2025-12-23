# Main Spark APIs:
    SparkContext = low-level handle to the cluster for RDDs
    SQLContext = pre-2.0 entry point for DataFrames + SQL
    StreamingContext = pre-Structured-Streaming entry point for DStreams
    SparkSession = latest unified entry point (SQL, DataFrames, Datasets, Structured Streaming) that wraps a SparkContext

# SparkContext:
    Lives on the driver; represents your app’s connection to the cluster.
    Cluster manager (local, YARN, K8s, Mesos, standalone)
    Executors and resources
    JARs and files you ship to executors
    In old Spark code, it was the primary entry point:
        ```java
            SparkConf conf = new SparkConf()
            .setAppName("JavaSparkStarter")
            .setMaster("local[*]");
            ...
            JavaSparkContext sc = new JavaSparkContext(conf);
        ```
    Used for:
        Create RDDs:
                ```java
                    sc.parallelize(list)
                    sc.textFile("hdfs:///...")
                ```
        Manage broadcast variables and accumulators.
        Submit actions that turn into jobs → stages → tasks → executors.
    Lifecycle:
        One SparkContext per JVM (normally).
        ```java
            sc.stop(); //tears down executors and releases resources.
        ```
    In modern Spark, you usually don’t create it directly; you get it from SparkSession:
    ```java
        SparkSession spark = SparkSession.builder().appName("App").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    ```

# SparkSession (modern unified entry point):
    Introduced in Spark 2.0 as the one main handle for everything:
        DataFrames / Datasets
        SQL
        Catalog
        UDF registration
        Structured Streaming
    It wraps a SparkContext:
        spark.sparkContext() → underlying SparkContext
    It also subsumes:
        SQLContext
        HiveContext
    Basic creation (your starter project style)
        ```java
            SparkSession spark = SparkSession.builder()
            .appName("JavaSparkStarter")
            .master("local[*]")
            .getOrCreate();
        ```
    Common things from SparkSession
        ```java
            // Batch reads/writes
            Dataset<Row> df = spark.read()
            .format("csv")
            .option("header", "true")
            .load("input.csv");
            ...
            df.write()
            .mode("overwrite")
            .parquet("output_path");
            ...
            // SQL
            df.createOrReplaceTempView("orders");
            Dataset<Row> agg = spark.sql(
            "SELECT customerId, sum(amount) as total FROM orders GROUP BY customerId"
            );
            ...
            // Structured Streaming
            Dataset<Row> streamingDf = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:9092")
            .option("subscribe", "topic")
            .load();
            ...
            StreamingQuery query = streamingDf
            .writeStream()
            .outputMode("append")
            .format("console")
            .start();
            ...
            //Access to lower-level bits
            // start with SparkSession, then grab JavaSparkContext only when you really need RDD-level stuff.
            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            SQLContext sqlContext = spark.sqlContext();
        ```
    | --------------------- | --------------------------------- | -------------------------------------------------------------------------------- |
    | Era / Version         | Entry-point type                  | Main abstractions                                                                |
    | --------------------- | --------------------------------- | -------------------------------------------------------------------------------- |
    | Spark 1.x             | SparkContext                      | RDDs                                                                             |
    | Spark 1.x + SQL       | SparkContext + SQLContext         | RDDs + DataFrames + SQL                                                          |
    | Spark 1.x + Streaming | SparkContext + StreamingContext   | RDDs + DStreams                                                                  |
    | Spark 2.x+ (modern)   | SparkSession (wraps SparkContext) | DataFrames, Datasets, SQL, Structured Streaming; RDDs via `spark.sparkContext()` |
    | --------------------- | --------------------------------- | -------------------------------------------------------------------------------- |

# Spark-provided API, grouped logically:
    Core (RDD / low-level):
        Key types:
            SparkConf
            JavaSparkContext / SparkContext
            JavaRDD<T>
            JavaPairRDD<K, V>
            Shared vars:
                Broadcast<T>
                AccumulatorV2<IN, OUT>
        Functional interfaces for transformations:
            Function<T, R> – map
            FlatMapFunction<T, R> – flatMap
            PairFunction<T, K, V> – map to key/value
            PairFlatMapFunction<T, Tuple2<K,V>>
            Function2<T1, T2, R> – reduce, aggregate
            VoidFunction<T> – foreach
            These are all in org.apache.spark.api.java.function.
    SQL / DataFrames / Datasets:
        Key types:
            SparkSession
            Dataset<T>
            Dataset<Row> (a DataFrame in Java)
            Row
            Column
            Encoder<T> & Encoders (for typed Datasets)
        Reader/Writer:
            DataFrameReader – spark.read()
            DataStreamReader – spark.readStream()
            DataFrameWriter<T> – dataset.write()
            DataStreamWriter<T> – dataset.writeStream()
        Grouped operations:
            RelationalGroupedDataset – returned by df.groupBy("col")
            KeyValueGroupedDataset<K, V> – for typed groupByKey
        Functions:
            org.apache.spark.sql.functions static methods:
            col, lit, when, sum, avg, count, max, min, date_add, to_date, etc.
        Java lambda interfaces (typed Dataset):
            MapFunction<T, R>
            FlatMapFunction<T, R>
            FilterFunction<T>
            ForeachFunction<T>
        UDFs:
            UDF1, UDF2, ..., UDF22 in org.apache.spark.sql.api.java.
            Register via spark.udf().register("name", udfInstance, DataType).
    Streaming APIs:
        Legacy Spark Streaming (DStreams):
        JavaStreamingContext
        JavaDStream<T>
        JavaPairDStream<K, V>
        JavaReceiverInputDStream<T>
        -   Function interfaces are basically the same as JavaRDD transformations.
        -   You interact with discretized micro-batches of RDDs.
    Structured Streaming (modern):
        No separate “context”; it’s all via SparkSession:
        Dataset<Row> where df.isStreaming() is true
        DataStreamReader – spark.readStream()
        DataStreamWriter<T> – df.writeStream()
        StreamingQuery, StreamingQueryException
        -   You write “queries” on top of streaming DataFrames/Datasets.
    MLlib and GraphX (main abstractions):
        MLlib (new DataFrame-based API)
            Estimator<M> – something that can .fit() to produce a Model.
            Transformer – something that can .transform(dataset).
            Pipeline, PipelineModel
            Example: LogisticRegression, RandomForestClassifier, etc.
        GraphX / GraphFrames
            Graph<V, E> in Scala for GraphX.
            Java typically uses GraphFrames (separate package) with:
                GraphFrame built on Dataset<Row> vertices/edges.

# Dataset<Row> vs Dataset<Orders>
    For most Spark SQL-style workloads, Dataset<Row> will be faster than Dataset<Orders> (or any typed Dataset) 
    because the engine can stay in its super-optimized binary format and avoid creating Java/Scala objects.
    ```java
        // Both of these are Dataset:
        Dataset<Row> df;                  // untyped, a.k.a DataFrame
        Dataset<SalesRecord> ds;          // typed, using Encoders.bean(SalesRecord.class)
    ```
    Under the hood Spark always uses an internal binary row format (InternalRow) in the Tungsten engine:
        Columns stored in compact, off-heap, columnar/binary form.
        Expression evaluation done via Catalyst + whole-stage codegen.
    The difference is:
        Dataset<Row>
            Uses a Row encoder (RowEncoder) but there is no need to convert to a user-defined Java object in your code.
            As long as you stick to Spark SQL / DataFrame APIs (select, withColumn, join, groupBy, agg, etc.), Spark can stay in the optimized binary representation end-to-end.
            Keeps everything in binary form.
            Leverages whole-stage codegen and vectorized execution.
            Avoids creating one Java object per row.
        Dataset<SalesRecord>
            Uses a bean encoder (Encoders.bean(SalesRecord.class) or case-class encoder in Scala).
            Creates lots of short-lived objects → GC pressure.
            Loses many of the SQL-level optimizations.
            Almost always slower, sometimes much slower on large datasets.
            Spark must:
                Decode InternalRow -> SalesRecord when you call map, flatMap, foreach, etc. that operate on SalesRecord.
                Encode back SalesRecord -> InternalRow when returning a new Dataset.
                This is extra object allocation + (de)serialization work on the JVM.