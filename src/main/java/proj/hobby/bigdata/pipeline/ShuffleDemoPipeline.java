package proj.hobby.bigdata.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class ShuffleDemoPipeline {

    public static void main(String[] args) {
        // Adjust this path for your machine
        // On Windows, make sure the directory exists: C:\tmp\spark-shuffle-demo
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleDemo")
                .setMaster("local[4]")
                .set("spark.sql.shuffle.partitions", "4") // SHUFFLE PARTITIONS
                .set("spark.sql.files.maxPartitionBytes", String.valueOf(128L * 1024 * 1024)) // 128 MB
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                // "Ideal" target size of shuffle partitions
                .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", String.valueOf(64L * 1024 * 1024)) // 64MB
                .set("spark.local.dir", "C:/tmp/spark-shuffle-demo");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // --- 1. Build small "orders" DataFrame in memory ---
        List<Row> ordersData = Arrays.asList(
                RowFactory.create(1, 101, 50.0),
                RowFactory.create(2, 102, 200.0),
                RowFactory.create(3, 101, 300.0),
                RowFactory.create(4, 103, 20.0),
                RowFactory.create(5, 104, 500.0),
                RowFactory.create(6, 102, 150.0),
                RowFactory.create(7, 105, 80.0),
                RowFactory.create(8, 101, 60.0)
        );

        StructType ordersSchema = new StructType(new StructField[]{
                new StructField("order_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("customer_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> orders = spark.createDataFrame(ordersData, ordersSchema);

        // Repartition to force a shuffle and create multiple partitions
        Dataset<Row> ordersRepart = orders.repartition(4, functions.col("customer_id"));

        // How many partitions did the file read produce?
        System.out.println("Input partitions (from file source): " +ordersRepart.rdd().getNumPartitions());

        // --- 2. Build small "customers" DataFrame in memory ---
        List<Row> customersData = Arrays.asList(
                RowFactory.create(101, "US"),
                RowFactory.create(102, "CA"),
                RowFactory.create(103, "US"),
                RowFactory.create(104, "UK"),
                RowFactory.create(105, "IN")
        );

        StructType customersSchema = new StructType(new StructField[]{
                new StructField("customer_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("country", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> customers = spark.createDataFrame(customersData, customersSchema)
                // Also repartition so join has shuffle on both sides
                .repartition(4, functions.col("customer_id"));

        // --- 3. Pipeline with multiple shuffles ---

        // Narrow: filter + withColumn (no shuffle)
        Dataset<Row> filtered = ordersRepart
                .filter(functions.col("amount").gt(50.0))
                .withColumn("amount_with_tax", functions.col("amount").multiply(1.1));

        // Wide: join on customer_id (shuffle)
        Dataset<Row> joined = filtered
                .join(customers, "customer_id");  // triggers shuffle

        // Wide: groupBy (shuffle)
        Dataset<Row> aggregated = joined
                .groupBy("country")
                .agg(functions.sum("amount_with_tax").alias("total_amount"));

        // Wide: orderBy (shuffle)
        Dataset<Row> result = aggregated
                .orderBy(functions.col("total_amount").desc());

        // --- 4. Action to actually execute the DAG ---
        result.show(false);

        // Sleep a bit so you can inspect Spark UI before app exits
        try {
            System.out.println("Sleeping for 60 seconds. Open http://localhost:4040 in your browser.");
            Thread.sleep(300_000);
        } catch (InterruptedException e) {
            // ignore
        }

        spark.stop();
    }
}
