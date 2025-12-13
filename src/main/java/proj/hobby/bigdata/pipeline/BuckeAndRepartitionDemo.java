package proj.hobby.bigdata.pipeline;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 *
 * https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html
 *
 */
public class BuckeAndRepartitionDemo {

    public static void main(String[] args) {

        // ------------------------------------------------------------------------------------
        // 1. SparkSession (local demo)
        // ------------------------------------------------------------------------------------
        SparkSession spark = SparkSession.builder()
                .appName("BucketingAndRepartitionDemo")
                .master("local[*]")
                // Uncomment this if you actually want to use saveAsTable + buckets
                // and have Hive support available.
                // .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // For demo: fewer shuffle partitions so the plan is easier to read
        spark.conf().set("spark.sql.shuffle.partitions", "8");

        // ------------------------------------------------------------------------------------
        // 2. Build sample DataFrames: orders & products
        // ------------------------------------------------------------------------------------

        // orders(order_id, product_id, quantity)
        StructType ordersSchema = new StructType()
                .add("order_id", DataTypes.IntegerType, false)
                .add("product_id", DataTypes.IntegerType, false)
                .add("quantity", DataTypes.IntegerType, false);

        List<Row> ordersRows = Arrays.asList(
                RowFactory.create(1, 101, 2),
                RowFactory.create(2, 102, 1),
                RowFactory.create(3, 101, 5),
                RowFactory.create(4, 103, 3),
                RowFactory.create(5, 104, 4)
        );

        Dataset<Row> orders = spark.createDataFrame(ordersRows, ordersSchema);

        // products(product_id, category_id, price)
        StructType productsSchema = new StructType()
                .add("product_id", DataTypes.IntegerType, false)
                .add("category_id", DataTypes.IntegerType, false)
                .add("price", DataTypes.DoubleType, false);

        List<Row> productsRows = Arrays.asList(
                RowFactory.create(101, 10, 19.99),
                RowFactory.create(102, 20, 5.49),
                RowFactory.create(103, 10, 7.99),
                RowFactory.create(104, 30, 49.99),
                RowFactory.create(105, 40, 99.99)
        );

        Dataset<Row> products = spark.createDataFrame(productsRows, productsSchema);

        System.out.println("=== Orders ===");
        orders.show(false);

        System.out.println("=== Products ===");
        products.show(false);

        // ------------------------------------------------------------------------------------
        // 3. Naive join (baseline) – Spark will shuffle both sides on product_id
        // ------------------------------------------------------------------------------------
        Dataset<Row> naiveJoin = orders.join(products, "product_id");

        System.out.println("\n=== Naive join (no explicit partitioning) ===");
        naiveJoin.show(false);

        System.out.println("\n=== Physical plan for naive join ===");
        naiveJoin.explain("extended");

        // ------------------------------------------------------------------------------------
        // 4. Repartition DataFrames on join key before join
        //    Pattern from AWS doc:
        //    df1_repartitioned = df1.repartition(N, "join_key")
        //    df2_repartitioned = df2.repartition(N, "join_key")
        //    df_joined        = df1_repartitioned.join(df2_repartitioned, "join_key")
        // ------------------------------------------------------------------------------------

        int numPartitions = 8; // choose based on data size and cluster

        Dataset<Row> ordersRepart = orders.repartition(numPartitions, col("product_id"));
        Dataset<Row> productsRepart = products.repartition(numPartitions, col("product_id"));

        Dataset<Row> repartJoin = ordersRepart.join(productsRepart, "product_id");

        System.out.println("\n=== Join after explicit repartition(product_id) on both sides ===");
        repartJoin.show(false);

        System.out.println("\n=== Physical plan for repartitioned join ===");
        repartJoin.explain("extended");

        // Note:
        // This doesn't completely eliminate shuffle in all cases, but it:
        //  - makes the partitioning of both DataFrames consistent on the join key
        //  - can reduce skew and network I/O during the join
        //  - gives you direct control over the partition count on that shuffle edge

        // ------------------------------------------------------------------------------------
        // 5. Bucketing both sides on the join key
        //
        // Bucketing pre-shuffles and pre-sorts the data on the join key and writes
        // it to “bucketed” tables. If both sides:
        //    - have the same number of buckets
        //    - are bucketed on the same join key
        // then Spark can often avoid a full shuffle on sort-merge joins.
        //
        // NOTE: This section requires Hive metastore / catalog access and
        //       sparkSession.enableHiveSupport() in a real environment.
        //       You can adapt this to your cluster / catalog.
        // ------------------------------------------------------------------------------------

        int numBuckets = 8;

        // For demo: write to bucketed tables (in real env, this is typically Parquet in a warehouse)
        // Make sure you have a warehouse dir configured, e.g. spark.sql.warehouse.dir
        // and Hive support if needed.
        /**
         *
         * 1. Can Spark write bucketed data without Hive? Yes.
         * This works even without Hive:
         * df.write()
         *   .bucketBy(8, "product_id")
         *   .sortBy("product_id")
         *   .saveAsTable("demo_orders_bucketed");
         * saveAsTable() requires a catalog/warehouse directory.
         * Spark will create directory structure and files, but without Hive support it cannot store bucket metadata.
         * So Spark sees the output as just Parquet files, NOT as a “bucketed table.”
         *
         * 2. Will Spark USE bucketing during joins without Hive support? No.
         * To skip shuffles on bucketed joins, Spark must know:
         *      Number of buckets
         *      Bucket column
         *      Sort order
         * These are stored in the Hive metastore table metadata. Without Hive support:
         *      Spark cannot read that metadata.
         *      It cannot guarantee consistent bucketing.
         *      It will NOT avoid shuffle.
         *      Physical plan will still show Exchange hashpartitioning(...).
         *      So bucketing becomes just fancy file layout, not a performance optimization.
         *
         * 3. What happens without Hive support?
         * If you run:  orders.write().bucketBy(8, "product_id").sortBy("product_id").saveAsTable("x")
         * Without enabling Hive:
         * Spark internally falls back to V2 catalog behavior
         * Table metadata does NOT store bucket info
         * On join, Spark still performs shuffle
         * → Exchange hashpartitioning(product_id, 8)
         * So the benefit is lost.
         */
        orders.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(numBuckets, "product_id")
                .sortBy("product_id")
                .saveAsTable("demo_orders_bucketed");

        products.write()
                .mode(SaveMode.Overwrite)
                .bucketBy(numBuckets, "product_id")
                .sortBy("product_id")
                .saveAsTable("demo_products_bucketed");

        // Read the bucketed tables back
        Dataset<Row> ordersBucketed = spark.table("demo_orders_bucketed");
        Dataset<Row> productsBucketed = spark.table("demo_products_bucketed");

        System.out.println("\n=== Bucketed tables: demo_orders_bucketed ===");
        ordersBucketed.show(false);

        System.out.println("=== Bucketed tables: demo_products_bucketed ===");
        productsBucketed.show(false);

        // Join bucketed tables on the same key
        Dataset<Row> bucketedJoin = ordersBucketed.join(productsBucketed, "product_id");

        System.out.println("\n=== Join between bucketed tables (same key, same #buckets) ===");
        bucketedJoin.show(false);

        System.out.println("\n=== Physical plan for bucketed join ===");
        bucketedJoin.explain("extended");

        // In a real cluster with larger data, this is where you'd expect:
        //  - reduced shuffle reading/writing
        //  - less spill to disk
        //  - more predictable join performance

        spark.stop();
    }
}
