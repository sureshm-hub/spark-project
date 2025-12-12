package proj.hobby.bigdata.pipeline;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class MiniPipeline {

    // Simple POJO for typed Dataset example
    public static class Order implements Serializable {
        private String orderId;
        private String customerId;
        private double amount;

        // *** Required: no-arg constructor + getters/setters for Spark encoder ***
        public Order() {}

        public Order(String orderId, String customerId, double amount) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
        }

        public String getOrderId() {
            return orderId;
        }
        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getCustomerId() {
            return customerId;
        }
        public void setCustomerId(String customerId) {
            this.customerId = customerId;
        }

        public double getAmount() {
            return amount;
        }
        public void setAmount(double amount) {
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId='" + orderId + '\'' +
                    ", customerId='" + customerId + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }

    public static void main(String[] args) {

        // ---------- 1. SparkSession (modern entrypoint) ----------
        SparkSession spark = SparkSession.builder()
                .appName("MiniPipeline")
                .master("local[*]")
                .getOrCreate();

        // You still *can* access SparkContext if you want RDDs:
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        String path = "src/main/resources/data/orders.csv"; // adjust if needed

        // =========================================================
        // A) RDD PIPELINE (using JavaSparkContext)
        // =========================================================

        // 1. Read raw lines
        JavaRDD<String> lines = sc.textFile(path);

        // 2. Extract header so we can skip it
        String header = lines.first();

        JavaRDD<String> dataLines = lines
                .filter(line -> !line.equals(header));  // drop header

        // 3. Parse lines into Order POJO
        JavaRDD<Order> ordersRdd = dataLines.map(line -> {
            String[] parts = line.split(",");
            String orderId = parts[0].trim();
            String customerId = parts[1].trim();
            double amount = Double.parseDouble(parts[2].trim());
            return new Order(orderId, customerId, amount);
        });

        // 4. Map to (customerId, amount) pairs
        JavaPairRDD<String, Double> byCustomer = ordersRdd.mapToPair(o ->
                new Tuple2<>(o.getCustomerId(), o.getAmount())
        );

        // 5. Aggregate: sum amounts per customer
        JavaPairRDD<String, Double> totalsByCustomerRdd = byCustomer.reduceByKey(Double::sum);

        System.out.println("=== RDD result: total amount per customer ===");
        for (Tuple2<String, Double> t : totalsByCustomerRdd.collect()) {
            System.out.println("customerId=" + t._1 + ", totalAmount=" + t._2);
        }

        // =========================================================
        // B) DATAFRAME / DATASET PIPELINE (using SparkSession)
        // =========================================================

        // Option 1: Let Spark infer the schema from CSV directly
        Dataset<Row> ordersDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(path);

        System.out.println("=== Raw DataFrame schema (inferred) ===");
        ordersDf.printSchema();

        // 1. Simple aggregation using untyped DataFrame API
        Dataset<Row> totalsByCustomerDf = ordersDf
                .groupBy("customerId")
                .agg(sum("amount").alias("totalAmount"))
                .orderBy("customerId");

        System.out.println("=== DataFrame result: total amount per customer ===");
        totalsByCustomerDf.show(false);

        // 2. Typed Dataset<Order> approach (showing encoder usage)
        Dataset<Order> ordersDs = ordersDf.as(Encoders.bean(Order.class));

        Dataset<Row> totalsByCustomerFromDs = ordersDs
                .groupBy(col("customerId"))
                .agg(sum("amount").alias("totalAmount"))
                .orderBy("customerId");

        System.out.println("=== Dataset<Order> result: total amount per customer ===");
        totalsByCustomerFromDs.show(false);

        // =========================================================
        // C) Demonstrate RDD -> DataFrame conversion
        // =========================================================

        // Define an explicit schema matching Order fields (if you want tight control)
        StructType orderSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("orderId", DataTypes.StringType, true),
                DataTypes.createStructField("customerId", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true)
        });

        // Spark can create a DataFrame from an RDD of Java Beans if the fields match
        Dataset<Row> dfFromRdd = spark.createDataFrame(ordersRdd, Order.class)
                .select("orderId", "customerId", "amount");

        System.out.println("=== DataFrame created from RDD<Order> ===");
        dfFromRdd.show(false);
        dfFromRdd.printSchema();

        // You could run the same aggregation again:
        Dataset<Row> totalsFromRddDf = dfFromRdd
                .groupBy("customerId")
                .agg(sum("amount").alias("totalAmount"))
                .orderBy("customerId");

        System.out.println("=== Aggregation on DataFrame created from RDD ===");
        totalsFromRddDf.show(false);

        // ---------- Clean up ----------
        spark.stop();
    }
}
