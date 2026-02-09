package proj.hobby.bigdata.streaming;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class SparkStructuredStreaming {

    private static final String CHECKPOINT_DIR_STRUCT  = "file:///tmp/spark-structured-checkpoint";

    /**
     * Structured Streaming
     *
     * Key ideas:
     * - “Streaming DataFrame” with the same API style as batch DataFrames
     * - Event-time processing + watermarking for late data
     * - Checkpointing is used for exactly-once-ish progress tracking (depends on sink/source)
     */
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("StructuredStreamingExample")
                .master("local[2]") // learning only
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Source: socket lines (value column is a string)
        Dataset<Row> raw = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        // Expect: "2026-02-07T12:00:01 some message"
        // Parse first token as event time, rest as text
        Column[] parts = new Column[]{ split(col("value"), "\\s+", 2).getItem(0),
                split(col("value"), "\\s+", 2).getItem(1) };

        Dataset<Row> parsed = raw
                .withColumn("event_time_str", parts[0])
                .withColumn("text", parts[1])
                .withColumn("event_time",
                        to_timestamp(col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss"))
                .filter(col("event_time").isNotNull())
                .withColumn("word", explode(split(lower(col("text")), "\\s+")))
                .filter(length(col("word")).gt(0));

        // 2.1 Late-arriving data: watermark says “accept late data up to X; beyond that drop old state”
        // 2.3 Sliding window: window(event_time, windowDuration, slideDuration)
        Dataset<Row> windowedCounts = parsed
                .withWatermark("event_time", "20 seconds")
                .groupBy(
                        window(col("event_time"), "30 seconds", "10 seconds"),
                        col("word")
                )
                .count()
                .orderBy(col("window").desc(), col("count").desc());

        // Sink: console (append/update mode depends on aggregation; use "update" here)
        StreamingQuery q = windowedCounts.writeStream()
                .format("console")
                .outputMode("update")
                // 3.2 checkpointing: stores offsets + progress + state store metadata
                .option("checkpointLocation", CHECKPOINT_DIR_STRUCT)
                .trigger(Trigger.ProcessingTime("5 seconds")) // micro-batch trigger
                .start();

        q.awaitTermination();
    }
}
