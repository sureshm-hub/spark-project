package proj.hobby.bigdata.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;


/**
 * Build/run:
 *   spark-submit --class SparkStreamingOneFile --master local[2] your-jar.jar dstream
 *   spark-submit --class SparkStreamingOneFile --master local[2] your-jar.jar structured
 *
 * Start a socket source:
 *   nc -lk 9999   (or ncat -lk 9999 on Windows)
 *
 * DStreams input lines (any text):
 *   hello spark hello
 *
 * Structured input lines (eventTime + text):
 *   2026-02-07T12:00:01 hello spark
 *   2026-02-07T12:00:03 hello
 */
public class SparkDStreams {

    // Pick a durable location in real clusters (HDFS/S3/DBFS). Local is fine for learning.
    private static final String CHECKPOINT_DIR_DSTREAM = "file:///tmp/spark-dstream-checkpoint";

    /**
     * Classic Spark Streaming (DStreams)
     *
     * Key ideas:
     * - Micro-batch engine: every batch interval => a new RDD
     * - DStream = sequence of RDDs over time
     * - updateStateByKey => stateful transformation, requires checkpointing
     * - window(...) => sliding window aggregation
     */
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("DStreamsExample")
                // For local learning only:
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        // Required for stateful ops + fault tolerance (stores metadata/state)
        jssc.checkpoint(CHECKPOINT_DIR_DSTREAM);

        // Source: socket text stream (good for learning; production is usually Kafka)
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // Tokenize into words -> (word, 1)
        JavaPairDStream<String, Integer> words = lines
                .flatMap(x -> Arrays.asList(x.trim().split("\\s+")).iterator())
                .filter(w -> !w.isEmpty())
                .mapToPair(w -> new Tuple2<>(w.toLowerCase(Locale.ROOT), 1));

        // 2.2 Stateful transformation: running counts using updateStateByKey
        // newValues = counts in current batch; state = previous running count
        JavaPairDStream<String, Integer> runningCounts = words.updateStateByKey(
                (List<Integer> newValues, Optional<Integer> state) -> {
                    int sum = state.orElse(0);
                    for (int v : newValues) sum += v;
                    return Optional.of(sum);
                }
        );

        // 2.3 Sliding window: counts per word over last 30s, sliding every 10s
        JavaPairDStream<String, Integer> windowCounts = words
                .reduceByKeyAndWindow(
                        Integer::sum,
                        Durations.seconds(30),
                        Durations.seconds(10)
                );

        // Sink: console (print a few)
        runningCounts.foreachRDD((JavaPairRDD<String, Integer> rdd, Time time) -> {
            List<Tuple2<String, Integer>> top = rdd
                    .mapToPair(t -> t) // no-op, just clarity
                    .take(10);

            System.out.println("\n=== DSTREAM Running Counts @ " + new Date(time.milliseconds()) + " ===");
            for (Tuple2<String, Integer> t : top) System.out.println(t._1 + " -> " + t._2);
        });

        windowCounts.foreachRDD((JavaPairRDD<String, Integer> rdd, Time time) -> {
            List<Tuple2<String, Integer>> top = rdd.take(10);
            System.out.println("\n=== DSTREAM Window(30s, slide 10s) @ " + new Date(time.milliseconds()) + " ===");
            for (Tuple2<String, Integer> t : top) System.out.println(t._1 + " -> " + t._2);
        });

        // 3.1 Fault tolerance: receiver-based sources can use WAL; in production prefer Kafka direct.
        // 3.2 Checkpointing: already enabled above; required for updateStateByKey and for recovery.
        jssc.start();
        jssc.awaitTermination();
    }


}