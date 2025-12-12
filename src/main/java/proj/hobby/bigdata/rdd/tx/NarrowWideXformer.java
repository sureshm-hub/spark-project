package proj.hobby.bigdata.rdd.tx;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class NarrowWideXformer {

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Narrow Wide Transformations")
                .master("local[*]").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

        String input = "src/main/resources/sample-data/words.txt";

        /**
         *
         * Narrow Tx's: map, flatMap, filter, mapPartitions, union, coalesce, withColumn etc;
         *
         * narrow = parent partition -> child partition (no shuffle)
         */

        JavaRDD<String> lines =  jsc.textFile(input);

        // Filter: Narrow
        JavaRDD<String> nonEmpty = lines.filter(l -> !l.trim().isEmpty());

        // flatMap: Narrow
        JavaRDD<String> words = nonEmpty.flatMap( l ->
                Arrays.asList(l.split("\\s+")).iterator());

        // map: Narrow
        JavaRDD<String> normalized = words.map(w -> w.toLowerCase().replaceAll("[^a-z0-9]", ""))
                                    .filter(w -> !w.isEmpty());

        System.out.println("=== Sample normalized words: Narrow Chain ===");
        normalized.take(10).forEach(System.out::println);

        // mapToPair: Narrow
        JavaPairRDD<String, Integer> wordToOne =  normalized.mapToPair(w -> new Tuple2<>(w, 1));
        wordToOne.take(10).forEach(System.out::println);


        /**
         *
         * Wide Tx's: reduceByKey, groupByKey, distinct, sortByKey, join, intersection, Repartition etc.,
         *
         * wide = requires SHUFFLE across partitions
         */

        // reduceByKey: wide
        JavaPairRDD<String, Integer> wordCounts = wordToOne.reduceByKey(Integer::sum);

        System.out.println("=== Word counts (WIDE: reduce by key) ===");
        wordCounts.take(10).forEach((t -> System.out.println( t._1+" ==> "+t._2)));

        // mapToPair: Narrow
        JavaPairRDD<Integer, String> countToWord = wordCounts.mapToPair(t ->  new Tuple2<>(t._2, t._1));

        // groupByKey: wide
        JavaPairRDD<Integer, Iterable<String>> wordsByFreq = countToWord.groupByKey();
        System.out.println("=== Words grouped by Frequency (WIDE: groupBykey) ===");
        wordsByFreq.take(10).forEach( t -> {
            System.out.println("Count = "+t._1+" --> "+t._2);
        });

        /**
         *
         * repartition = Wide
         * coalesce (no shuffle) = Narrow
         */
        JavaRDD<String> repartitioned = normalized.repartition(4);
        JavaRDD<String> coalesced = repartitioned.coalesce(2, false);

        System.out.println("\nPartitions after repartition(4): " + repartitioned.getNumPartitions());
        System.out.println("Partitions after coalesce(2): " + coalesced.getNumPartitions());

        jsc.close();
        ss.stop();
    }
}
