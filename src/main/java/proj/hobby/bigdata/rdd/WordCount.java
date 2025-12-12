package proj.hobby.bigdata.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Word Count")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> lines = sc.textFile("src/main/resources/sample-data/words.txt");
        lines.flatMap(line -> Arrays.asList(line.split("\s+")).iterator())
                .mapToPair(word -> new scala.Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .foreach(t -> System.out.println(t._1 + " => " + t._2));

        sc.close();
        spark.stop();
    }
}