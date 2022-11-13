package Spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class SearchWord {
    private JavaSparkContext jsc;
    public void Run(String input){
        SparkConf conf = new SparkConf().setAppName(SearchWord.class.getName()).setMaster("local[3]");
        // Create a new spark context
        jsc = new JavaSparkContext(conf);
        // Load Dataset
        JavaRDD<String> index = jsc.textFile("output_inverted_index/*");
        JavaRDD<Tuple2<String, String>> indexTuple = index.map(entry -> {
            String[] parts = entry.split(",");
            String word = parts[0].replace("(", "");
            String locs = parts[1].replace(")", "");
            return new Tuple2<String, String>(word, locs);
        });

        JavaPairRDD<String, String> indexRDD = JavaPairRDD.fromJavaRDD(indexTuple);
        JavaPairRDD<String, String> searchResult = indexRDD.
            filter(idx -> idx._1().equals(input));
        searchResult.saveAsTextFile("output_search_word");
        jsc.close();
    }

}
