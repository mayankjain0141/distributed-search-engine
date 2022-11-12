package Spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;


public class SearchWord {
    private JavaSparkContext jsc;
    public void Run(String input){
        SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local[3]");
        // Create a new spark context
        jsc = new JavaSparkContext(conf);
        // Load Dataset
        JavaRDD<String> index = jsc.textFile("output/inverted_index/part-*");

        JavaRDD<String> searchResult = index.filter(text -> text.equals(input));

        searchResult.saveAsTextFile("output/search_word");

    }

}
