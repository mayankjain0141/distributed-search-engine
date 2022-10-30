package Spark;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YoutubeTitleWordCount {
    private static final String COMMA_DELIMITER = ",";
    public void Run() {

        Logger.getLogger("org").setLevel(Level.ERROR);
		// CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
      
        // LOAD DATASETS
        JavaRDD<String> videos = sparkContext.textFile("data/youtube/USvideos.csv");

        // TRANSFORMATIONS
        JavaRDD<String> titles =videos
                .map(YoutubeTitleWordCount::extractTitle)
                .filter(StringUtils::isNotBlank);

        JavaRDD<String> words = titles.flatMap( title -> Arrays.asList(title
                .toLowerCase()
                .trim()
                .replaceAll("\\p{Punct}","")
                .split(" ")).iterator());
		
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue();
        List<Map.Entry> sorted = wordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
      
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

        sparkContext.close();
    }


    public static String extractTitle(String videoLine){
        try {
            return videoLine.split(COMMA_DELIMITER)[2];
        }catch (ArrayIndexOutOfBoundsException e){
            return "";
        }
    }
}

