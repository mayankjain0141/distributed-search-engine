package Spark;
import java.lang.String;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;




public class PositivityRank {
    private JavaSparkContext jsc;
    void Run(){
        SparkConf conf = new SparkConf().setAppName(PositivityRank.class.getName()).setMaster("local[3]");
        // Create a new spark context
        jsc = new JavaSparkContext(conf);
        // Load Dataset
        JavaRDD<String> posWords = jsc.textFile("data/positive words.csv");
        JavaRDD<Tuple2<String,String>> posWordsTuple = posWords.map(
            entry -> {
                String[] parts = entry.split(",");
                return new Tuple2<>(parts[1], parts[0]);
            }
        );
        JavaPairRDD<String, String> posWordsRDD = JavaPairRDD.fromJavaRDD(posWordsTuple);
        JavaRDD<String> index = jsc.textFile("output_inverted_index/*");
        JavaRDD<Tuple2<String, String>> indexTuple = index.map(entry -> {
            String[] parts = entry.split(",");
            String word = parts[0].replace("(", "");
            String locs = parts[1].replace(")", "");
            return new Tuple2<String, String>(word, locs);
        });
        JavaPairRDD<String, String> indexRDD = JavaPairRDD.fromJavaRDD(indexTuple);
        JavaPairRDD<String, Tuple2<String, Optional<String>>> combined = indexRDD.leftOuterJoin(posWordsRDD);
        JavaPairRDD<String, String> cleanedJoin = combined
            .mapValues(
                val -> {
                        String[] fileInfoList = val._1().split(" ");
                        String modVal = "";
                        for (int i=0;i<fileInfoList.length;i++){
                            String f = fileInfoList[i];
                            String[] parts = f.split(":");  
                            String file = parts[0];
                            String[] lineInfoList = parts[1].split(";");
                            if(i>0){
                                modVal+=" ";
                            }
                            modVal+=file+":"+String.valueOf((val._2.isPresent())?lineInfoList.length:0);
                        }
                        return modVal;
                }
            );
        JavaPairRDD<Integer, String> cntToFile = cleanedJoin
            .flatMapValues(val -> Arrays.asList(val.split(" ")).iterator())
            .mapToPair( entry -> {
                String[] parts = entry._2.split(":");
                String file = parts[0];
                Integer posCnt = Integer.valueOf(parts[1]);
                return new Tuple2<String, Integer>(file, posCnt);
            })
            .reduceByKey( (x, y)-> x+y)
            .mapToPair(entry->{
                return new Tuple2<Integer, String>(entry._2(), entry._1());
            });
        JavaPairRDD<Integer, String> rank = cntToFile
            .sortByKey(false);
        rank.saveAsTextFile("output_positivity_rank");
    }
    
}
