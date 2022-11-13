package Spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;




public class SearchPhrase {
    private JavaSparkContext jsc;
    public void Run(String op, String phrase) {
        SparkConf conf = new SparkConf().setAppName(SearchPhrase.class.getName()).setMaster("local[3]");
        // Create a new spark context
        jsc = new JavaSparkContext(conf);
        // Load Dataset
        JavaRDD<String> index = jsc.textFile("output_inverted_index/*");
        JavaPairRDD<String, String> files = jsc.wholeTextFiles("data/textfiles/*");
        JavaPairRDD<String, Tuple2<String, Integer>> fileToLine = files.flatMapValues(text -> InvertedIndex.AddLineNum(text))
                .filter(line -> !line._2()._1().isEmpty());

        JavaPairRDD<String, String> fileLineToText = fileToLine
            .mapToPair(
                entry -> {
                    String[] path = entry._1().split("/", 0);
                    String file = path[path.length-1];
                    String text = entry._2()._1();
                    String line = String.valueOf(entry._2()._2());
                    return new Tuple2<>(line+"@"+file, text);
                }
            );      
        JavaRDD<Tuple2<String, String>> indexTuple = index.map(entry -> {
            String[] parts = entry.split(",");
            String word = parts[0].replace("(", "");
            String locs = parts[1].replace(")", "");
            return new Tuple2<String, String>(word, locs);
        }); 
        String[] words = phrase.split(" ");
        String filterRegex = "("+String.join("|", words)+")";
        // System.out.println(">>>"+filterRegex);
        JavaPairRDD<String, String> indexRDD = JavaPairRDD.fromJavaRDD(indexTuple);
        JavaPairRDD<String, String> filterdIndex = indexRDD.filter(idx -> idx._1().matches(filterRegex));
        
        // System.out.println(">>>"+filterdIndex.values().collect());
        JavaPairRDD<String, String> searchResult = null;
        JavaPairRDD<String, String> tempResult1 = null;
        
        if(op == "OR"){
            JavaRDD<String> loc = filterdIndex.values();
        
        JavaRDD<List<Tuple2<String, String>>> locTuple2 = loc.map(entry->{
            String[] fileList = entry.split(" ");
            List<Tuple2<String, String>> list = new ArrayList<>();
            for(String f: fileList){
                String[] parts = f.split(":");
                String[] lineInfoList = parts[1].split(";");
                for(String l: lineInfoList){
                    String[] lineParts = l.split("#");
                    list.add(new Tuple2<String,String>(lineParts[0]+"@"+parts[0], lineParts[1]));
                } 
            }
            return list;
        });
        List<Tuple2<String, String>> fileLineInfoList = locTuple2.fold(new ArrayList<Tuple2<String, String>>(), 
            (a, b) -> {
               a.addAll(b);
               return a;
            }
        );

        JavaPairRDD<String, String> fileLineToPos = jsc.parallelizePairs(fileLineInfoList);
        
        tempResult1 = fileLineToPos
            .reduceByKey(
                (a, b) -> {
                    return a+" "+b;
                }
            );

        } else{
            // List<List<Tuple2<String,String>>> masterList = new ArrayList<>();
            JavaRDD<List<Tuple2<String, String>>> x = filterdIndex.map(entry->{
                String[] fileList = entry._2().split(" ");
                List<Tuple2<String, String>> list = new ArrayList<>();
                for(String f: fileList){
                    String[] parts = f.split(":");
                    String[] lineInfoList = parts[1].split(";");
                    for(String l: lineInfoList){
                        String[] lineParts = l.split("#");
                        list.add(new Tuple2<String,String>(lineParts[0]+"@"+parts[0], lineParts[1]));
                    } 
                }
                return list;  
            });
            List<List<Tuple2<String,String>>> masterList = x.collect();
            // if(masterList.size()==0){
            //     System.out.println("a khali...");
            //     return;
            // }
            List<JavaPairRDD<String, String>> parallelizedList = new ArrayList<>();
            for(List<Tuple2<String, String>> list : masterList){
                JavaPairRDD<String, String> b = jsc.parallelizePairs(list);
                parallelizedList.add(b);
            }
            // if(parallelizedList.size()==0){
            //     System.out.println("|| list khali...");
            //     return;
            // }
            tempResult1 = parallelizedList.get(0);
            for(int i=1;i<parallelizedList.size();i++){
                JavaPairRDD<String, Tuple2<String, String>> tempResult = tempResult1.join(parallelizedList.get(i));
                tempResult1 = tempResult.mapValues(
                    field -> field._1()+" "+field._2()
                );
            }

        }

        JavaPairRDD<String, Tuple2<String,String>> tempResult2 = tempResult1.join(fileLineToText);
        
        // tempResult2.saveAsTextFile("tempresult");
        searchResult = tempResult2
            .mapValues(
                a -> a._1()+" "+a._2()
            );

        if (searchResult != null) {
            searchResult.saveAsTextFile("output_search_phrase");
        }
    }
    
}
