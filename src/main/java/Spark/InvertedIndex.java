package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;

import java.util.StringTokenizer;

public class InvertedIndex {
    private JavaSparkContext sparkContext;

    public void Run() {
        SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local[3]");
        // Create a new spark context
        sparkContext = new JavaSparkContext(conf);
        // Load Dataset
        JavaPairRDD<String, String> files = sparkContext.wholeTextFiles("data/textfiles/*");

        JavaPairRDD<String, Tuple2<String, Integer>> fileToLine = files.flatMapValues(text -> AddLineNum(text))
                .filter(line -> !line._2()._1().isEmpty());
        JavaPairRDD<String, Tuple3<String, Integer, Integer>> fileToWordInLine = fileToLine
                .flatMapValues(line -> AddWordPos(line))
                .filter(text -> text._2._1().length()>=3)
                .filter(text -> text._2._1().matches("[a-z][^ ]*"));

        JavaPairRDD<String, String> wordToPosInLineInFile = fileToWordInLine
                .mapToPair(f2w -> SwapFileWithWord(f2w));
        JavaPairRDD<String, String> result = wordToPosInLineInFile.
            reduceByKey(
                (a, b) -> {
                    
                    String[] bParts = b.split(":",2);
                    String bFile = bParts[0];
                    int idx = a.indexOf(bFile+":");
                    if(idx < 0){
                        return a+" "+b;
                    } else{
                        // System.out.println("***"+b);
                        String bPosIna = a.substring(idx);
                        String aContentsBeforeB = a.substring(0, idx);
                        String[] aContentsFromB = bPosIna.split(" ", 2);
                        // System.out.println(aContentsFromB);
                        String[] x = aContentsFromB[0].split(":",2);
                        // if(x.length<2)
                        //     System.out.println("***"+a+">>>"+b);
                        String bFileContentsInA = x[1];

                        String bFileContents = bParts[1];
                        return aContentsBeforeB+bFile+":"+bFileContentsInA+bFileContents+(aContentsFromB.length>1?" "+aContentsFromB[1]:"");
                    }
                }
            );

        // JavaPairRDD<String, List<Tuple2<String, List<Tuple2<Integer, List<Integer>>>>>> result = resultRDD.mapValues(
        //         fileRDD -> RddToList1(fileRDD));

        // System.out.println(result.collectAsMap());
        result.saveAsTextFile("output/inverted_index"); 
        sparkContext.close();
    }

    public static List<Tuple2<Integer, List<Integer>>> RddToList2(JavaPairRDD<Integer, List<Integer>> lineRdd) {
        return lineRdd.collect();
    }

    public static List<Tuple2<String, List<Tuple2<Integer, List<Integer>>>>> RddToList1(
            JavaPairRDD<String, JavaPairRDD<Integer, List<Integer>>> fileRDD) {
        return fileRDD.mapValues(
                lineRdd -> RddToList2(lineRdd)).collect();
    }

    public static Tuple2<String, String> SwapFileWithWord(
            Tuple2<String, Tuple3<String, Integer, Integer>> f2w) {
        String[] path = f2w._1().split("/", 0);
        String file = path[path.length-1];
        String line = String.valueOf(f2w._2._2());
        String pos = String.valueOf(f2w._2._3());
        String loc = file+":"+line+"#"+pos+";";
        return new Tuple2<String, String>(f2w._2._1(), loc);
    }

    public static Iterator<Tuple3<String, Integer, Integer>> AddWordPos(Tuple2<String, Integer> line) {
        List<Tuple3<String, Integer, Integer>> words = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(line._1(),
                "[\t\\\n\r \\/\\+\\*\\-\\_\\.\\,\\:\\;\\!\\?\\(\\)\\<\\>\\\"\\{\\}]");
        int wordPos = 0;
        while (st.hasMoreTokens()) {
            // String tok = st.nextToken().replaceAll("\n", "").replaceAll("\r", "");
            words.add(new Tuple3<String, Integer, Integer>(st.nextToken(), line._2(), wordPos));
            wordPos++;
        }
        return words.iterator();
    }

    public static Iterator<Tuple2<String, Integer>> AddLineNum(String text) {
        List<Tuple2<String, Integer>> lines = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(text, "\n");
        int lineNum = 0;
        while (st.hasMoreTokens()) {
            lines.add(new Tuple2<String, Integer>(st.nextToken().toLowerCase(), lineNum));
            lineNum++;
        }
        return lines.iterator();
    }
}
