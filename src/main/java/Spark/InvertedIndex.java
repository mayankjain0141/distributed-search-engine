package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import Spark.Convertors.Tuple3ToRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;

import java.util.StringTokenizer;

public class InvertedIndex {
    public static JavaSparkContext sparkContext;

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
                .filter(text -> text._2._1().matches("[a-z]+('[a-z]+){0,1}"));

        JavaPairRDD<String, Tuple3<String, Integer, Integer>> wordToPosInLineInFile = fileToWordInLine
                .mapToPair(f2w -> SwapFileWithWord(f2w));
        Tuple3ToRDD t3tordd = new Tuple3ToRDD();
        JavaPairRDD<String, JavaPairRDD<String, JavaPairRDD<Integer, List<Integer>>>> resultRDD = wordToPosInLineInFile
                .mapValues(t3tordd)
                .reduceByKey(
                        (field1, field2) -> {
                            return field1.union(field2).reduceByKey(
                                    (a, b) -> {
                                        return a.union(b).reduceByKey(
                                                (x, y) -> {
                                                    x.addAll(y);
                                                    return x;
                                                });
                                    });
                        });

        JavaPairRDD<String, List<Tuple2<String, List<Tuple2<Integer, List<Integer>>>>>> result = resultRDD.mapValues(
                fileRDD -> RddToList1(fileRDD));

        result.coalesce(1).saveAsTextFile("output/inverted_index");
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

    public static Tuple2<String, Tuple3<String, Integer, Integer>> SwapFileWithWord(
            Tuple2<String, Tuple3<String, Integer, Integer>> f2w) {
        return new Tuple2<String, Tuple3<String, Integer, Integer>>(f2w._2._1(),
                new Tuple3<String, Integer, Integer>(f2w._1(), f2w._2._2(), f2w._2._3()));
    }

    public static Iterator<Tuple3<String, Integer, Integer>> AddWordPos(Tuple2<String, Integer> line) {
        List<Tuple3<String, Integer, Integer>> words = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(line._1(),
                "[\\p{javaWhitespace}\\.\\,\\:\\;\\!\\?\\(\\)\\<\\>\\\"\\{\\}]");
        int wordPos = 0;
        while (st.hasMoreTokens()) {
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
