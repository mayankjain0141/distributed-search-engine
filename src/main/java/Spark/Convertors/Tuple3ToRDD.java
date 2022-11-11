package Spark.Convertors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

import Spark.InvertedIndex;

public class Tuple3ToRDD implements
        Function<Tuple3<String, Integer, Integer>, JavaPairRDD<String, JavaPairRDD<Integer, List<Integer>>>> {

    public JavaPairRDD<String, JavaPairRDD<Integer, List<Integer>>> call(Tuple3<String, Integer, Integer> path) {
        List<Integer> wordPos = new ArrayList<>();
        wordPos.add(path._3());
        List<Tuple2<Integer, List<Integer>>> lineWordPairs = new ArrayList<>();
        lineWordPairs.add(new Tuple2<>(path._2(), wordPos));
        JavaPairRDD<String, JavaPairRDD<Integer, List<Integer>>> fileLineWord = null;
        try {
            JavaPairRDD<Integer, List<Integer>> lineWordRdd = InvertedIndex.sparkContext
                    .parallelizePairs(lineWordPairs);
            List<Tuple2<String, JavaPairRDD<Integer, List<Integer>>>> fileLineWordPairs = new ArrayList<>();
            fileLineWordPairs.add(new Tuple2<>(path._1(), lineWordRdd));
            fileLineWord = InvertedIndex.sparkContext.parallelizePairs(fileLineWordPairs);
        } catch (NullPointerException e) {
            System.out.println("sc is nil..." + e);
        }
        return fileLineWord;
    }
}
