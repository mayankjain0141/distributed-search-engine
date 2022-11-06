import Spark.SparkExecutor;
import Hadoop.HadoopExecutor;

public class Driver {
    public static void main(String[] args){
        // System.out.println(SparkExecutor.SparkDriver("wordCount"));
        System.out.println(HadoopExecutor.HadoopDriver("invertedIndex"));
    }
}
