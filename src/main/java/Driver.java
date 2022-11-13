//import Spark.SparkExecutor;
//
//public class Driver {
//    public static void main(String[] args){
//        // System.out.println(SparkExecutor.SparkDriver("invertedIndex"));
//        // System.out.println(SparkExecutor.SparkDriver("searchWord"));
//        // System.out.println(SparkExecutor.SparkDriver("searchPhrase"));
//        System.out.println(SparkExecutor.SparkDriver("positivityRank"));
//    }
//}
import java.util.Arrays;
        import Spark.SparkExecutor;
        import Hadoop.HadoopExecutor;
import org.jetbrains.annotations.NotNull;

public class Driver {
    public static void main(String @NotNull [] args){
        String framework = args[0];
        String op =  args[1];
        if(framework.toLowerCase() == "spark"){
            switch(op) {
                case "buildIndex":
                    System.out.println(SparkExecutor.buildIndex());
                    break;
                case "searchWord":
                    String word = args[2];
                    System.out.println(SparkExecutor.searchWord(word));
                    break;
                case "searchPhrase":
                    String operation = args[2];
                    String phrase = String.join(",", Arrays.copyOfRange(args, 3, args.length));
                    System.out.println(SparkExecutor.searchPhrase(operation,phrase));
                    break;
                case "positivityRank":
                    System.out.println(SparkExecutor.computePositivityRank());
                    break;
                default:
                    System.out.println("Please enter a valid operation");
            }

        }else if(framework.toLowerCase() == "hadoop"){
            switch(op) {
                case "buildIndex":
                    System.out.println(HadoopExecutor.buildIndex());
                    break;
                case "searchWord":
                    String word = args[2];
                    System.out.println(HadoopExecutor.searchWord(word));
                    break;
                case "searchPhrase":
                    String operation = args[2];
                    String phrase = String.join(",", Arrays.copyOfRange(args, 3, args.length));
                    System.out.println(HadoopExecutor.searchPhrase(operation,phrase));
                    break;
                case "positivityRank":
                    System.out.println(HadoopExecutor.computePositivityRank());
                    break;
                default:
                    System.out.println("Please enter a valid operation");
            }
        }else{
            System.out.println("Please enter one of the valid frameworks i.e. hadoop or spark.");
        }
    }
}