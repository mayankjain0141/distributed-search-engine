package Spark;

public class SparkExecutor {
    public static String SparkDriver(String op){
        System.out.println(op);
        if(op == "wordCount"){
            new YoutubeTitleWordCount().Run();
        }
        return op;
    }


    
}
