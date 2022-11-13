package Spark;

public class SparkExecutor {
    public static String SparkDriver(String op){
        System.out.println(op);
        if (op == "invertedIndex"){
            new InvertedIndex().Run();
        }
        else if (op == "searchWord"){
            new SearchWord().Run("bigicontop");
        }
        else if (op == "searchPhrase"){
            new SearchPhrase().Run("AND", "modem device that allows computer");
        }
        else if(op == "positivityRank"){
            new PositivityRank().Run();
        }
        return op;
    }


    
}
