//package Spark;
//
//public class SparkExecutor {
//    public static String SparkDriver(String op){
//        System.out.println(op);
//        if (op == "invertedIndex"){
//            new InvertedIndex().Run();
//        }
//        else if (op == "searchWord"){
//            new SearchWord().Run("bigicontop");
//        }
//        else if (op == "searchPhrase"){
//            new SearchPhrase().Run("AND", "modem device that allows computer");
//        }
//        else if(op == "positivityRank"){
//            new PositivityRank().Run();
//        }
//        return op;
//    }

package Spark;

public class SparkExecutor {
        public static String buildIndex(){
            try {
                // call build index
                new InvertedIndex().Run();
            }catch(Exception e){
                System.out.println("*** Exception in build index ***");
            }
            return "";
        }
        public static String searchWord(String word){
            try {
                //call search word
                new SearchWord().Run(word);
            }catch(Exception e){
                System.out.println("*** Exception in search word ***");
            }
            return "";
        }
        public static String searchPhrase(String op,String phrase){
            try {
                // call search phrase
                new SearchPhrase().Run(op,phrase);
            }catch(Exception e){
                System.out.println("*** Exception in search phrase ***");
            }
            return "";
        }
        public static String computePositivityRank(){
            try {
                //call ranking func
                new PositivityRank().Run();
            }catch(Exception e){
                System.out.println("*** Exception in computing Positivity rank ***");
            }
            return "";
        }

    }
}
