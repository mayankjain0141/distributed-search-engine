package Hadoop;

public class HadoopExecutor{
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
        }catch(Exception e){
            System.out.println("*** Exception in search phrase ***");
        }
        return "";
    }
    public static String computePositivityRank(){
        try {
            //call ranking func
        }catch(Exception e){
            System.out.println("*** Exception in computing Positivity rank ***");
        }
        return "";
    }
}
