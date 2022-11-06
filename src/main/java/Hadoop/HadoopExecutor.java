package Hadoop;

public class HadoopExecutor {
    public static String HadoopDriver(String op){
        System.out.println(op);
        if(op == "invertedIndex"){
            try{
                new InvertedIndex().Run();
            } catch(Exception e){
                System.out.println("exception in InvertedIndex");
            }
        }
        return op;
    }
}
