
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

  /*
   * This is the Mapper class. It extends the hadoop's Mapper class.
   * This maps input key/value pairs to a set of intermediate(output) key/value
   * pairs.
   * Here our input key is a Object and input value is a Text.
   * And the output key is a Text and value is an Text. [word<Text>
   * DocID<Text>]<->[aspect 5722018411]
   */
  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, Text> {

    /*
     * Hadoop supported datatypes. This is a hadoop specific datatype that is used
     * to handle
     * numbers and Strings in a hadoop environment. IntWritable and Text are used
     * instead of
     * Java's Integer and String datatypes.
     * Here 'one' is the number of occurance of the 'word' and is set to value 1
     * during the
     * Map process.
     */
    // private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String DocId = fileSplit.getPath().getName();
      // Split DocID and the actual text
      // String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
      String value_raw = value.toString();

      // Reading input one line at a time and tokenizing by using space, "'", and "-"
      // characters as tokenizers.
      StringTokenizer itr = new StringTokenizer(value_raw, "\n");
      int lineNum = 0;
      // Iterating through all the words available in that line and forming the
      // key/value pair.
      while (itr.hasMoreTokens()) {
        StringTokenizer wordItr = new StringTokenizer(itr.nextToken().toString(),
            "[\t\\\n\r \\/\\+\\*\\-\\_\\.\\,\\:\\;\\!\\?\\(\\)\\<\\>\\\"\\{\\}]&#~@$%^*|");
        int wordPos = 0;
        while (wordItr.hasMoreTokens()) {
          // Remove non alphabetic tokens
          String tempWord = wordItr.nextToken().toLowerCase();
          if (!tempWord.matches("^[a-z_][^ ]*")) {
            continue;
          }
          word.set(tempWord);
          if (word.toString() != "" && !word.toString().isEmpty()) {
            /*
             * Sending to output collector(Context) which in-turn passed the output to
             * Reducer.
             * The output is as follows:
             * 'word1' 5722018411
             * 'word1' 6722018415
             * 'word2' 6722018415
             */
            String valString = DocId + "," + lineNum + "#" + wordPos;
            context.write(word, new Text(valString));
          }
          wordPos++;
        }
        lineNum++;
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text, Text, Text, Text> {
    /*
     * Reduce method collects the output of the Mapper calculate and aggregate the
     * word's count.
     */
    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {

      // HashMap<String, Integer> map = new HashMap<String, Integer>();
      HashMap<String, ArrayList<String>> mp = new HashMap<String, ArrayList<String>>();

      /*
       * Iterable through all the values available with a key [word] and add them
       * together and give the
       * final result as the key and sum of its values along with the DocID.
       */

      for (Text val : values) {
        String[] arrOfVal = val.toString().split(",", 2);
        if (arrOfVal.length == 2) {
          String docID = arrOfVal[0];
          String wordLoc = arrOfVal[1];
          if (mp.containsKey(docID)) {
            mp.get(docID).add(wordLoc);
          } else {
            ArrayList<String> list = new ArrayList<String>();
            list.add(wordLoc);
            mp.put(docID, list);
          }
        }
      }
      StringBuilder docValueList = new StringBuilder();
      docValueList.append("(");
      for (String docID : mp.keySet()) {
        StringBuilder docVal = new StringBuilder();
        for (String s : mp.get(docID)) {
          docVal.append(s);
          docVal.append(";");
        }
        docValueList.append(docID + ":" + docVal.toString() + " ");
      }
      docValueList.append(")");

      context.write(key, new Text(docValueList.toString()));
    }
  }

  public void Run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    // Commend out this part if you want to use combiner. Mapper and Reducer input
    // and outputs type matching might be needed in this case.
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileSystem fs = FileSystem.get(conf);
    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    // if (fs.exists(out)) {
    // fs.delete(out, true);
    // }

    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    // Commend out this part if you want to use combiner. Mapper and Reducer input
    // and outputs type matching might be needed in this case.
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileSystem fs = FileSystem.get(conf);
    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    // if (fs.exists(out)) {
    // fs.delete(out, true);
    // }

    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
