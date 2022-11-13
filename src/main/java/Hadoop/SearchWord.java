package Hadoop;
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

public class SearchWord {

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
        private Text word2 = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String value_raw = value.toString();

            Configuration conf = context.getConfiguration();
            String searchString = conf.get("word");
            StringTokenizer itr = new StringTokenizer(value_raw, "\n");

            while (itr.hasMoreTokens()) {
                String line = itr.nextToken().toString();
                String[] arrOfVal = line.split("	", 2);
                if (arrOfVal.length == 2) {
                    if (arrOfVal[0].equals(searchString.toLowerCase())){
                        // System.out.println(line);
                        word.set(arrOfVal[0]);
                        word2.set(arrOfVal[1]);
                        context.write(word, word2);
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key,val);
            }
        }
    }

    public void Run(String word) throws Exception {
        Configuration conf = new Configuration();
        conf.set("word",word);

        Job job = Job.getInstance(conf, "search word");
        job.setJarByClass(SearchWord.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(conf);
        Path in = new Path("data/textfiles");
        Path out = new Path("output_inverted_index");
        // if (fs.exists(out)) {
        // fs.delete(out, true);
        // }

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("word",args[2]);

        Job job = Job.getInstance(conf, "search word");
        job.setJarByClass(SearchWord.class);
        job.setMapperClass(TokenizerMapper.class);

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

