import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// A Hadoop MapReduce job that counts word occurrences in input text files,
// but only outputs words whose total count exceeds 5.
public class WordCount1 {

  // Mapper: reads each line of input text and emits (word, 1) for every
  // token found, so downstream stages can sum up occurrences per word.
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    // Reusable IntWritable representing the constant count "1" for each
    // occurrence of a word (avoids allocating a new object per token).
    private final static IntWritable one = new IntWritable(1);
    // Reusable Text object to hold the current word (avoids re-allocation).
    private Text word = new Text();

    // key: byte offset of the line in the file (unused here)
    // value: the line of text being processed
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Split the line into whitespace-delimited tokens (words).
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        // Set the word and emit (word, 1) to the framework.
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // Combiner: performs local (per-mapper) aggregation of word counts before
  // data is shuffled across the network to reducers. This reduces the
  // amount of intermediate data transferred. Logically identical to the
  // final reducer's summing behavior, but runs on mapper output.
  public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> { 
    private IntWritable result = new IntWritable();

    // key: a word; values: partial counts of that word from this mapper
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      // Sum all partial counts for this word.
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      // Combiner always emits the partial sum (no filtering here).
      context.write(key, result);
    }
  }

  // Reducer: aggregates all counts for each word across all mappers/combiners
  // and emits the word's total count — but ONLY if that total exceeds 5,
  // acting as a filter on infrequent words in the final output.
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> { 
    private IntWritable result = new IntWritable();

    // key: a word; values: all counts (or combined partial sums) for that word
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      // Sum all counts (or combiner-produced partial sums) for this word.
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      // Only output the word if its total count is greater than 5.
      if ( sum > 5) {
      context.write(key, result);
        }
    }
  }

  public static void main(String[] args) throws Exception {
    // Standard Hadoop job setup.
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    // Set the class containing main() so Hadoop can locate the job's JAR.
    job.setJarByClass(WordCount1.class);
    // Register the mapper class.
    job.setMapperClass(TokenizerMapper.class);
      
    // Register the combiner class for local pre-aggregation.
    job.setCombinerClass(IntSumCombiner.class);
      
    // Register the reducer class for final aggregation/filtering.
    job.setReducerClass(IntSumReducer.class);
    // Define the output key/value types for the job.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // args[0]: input path, args[1]: output path.
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // Submit the job and wait for it to complete; exit with status
    // code 0 on success, 1 on failure.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}