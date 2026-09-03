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

// import LongWritable type and RegexMapper class
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;

public class URLCount1 {

    // Remove TokenizerMapper as unnecessary

    // change typing of function to LongWritable. Retain functionality.
    public static class LongSumCombiner
       extends Reducer<Text,LongWritable,Text,LongWritable> { 
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  // change typing of function to LongWritable. Retain functionality.    
  public static class LongSumReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> { 
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if ( sum > 5) {
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // setup RegexMapper with URL pattern (provided)
    conf.set(RegexMapper.PATTERN, "href=\"([^\"]*)\"");
    // match on group 1 to only get URL not href metadata
    conf.set(RegexMapper.GROUP, "1");
    
    Job job = Job.getInstance(conf, "url count");
    job.setJarByClass(URLCount1.class);

    // use our RegexMapper
    job.setMapperClass(RegexMapper.class);

    // change reducers to Long type
    job.setCombinerClass(LongSumCombiner.class);
    job.setReducerClass(LongSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    
    // set output to Long type
    job.setOutputValueClass(LongWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
