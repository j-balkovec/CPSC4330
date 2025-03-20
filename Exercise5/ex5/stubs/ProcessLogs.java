package stubs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class ProcessLogs {

  // num tasks
  private static final int NUM_REDUCERS = 12;

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Process Logs");

    job.setJarByClass(ProcessLogs.class);

    // set I/O paths
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setMapperClass(LogMonthMapper.class);
    job.setReducerClass(CountReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // partitioner conf
    job.setPartitionerClass(MonthPartitioner.class);
    job.setNumReduceTasks(NUM_REDUCERS); // total number of tasks = 12

    // Delete output dir if it already exists
    //   >>>   NOTE: here to prevent hadoop from throwing an error
    //   >>>   NOTE: docker + hadoop conf
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true); // delete the output path if it already exists
    }

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}