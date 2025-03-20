// Jakob Balkovec
// CPSC 4330
// Fri Jan 31st 2025

// Driver (entry point)


package stubs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

// CLASS: InvertedIndex
public class InvertedIndex {

    // PRE: None
    // POST: Job output/Reducer output
    // DESC: Runs MapReduce Job and created an inverted index
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: InvertedIndex <input dir> <output dir>\n");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);


        /*
         * We are using a KeyValueText file as the input file.
         * Therefore, we must call setInputFormatClass.
         * There is no need to call setOutputFormatClass, because the
         * application uses a text file for output.
         */

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);


        // I/O format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        // reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // delete the output path if it already exists
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
