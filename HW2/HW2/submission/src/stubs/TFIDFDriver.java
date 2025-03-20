// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// TFIDF.java
//       Driver

// TODO: integrate DataCleaner into mapper
package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

public class TFIDFDriver {

    // Logging config
    private static final Logger logger = Logger.getLogger(TFIDFDriver.class.getName());
    private static final String LOGGER_NAME = "tfidf-logger";
    private static final String LOG_FILE_NAME = "logs/" + LOGGER_NAME + ".log";

    static {
        try {
            File logDirectory = new File("logs");
            if (!logDirectory.exists()) {
                logDirectory.mkdirs();
            }

            Handler[] handlers = logger.getHandlers();
            for (Handler handler : handlers) {
                if (handler instanceof ConsoleHandler) {
                    logger.removeHandler(handler);
                }
            }

            // file handler config
            FileHandler fileHandler = new FileHandler(LOG_FILE_NAME, true); // append mode
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);

            // set logging level
            logger.setLevel(Level.ALL);
        } catch (IOException e) {
            System.err.println("Failed to set up logger: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: TFIDFDriver <input dir> <temp1 dir> <temp2 dir> <output dir>");
            logger.severe("invalid arguments. expected 4 directories.");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // removing output and temp directories before execution
        Path temp1Path = new Path(args[1]);
        Path temp2Path = new Path(args[2]);
        Path outputPath = new Path(args[3]);

        logger.info("checking and deleting existing directories if they exist");

        if (fs.exists(temp1Path)) {
            logger.info("deleting temp1 directory: " + temp1Path.toString());
            fs.delete(temp1Path, true);
        }

        if (fs.exists(temp2Path)) {
            logger.info("deleting temp2 directory: " + temp2Path.toString());
            fs.delete(temp2Path, true);
        }

        if (fs.exists(outputPath)) {
            logger.info("deleting output directory: " + outputPath.toString());
            fs.delete(outputPath, true);
        }

        // Job 1: compute tf
        // ---------------------------------------------------------
        Job job1 = Job.getInstance(conf, "TF Count");
        job1.setJarByClass(TFIDFDriver.class);
        job1.setMapperClass(TFMapper.class);
        job1.setReducerClass(TFReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            logger.severe("job 1 failed.");
            System.exit(1);
        }
        logger.info("job 1 completed successfully");
        // ---------------------------------------------------------


        // Job 2: compute total word count
        // ---------------------------------------------------------
        Job job2 = Job.getInstance(conf, "Word Count");
        job2.setJarByClass(TFIDFDriver.class);
        job2.setMapperClass(TotalWordCountMapper.class);
        job2.setReducerClass(TotalWordCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        if (!job2.waitForCompletion(true)) {
            logger.severe("job 2 failed.");
            System.exit(1);
        }
        logger.info("job 2 completed successfully");
        // ---------------------------------------------------------


        // Job 3: compute tfidf
        // ---------------------------------------------------------
        Job job3 = Job.getInstance(conf, "TF-IDF");
        job3.setJarByClass(TFIDFDriver.class);
        job3.setMapperClass(TFIDFMapper.class);
        job3.setReducerClass(TFIDFReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        if (!job3.waitForCompletion(true)) {
            logger.severe("job 3 failed.");
            System.exit(1);
        }
        logger.info("job 3 completed successfully");
        // ---------------------------------------------------------

        logger.info("all jobs finished successfully. exiting now.");
        System.exit(0);
    }
}