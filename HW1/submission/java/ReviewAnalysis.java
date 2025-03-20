// CPSC 4330
// Jakob Balkovec

// Entry point of the program

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

// logging libraries
import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.net.URI;

public class ReviewAnalysis {

    // logging config

    private static final Logger logger = Logger.getLogger(ReviewAnalysis.class.getName());
    private static final String LOG_FILE_NAME = "logs/review_analysis.log";

    static {
        try {

            File logDirectory = new File("logs");
            if (!logDirectory.exists()) {
                logDirectory.mkdirs();
            }

            // file handler config
            FileHandler fileHandler = new FileHandler(LOG_FILE_NAME, true); // Append mode
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);

            // set logging level
            logger.setLevel(Level.ALL);
        } catch (IOException e) {
            System.err.println("Failed to set up logger: " + e.getMessage());
        }
    }

    // logging config

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReviewAnalysis <input path> <output path>");
            logger.severe("Invalid arguments. Usage: ReviewAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReviewAnalysis");

        job.setJarByClass(ReviewAnalysis.class);
        job.setMapperClass(ReviewMapper.class);
        job.setReducerClass(ReviewReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Logging input and output paths
        logger.info("Input path: " + args[0]);
        logger.info("Output path: " + args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);

        // docker + hadoop conf
        // FileSystem fs = FileSystem.get(conf);

        // AWS S3 conf
        FileSystem fs = FileSystem.get(URI.create("s3://hw1-jbalkovec/"), conf);

        // Delete output dir if it already exists
        //   >>>   NOTE: here to prevent hadoop from throwing an error
        if (fs.exists(outputPath)) {
            logger.warning("Output path exists. Deleting: " + outputPath.toString());
            fs.delete(outputPath, true); // delete the output path if it already exists
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        logger.info("Starting job execution...");
        boolean success = job.waitForCompletion(true);

        int exitCode = success ? 0 : 1;

        if (success) {
            logger.info("Job completed successfully.");
            System.exit(exitCode);
        } else {
            logger.severe("Job failed.");
            System.exit(exitCode);
        }
    }
}