// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// TFReducer.java
//       Implementation for the TFReducer class
//
// STATUS: Job #1

// TODO: wordcount
// FIXME: wordcount

package stubs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

public class TFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Logger logger = Logger.getLogger(TFReducer.class.getName());
    private static final String LOGGER_NAME = "tf-reducer-logger";
    private static final String LOG_FILE_NAME = "logs/" + LOGGER_NAME + ".log";

    // DESC: Static block; initializes the logger
    // PRE:  None
    // POST: Logger instance is created (name + dir + instance)
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
            System.err.println("Failed to set up " + LOGGER_NAME + " logger: " + e.getMessage());
        }
    }

    // DESC: Aggregates term counts by summing occurrences across documents and calculates term frequency (TF) for each term in its document.
    // PRE: Receives term-document pairs with counts (term@doc, count) from the mapper.
    // POST: Outputs term-document pairs with the computed TF value (term@doc, tf).
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        logger.info("reducing key: " + key.toString());

        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();

            if (val.get() % 10 == 0) { // log every 10th value so we don't overflow the logs
                logger.fine("intermediate sum for key [" + key.toString() + "]: " + sum);
            }
        }
        logger.info("Final sum for key [" + key.toString() + "]: " + sum);

        context.write(key, new IntWritable(sum));
        logger.info("Written output - Key: " + key.toString() + ", Value: " + sum);
    }
}
