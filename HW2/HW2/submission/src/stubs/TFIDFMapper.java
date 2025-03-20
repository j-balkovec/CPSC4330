// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// TFIDFMapper.java
//       Implementation for the TFIDFMapper class
//
// STATUS: Job #3

package stubs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import scripts.DataCleaner;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

// CLASS: TFIDFMapper
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = Logger.getLogger(TFIDFMapper.class.getName());
    private static final String LOGGER_NAME = "tfidf-mapper-logger";
    private static final String LOG_FILE_NAME = "logs/" + LOGGER_NAME + ".log";

    private Text term = new Text();
    private Text valueOut = new Text();

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

    // DESC: Processes input, splitting into term-doc pairs for TF or DF data and outputs the term with value.
    // PRE:  Assumes input format "term@doc\tvalue" for TF data and "term\tDF" for DF data.
    // POST: Outputs term with either TF or DF value.
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        if (tokens.length < 3) {
            logger.warning("skipping malformed input: " + value.toString());
            return;
        }

        String[] termDocPair = tokens[0].split("@");
        if (termDocPair.length < 2) {
            logger.warning("skipping malformed term-doc pair: " + tokens[0]);
            return;
        }

        String term = termDocPair[0];
        String docID = termDocPair[1];
        int termCount = Integer.parseInt(tokens[1]);
        int totalWords = Integer.parseInt(tokens[2]);

        String outputValue = docID + "=" + termCount + "=" + totalWords;
        context.write(new Text(term), new Text(outputValue));

        logger.fine("processed key: " + key.get() + " | Term: " + term + " | Output: " + outputValue);
    }
}
