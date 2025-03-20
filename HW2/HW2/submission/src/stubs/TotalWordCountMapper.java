// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// DFMapper.java
//       Implementation for the DFMapper class
//
// STATUS: Job #2

package stubs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.logging.*;

// CLASS: DFMapper
public class TotalWordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = Logger.getLogger(TotalWordCountMapper.class.getName());
    private static final String LOGGER_NAME = "totalwordcount-mapper-logger";
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

    // DESC: Processes input lines, splits them into term and document, and outputs the term-doc pair.
    // PRE:  Assumes input lines are in the format "term@doc\tTF".
    // POST: Outputs a key-value pair with the term and document.
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        if (tokens.length != 2) {
            logger.warning("invalid input format, skipping line: " + value.toString());
            return;
        }

        String[] termDocPair = tokens[0].split("@");
        if (termDocPair.length != 2) {
            logger.warning("invalid term-doc pair, skipping line: " + tokens[0]);
            return;
        }

        String term = termDocPair[0];
        String docID = termDocPair[1];
        String termCount = tokens[1];

        logger.info("processed term: " + term + ", doc: " + docID + ", count: " + termCount);

        String outputValue = term + "=" + termCount;
        context.write(new Text(docID), new Text(outputValue));

        logger.info("emitted key-value pair -> key: " + docID + ", value: " + outputValue);
    }
}
