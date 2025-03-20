// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// DFReducer.java
//       Implementation for the DFReducer class
//
// STATUS: Job #2

package stubs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.*;

// CLASS: DFReducer
public class TotalWordCountReducer extends Reducer<Text, Text, Text, Text> {

    private static final Logger logger = Logger.getLogger(TotalWordCountReducer.class.getName());
    private static final String LOGGER_NAME = "totalwordcount-reducer-logger";
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

    // DESC: Calculates the document frequency (DF) for each term and outputs the result.
    // PRE:  Input contains a list of document names for each term.
    // POST: Outputs the term and its corresponding DF (document frequency).
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        logger.info("processing document: " + key.toString());

        int totalWordCount = 0;
        Map<String, Integer> termCounts = new HashMap<>();

        for (Text value : values) {
            String[] parts = value.toString().split("=");

            if (parts.length < 2) {
                logger.warning("skipping invalid input: " + value.toString());
                continue;
            }

            String term = parts[0];
            int count = 0;

            try {
                count = Integer.parseInt(parts[1]);
            } catch (NumberFormatException numFormatError) {
                logger.warning("invalid number format in input: " + value.toString());
                continue;
            }

            totalWordCount += count;
            termCounts.put(term, count);

            logger.info("term: " + term + ", count: " + count);


        }

        if (totalWordCount == 0) {
            logger.warning("total word count is zero for document: " + key.toString());
            return;
        }

        for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {

            String termDocKey = entry.getKey() + "@" + key.toString();
            String outputValue = entry.getValue() + "\t" + totalWordCount;

            context.write(new Text(termDocKey), new Text(outputValue));

            logger.info("emitted key-value pair -> key: " + termDocKey + ", value: " + outputValue);
        }
    }
}
