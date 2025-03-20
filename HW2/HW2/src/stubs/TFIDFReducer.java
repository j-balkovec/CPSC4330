// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// TFIDFReducer.java
//       Implementation for the TFIDFReducer class
//
// STATUS: Job #3

package stubs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.*;

// CLASS: TFIDFReducer
public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

    private static final Logger logger = Logger.getLogger(TFIDFReducer.class.getName());
    private static final String LOGGER_NAME = "tfidf-reducer-logger";
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

    // DESC: Counts and returns the total number of files in the "cleaned_ebooks" directory.
    // PRE:  Assumes the "cleaned_ebooks" directory exists and contains files.
    // POST: Returns the total number of documents (files) in the directory.
    private static int getTotalNumOfDocuments() {
        int documentCount = 0;
        File dir = new File("ebooks"); // fixed bug
        File[] files = dir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    documentCount++;
                }
            }
        }

        logger.info("total documents in 'cleaned_ebooks' directory: " + documentCount);

        return documentCount;
    }

    // NOTE: initialize once
    private static final int totalDocuments;

    static {
        totalDocuments = getTotalNumOfDocuments(); // Initialize once
    }

    // DESC: Processes the input values, computes TFIDF for each (term, document) pair, and writes the result.
    // PRE:  Input values contain DF and TF data. Total document count is retrieved dynamically.
    // POST: Outputs the TFIDF for each (term, document) pair. Skips computation if DF is 0 to avoid division by zero.
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> documentStats = new ArrayList<>();
        int documentFrequency = 0;

        for (Text value : values) {
            documentStats.add(value.toString());
            documentFrequency++;
        }

        if(documentFrequency == 0){
            logger.severe("document frequency is 0 for term <computing df>: " + key.toString());
        }

        logger.info("\033[1mProcessing key: " + key.toString() + "\033[0m"); // bolded

        for (String documentData : documentStats) {
            String[] tokens = documentData.split("=");
            if (tokens.length < 3) continue;

            String docID = tokens[0];

            // tf, idf, tfidf computations
            double tf, idf = 0, tfidf = 0;
            tf = Double.parseDouble(tokens[1]) / Double.parseDouble(tokens[2]);

            if (documentFrequency > 0) {
                idf = Math.log((double) totalDocuments / documentFrequency);
            } else {
                logger.severe("documentFrequency is 0 for term <computing tfidf>: " + key.toString());
                idf = 0; // Prevent -Infinity issues
            }

            tfidf = tf * idf;

            String outputKey = key.toString() + "@" + docID;
            String tfidfAsString = String.format("%.8f", tfidf);

            context.write(new Text(outputKey), new Text(tfidfAsString));

            logger.fine("computed all 3 for: <" + outputKey + "> | tf: " + tf + " | idf: " + idf + " | tfidf: " + tfidf);

        }

    }
}