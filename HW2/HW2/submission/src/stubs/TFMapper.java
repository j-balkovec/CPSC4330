// Jakob Balkovec
// CPSC 4330
//      Fri Jan 31st
//
// TFMapper.java
//       Implementation for the TFMapper class
//
// STATUS: Job #1

package stubs;

import org.apache.hadoop.io.*;          // import everything
import org.apache.hadoop.mapreduce.*;   // import everything
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import scripts.DataCleaner;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.*;

// CLASS: TFMapper
public class TFMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final Text wordKey = new Text();
    private static final IntWritable one = new IntWritable(1);

    private static final Logger logger = Logger.getLogger(TFMapper.class.getName());
    private static final String LOGGER_NAME = "tf-mapper-logger";
    private static final String LOG_FILE_NAME = "logs/" + LOGGER_NAME + ".log";

    // DESC: Static block; initializes the logger
    // PRE: None
    // POST: Logger instance is created (name + dir + instance)
    static {
        try {

            File logDirectory = new File("logs");
            if (!logDirectory.exists()) {
                logDirectory.mkdirs();
            }

            // remove console logger from root logger
            Logger rootLogger = Logger.getLogger("");
            Handler[] handlers = rootLogger.getHandlers();
            for (Handler handler : handlers) {
                if (handler instanceof ConsoleHandler) {
                    rootLogger.removeHandler(handler);
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

    // DESC: Tokenizes input lines into terms, associates each term with its document, and outputs term-document pairs with a count of 1.
    // PRE:  Assumes input data is cleaned, with terms in lowercase and non-alphabetical characters removed.
    // POST: Outputs term-document pairs (term@doc, 1) to the context for further processing.
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        logger.info("\033[1mProcessing file: " + fileName + "\033[0m"); // bolded

        String cleanedLine = DataCleaner.cleanLine(value.toString()); // clean line before tokenizing @see: scripts.DataCleaner
        StringTokenizer tokenizer = new StringTokenizer(cleanedLine, " ");

        while (tokenizer.hasMoreTokens()) {
            String term = tokenizer.nextToken();
            if (!term.isEmpty()) {
                String output = term + "@" + fileName;
                wordKey.set(output);
                context.write(wordKey, one);

                logger.fine("processed term: " + output);
            } else {
                logger.warning("skipped empty term in file: " + fileName);
            }
        }
    }
}
