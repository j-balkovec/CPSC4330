// Jakob Balkovec
// CPSC 4330
//      Wed Jan 29th
//
// DataCleaner.java
//       Cleans the data/files of stop words
//
//       NOTE: Stop words yoinked from here: https://gist.github.com/sebleier/554280
//       NOTE: Moved/Copied to file "StopWords.txt"

// STATUS: Done

// TEST:
//
// test using "grep" + "word" + "file" --- <grep "here" 1080-0.txt>
// Examples of stop words: [once, here, there, when, where]
//

// COMPILATION:
// from src: javac -d . scripts/DataCleaner.java
// to run: java scripts.DataCleaner

package scripts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.*;

// FIXME: words are getting all fucked up when cleaned: example ==> twentyone || fivestoried
// replace: - w whitespace and -- w whitespace

// CLASS: Utility for cleaning the data/files of stop words
public class DataCleaner {

    private static final Logger logger = Logger.getLogger(DataCleaner.class.getName());
    private static final String LOGGER_NAME = "data-cleaner-logger";
    private static final String LOG_FILE_NAME = "logs/" + LOGGER_NAME + ".log";

    private static final String STOP_WORDS_FILE = new File("scripts/StopWords.txt").getAbsolutePath();
    private static final Set<String> STOP_WORDS = new HashSet<>();

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

    // PRE:  "StopWords.txt" file must exist at the specified path.
    // POST: The STOP_WORDS set is populated with all stop words from the file.
    // DESC: Loads stop words from a file into a HashSet for quick lookup.
    public static void LoadStopWords() throws IOException {
        logger.info("Loading stop words from: " + STOP_WORDS_FILE);
        try (BufferedReader br = new BufferedReader(new FileReader(STOP_WORDS_FILE))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (!line.isEmpty()) {
                    STOP_WORDS.add(line);
                    count++;
                }
            }
            logger.info("Loaded " + count + " stop words.");
        } catch (IOException e) {
            logger.severe("Failed to load stop words: " + e.getMessage());
            throw e;
        }
    }

    // PRE:  Input string must be non-null and contain raw text.
    // POST: Returns a cleaned string with stop words and special characters removed.
    // DESC: Processes a line by removing punctuation, converting to lowercase, and filtering out stop words.
    public static String cleanLine(String line) {
        logger.fine("Cleaning line: " + line);

        // "?", "!", ".", ",",...
        // \\W+
        String OLDcustomRegex = "(\\B'\\b|\\b'\\B|[^\\w'])";
        String NEWcustomRegex = "^[a-zA-Z\\s]+$";

        line = line.replaceAll(NEWcustomRegex, " ").toLowerCase();

        StringBuilder cleanedLine = new StringBuilder();
        String[] words = line.split("\\s+");

        int removedWords = 0;
        for (String word : words) {
            if (!word.isEmpty() && !STOP_WORDS.contains(word)) {
                cleanedLine.append(word).append(" ");
            } else {
                removedWords++;
            }
        }

        String result = cleanedLine.toString().trim();
        logger.fine("Cleaned line: " + result + " | Removed words: " + removedWords);
        return result;
    }
}
