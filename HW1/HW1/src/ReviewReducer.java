// CPSC 4330
// Jakob Balkovec

// <-- Reducer class implementation -->

// <idea>
// For each "product_id", sum up the total reviews and total ratings
//      compute the average rating and emit the result

// <output>
// [ProductID, TotalReviews, AverageRating]
//      - [1st col] -> ProductID
//      - [2nd col] ->Some random data (irrelevant)
//      - [3rd col] ->Rating

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Logging libraries used for <!-- debug only -->
//                 -> Remove for submission
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

public class ReviewReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    //logging config

    private static final Logger logger = Logger.getLogger(ReviewReducer.class.getName());
    private static final String LOG_FILE_NAME = "logs/review_reducer.log";

    static {
        try {
            File logDir = new File("logs");
            if (!logDir.exists()) {
                logDir.mkdirs();
            }

            // file handler config
            FileHandler fileHandler = new FileHandler(LOG_FILE_NAME);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);

            // logging level
            logger.setLevel(Level.ALL);
        } catch (IOException error) {
            System.err.println("failed to set up logger: " + error.getMessage());
        }
    }

    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        logger.log(Level.INFO, "<user_jb> [LOG]: \"ReviewReducer\" setup completed");
    }

    //logging config

    private final Text result = new Text();

    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        double sum = 0.0;

        // log
        logger.info("processing key: " + key.toString());
        // log

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;

            // log
            logger.fine("adding value: " + val.get() + " to sum: " + sum + ", with count of: " + count);
            // log
        }

        if (count > 0) {
            double avg = sum / count;
            String formattedAvg = String.format("%.2f", avg);

            result.set(count + "\t" + formattedAvg);

            // log
            logger.info("computed avg: " + formattedAvg + " for key: " + key);
            logger.warning("result as hadoop.Text: " + result);
            // log

            context.write(key, result);
        } else {

            // log
            logger.warning("no ratings found for key: " + key);
            // log

            result.set(count + "\tNo Ratings");
            context.write(key, result);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.log(Level.INFO, "<user_jb> [LOG]: \"ReviewReducer\" cleanup completed");
    }
}
