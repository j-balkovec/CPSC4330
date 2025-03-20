// CPSC 4330
// Jakob Balkovec

// <-- Mapper class implementation -->

// <idea>
// Parse each line, extract "product_id" and the rating
//      Emit "product_id" as the key and a tuple (1, rating) as the value
//      NOTE: "1" represents one review


// <input>
// CSV file with at least 3 columns
//      - [1st col] -> ProductID
//      - [2nd col] -> Some random data (irrelevant)
//      - [3rd col] -> Rating
//      - [4th col] -> Some random data (irrelevant)

// Example entry:
//        [B01H5PPJT4,AQ9F73HI3WK17,5.0,1538006400]

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.File;

// Logging libraries used for <!-- debug only -->
//                 -> Remove for submission
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.FileHandler;

public class ReviewMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    // logging config

    private static final Logger logger = Logger.getLogger(ReviewMapper.class.getName());
    private static final String LOG_FILE_NAME = "logs/review_mapper.log";

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

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        logger.log(Level.INFO, "<user_jb> [LOG]: \"ReviewMapper\" setup completed");
    }

    // logging config

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // log
        logger.log(Level.FINE, "processing line: {0}", line);
        // log

        String[] fields = line.split(",");

        if (fields.length >= 3) {
            String productId = fields[0];  // "ProductID" is in the 1st column
            String ratingStr = fields[2]; // "Rating" is in the 3rd column

            try {
                double rating = Double.parseDouble(ratingStr);

                // log
                logger.log(Level.FINE, "parsed rating: {0} for productId: {1}",
                        new Object[]{rating, productId});
                // log

                context.write(new Text(productId), new DoubleWritable(rating));
            } catch (NumberFormatException formatError) {
                // FIXME: add processing logic for invalid lines

                // log
                logger.log(Level.WARNING, "invalid rating found in line: {0}", line);
                // log
            }
        } else {

            // log
            logger.log(Level.WARNING, "line does not have enough fields: {0}", line);
            // log

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.log(Level.INFO, "<user_jb> [LOG]: \"ReviewMapper\" cleanup completed");
    }
}
