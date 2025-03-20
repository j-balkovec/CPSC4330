package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * Example input line:
     * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
     */

    // IDEA:
    //    Get the [key](96.7.4.14) and the [value](month = Apr) from the line and return it
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // convert the input line to a string and split by spaces
        String line = value.toString();
        String[] parts = line.split(" ");

        // NOTE
        // parts should be:
        // [96.7.4.14, -, -, [24/Apr/2011:04:20:11 -0400], "GET /cat.jpg HTTP/1.1", 200 12433]
        //  >>> [0, 1, 2, 3, 4, 5, 6]

        // guard to ensure formatted as expected
        if (parts.length > 3) {
            String ipAddress = parts[0]; // get IP
            String datePart = parts[3];  // get date

            // remove [] >>> 24/Apr/2011:04:20:11 -0400
            datePart = datePart.replace("[", "")
                    .replace("]", "");

            String[] dateParts = datePart.split("/"); // [date] split by '/' to get the month
            if (dateParts.length > 1) {
                String month = dateParts[1]; // extract month ("Apr")

                // return IP (key), DATE (value)
                context.write(new Text(ipAddress), new Text(month));
            }
        }
    }
}
