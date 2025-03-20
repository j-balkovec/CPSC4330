package stubs;

import java.time.Month;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K2, V2> extends Partitioner<Text, Text> implements Configurable {

    private Configuration configuration;
    HashMap<String, Integer> months = new HashMap<String, Integer>();

    /**
     * Set up the months hash map in the setConf method.
     */
    @Override
    public void setConf(Configuration configuration) {
        /*
         * Add the months to a HashMap.
         */

        /// NOTE: use the pre-defined enum in the "java.time" library
        ///     >>> NOTE: for more details see "java.time"

        // HashMap entry:
        // Month.values() = string repr of month
        // month.ordinal() = int repr of the month

        this.configuration = configuration;

        //  >>> kvp: (XXX, XXX#); where XXX are the first 3 letters of the month
        for (Month month : Month.values()) {
            months.put(month.name().substring(0, 3), month.ordinal());
        }
    }

    /**
     * Implement the getConf method for the Configurable interface.
     */
    @Override
    public Configuration getConf() {

        // try catch block
        //  >>> make a copy of the configuration, if == null, then throw error, else return conf

        try {
            if (configuration == null) {
                throw new NullPointerException("<error>: configuration is null");
            }

            return new Configuration(configuration);
        } catch (NullPointerException e) {
            System.out.println("<error>: " + e.getMessage());

        } catch (Exception e) {
            System.out.println("<error>: an unexpected error occurred: " + e.getMessage());
        }

        return null; // return clause to avoid compilation error
    }

    /**
     * You must implement the getPartition method for a partitioner class.
     * This method receives the three-letter abbreviation for the month
     * as its value. (It is the output value from the mapper.)
     * It should return an integer representation of the month.
     * Note that January is represented as 0 rather than 1.
     * <p>
     * For this partitioner to work, the job configuration must have been
     * set so that there are exactly 12 reducers.
     */
    public int getPartition(Text key, Text value, int numReduceTasks) {
        // map the month (value) to the appropriate reducer number.
        // use the months HashMap to get the month index and calculate the partition.

        // check if key or value is null
        if (key == null || value == null) {
            throw new IllegalArgumentException("<error>: key or value is null");
        }

        // check if the value (month) exists in the months map
//        if (!months.containsKey(value.toString())) {
//            throw new IllegalArgumentException("<error>: invalid month value: " + value.toString());
//        }

        // get index
        int monthIndex = months.get(value.toString());

        // ensure it maps to one of the 12 reducers
        return monthIndex % numReduceTasks;
    }
}
