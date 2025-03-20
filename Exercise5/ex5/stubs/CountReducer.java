package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, IntWritable> {

    // IDEA:
    //    reduce() needs to group|count the number of instances wrt/ month
    //        >>> output a key-value pair (IP address, total # of hits wrt/ month)
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // var [count] for tracking the # of hits
        // iterate over values to get total # of hits
        // write it as a kvp (IP, # of hits wrt/ month)

        int hitCount = 0;
        for (Text value : values) {
            hitCount++;
        }

        context.write(key, new IntWritable(hitCount));
    }
}
