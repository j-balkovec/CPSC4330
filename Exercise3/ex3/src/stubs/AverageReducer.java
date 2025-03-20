// Jakob Balkovec
// CPSC 4330
// Wed 15 Jan 2025

// Reducer class implementation

package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  //FIXME: expected org.apache.hadoop.io.DoubleWritable, received org.apache.hadoop.io.IntWritable

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    // <intuition>: Grab the first letter and the list that contains lengths.
    //              Reduce over that to get the average. Use the size of the list
    //              and the num of items.

    // init our sum and count variables
    double sum = 0;
    int count = 0;

    // Iterate through the list of values for the given key
    for (IntWritable val : values) {
      sum += val.get();
      count++;
    }

    // Average | Ternary operator for concisenes
    double average = (count == 0) ? 0.0 : (double) sum / count;

    // Write the output: key (first letter) and average length
    context.write(key, new DoubleWritable(average));
  }
}