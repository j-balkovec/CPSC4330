// Jakob Balkovec
// CPSC 4330
// Fri Jan 31st 2025

// This file provides the implementation of the IndexReducer class

package stubs;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values.
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word.
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  private static final String SEPARATOR = ", ";
  // PRE: key has to exist and hold a valid value, values has to be iterable and non empty, context must be initialized (valid instance)
  // POST: word -> filename@lineNumber, filename@lineNumber, ...
  // DESC: builds a readable string in the valueList variable that contains an index of all the locations of the word :).
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {

    StringBuilder valueList = new StringBuilder();

    Iterator<Text> iterator = values.iterator();
    while (iterator.hasNext()) {
      valueList.append(iterator.next().toString());
      if (iterator.hasNext()) {
        valueList.append(SEPARATOR); // separate locations with commas
      }
    }

    // (word, occurances[list])
    context.write(key, new Text(valueList.toString()));
  }
}