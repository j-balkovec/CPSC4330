// Jakob Balkovec
// CPSC 4330
// Wed 15 Jan 2025

// Mapper class implementation

package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  // can be overridden in subclasses if needed
  public LetterMapper() {
    super();
  }

  //FIXME: expected org.apache.hadoop.io.DoubleWritable, received org.apache.hadoop.io.IntWritable

  @Override
  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    // <intuition>: Take the first letter of the word (case-sensitive)
    //              and pair it with the length of the word

    // Make it a string | [key: LongWritable, value: Text]
    String line = value.toString();

    // Split the line into words using regex to handle non-word characters
    for (String word : line.split("\\W+")) {

      if (!word.isEmpty()) {
        // Extract the first letter of the word
        String firstLetter = word.substring(0, 1);
        int lengthOfWord = word.length();

        // Write the extracted letter and the length of the word
        context.write(new Text(firstLetter), new IntWritable(lengthOfWord));
      }
    }
  }
}
