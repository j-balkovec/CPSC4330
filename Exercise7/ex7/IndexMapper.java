// Jakob Balkovec
// CPSC 4330
// Fri Jan 31st 2025

// This file provides the implementation of the IndexMapper class

package stubs;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

// CLASS: Mapper
public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  private Text word = new Text();
  private Text location = new Text();

  // PRE: "key" has to exist and hold a valid value, "value" has to hold a valid "value", context needs to be initialized
  // POST: Maps the word and the location as a kvp
  // DESC: reads lines of text from a file and extracts words to create an inverted index.
  @Override
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

    // yoinked from the pdf
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    Path path = fileSplit.getPath();
    String filename = path.getName();

    String lineNumber = key.toString();
    String fileLocation = filename + "@" + lineNumber;

    StringTokenizer tokenizer = new StringTokenizer(value.toString());

    while (tokenizer.hasMoreTokens()) {

      // remove punctuation
      String token = tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", "");

      // ensure it's not empty after cleanup
      if (!token.isEmpty()) {
        word.set(token);
        location.set(fileLocation);
        context.write(word, location); // emit word -> location
      }
    }
  }
}