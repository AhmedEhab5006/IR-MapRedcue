import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StubReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Collect the positions for each word in each document
      Set<String> positions = new HashSet<>();
      for (Text value : values) {
    	  
          positions.add(value.toString()); // Add the (docID:position) to the set to avoid duplicates
      }
      
      
      Double pos = Double.parseDouble(positions.toString());
      DoubleWritable doubleWritable = new DoubleWritable(pos);
      
      
     

      context.write(key , doubleWritable);
  }
  }
