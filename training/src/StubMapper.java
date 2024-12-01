import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StubMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text word = new Text();
	private IntWritable position = new IntWritable();

	
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get document ID and the content of the document (line)
        String docID = value.toString().split("\t")[0]; // Assuming format "docID<tab>content"
        String line = value.toString().split("\t")[1];
        
        
     
        String[] words = line.split("\\s+");
        for (int i = 0; i < words.length; i++) {
            String cleanWord = words[i].replaceAll("[^a-zA-Z0-9]", "").toLowerCase(); // Clean and lowercase the word

            if (!cleanWord.isEmpty()) {
                word.set(cleanWord);
                position.set(i); 
                context.write(word, position);
            }
  
  }
  }
}
