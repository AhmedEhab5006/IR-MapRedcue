package posIndexFinal2024;


import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StubMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();
    private Text position = new Text();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] parts = value.toString().split("\t");

        // Check if the line has both a docID and content
        if (parts.length >= 2) {
            String docID = parts[0];   // Document ID
            String line = parts[1];    // Content of the document

            // Split the content into words using space as delimiter
            String[] words = line.split("\\s+");


            for (int i = 0; i < words.length; i++) {
                // Clean and lowercase the word
                String cleanWord = words[i].replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

                // Check if the cleaned word is not empty
                if (!cleanWord.isEmpty()) {
                    word.set(cleanWord);
                    position.set(docID + ":" + i);  // Set the word's position
                    context.write(word, position);  // Write the word and its position to context
                }

            }
        }
    }
}

