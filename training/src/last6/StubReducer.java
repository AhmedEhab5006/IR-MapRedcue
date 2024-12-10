package last6;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class StubReducer extends Reducer<Text, Text, Text, Text> {

    private List<String> sortDocIdsNumerically(Map<String, List<Integer>> positionalIndex) {

        List<String> docIds = new ArrayList<>(positionalIndex.keySet());
        Collections.sort(docIds, new Comparator<String>() {
            @Override
            public int compare(String docId1, String docId2) {
                int id1 = Integer.parseInt(docId1);
                int id2 = Integer.parseInt(docId2);
                return Integer.compare(id1, id2);
            }
        });
        return docIds;
    }
    
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Collect the positions for each word in each document
        Map <String , List<Integer> > positionalIndex = new HashMap<>();
        for (Text value : values) {
        	
        	String docIDandPos[] = value.toString().split(":");
            String docID = docIDandPos[0];
            int pos = Integer.parseInt(docIDandPos[1]);
            
          //============================================

            if (!positionalIndex.containsKey(docID)) {
                positionalIndex.put(docID, new ArrayList<Integer>());
            }
            positionalIndex.get(docID).add(pos);


        }
        List<String> sortedDocIds = sortDocIdsNumerically(positionalIndex);
        StringBuilder indexBuilder = new StringBuilder();
        
        for (String docId : sortedDocIds) {
            List<Integer> positions = positionalIndex.get(docId);



            Collections.sort(positions);
            
            StringBuilder positionsBuilder = new StringBuilder();
            for (int i = 0; i < positions.size(); i++) {
                positionsBuilder.append(positions.get(i) + 1);
                if (i < positions.size() - 1) {
                    positionsBuilder.append(",");
                }

            }
            indexBuilder.append(docId).append(":");
            indexBuilder.append(positionsBuilder.toString());
            indexBuilder.append(";");
        }
        if (indexBuilder.length() > 0) {
            indexBuilder.setLength(indexBuilder.length() - 1);
        }
        
        
        context.write(key, new Text(indexBuilder.toString()));

    }

}

            
        
