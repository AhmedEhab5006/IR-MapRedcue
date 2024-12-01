import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StubDriver {
	public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PositionalIndex <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf , "Positional Index");
        job.setJarByClass(StubTest.class);
        job.setMapperClass(StubMapper.class);
        job.setCombinerClass(StubReducer.class);
        job.setReducerClass(StubReducer.class);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        
     // Set the input folder
        FileInputFormat.addInputPath(job, new Path(args[0])); // Process all files in the folder

        // Set the output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}
}

