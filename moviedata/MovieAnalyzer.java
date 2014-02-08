package moviedata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAnalyzer {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		if(args.length == 3) {
			
			Configuration conf = new Configuration();   
			
			if (args[2].matches("^\\d+$")) {
				// Check if the argument three (n) is a number
				conf.set("n", args[2]);
			}
		    Job job = new Job(conf, "userRatings");
		    
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setJarByClass(MovieAnalyzer.class);
		    job.setMapperClass(UserRating.Map.class);
		    job.setCombinerClass(UserRating.Reduce.class);
		    job.setReducerClass(UserRating.Reduce.class);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    job.waitForCompletion(true);
		}
		else {
			System.out.println("Expecting three input arguments! <Input Path><Output Path><n value>");
		}
	}

}
