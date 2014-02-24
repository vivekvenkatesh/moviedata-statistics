/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Description
 * -----------
 * Main Class to create the appropriate Map Reduce Jobs based on the User Input
 * 
 */
package moviedata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAnalyzer {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		if(args.length == 4) {
			if(args[2].equals("UserRating")) {
				/*
				 * Find the user ids who have rated at least 'n' movies
				 */
				Configuration conf = new Configuration();   

				if (args[3].matches("^\\d+$")) {
					// Check if the argument four (n) is a number
					conf.set("n", args[3]);
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
			} // End of UserRating job
			else if (args[2].equals("MovieGenre")) {
				/*
				 * Display the list of Genres with input given as Command Line Arguments separated by ::
				 */
				Configuration conf = new Configuration();
				conf.set("title", args[3]);
				Job job = new Job(conf, "movieGenre");
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				job.setJarByClass(MovieAnalyzer.class);
				job.setMapperClass(MovieGenre.Map.class);
				job.setReducerClass(MovieGenre.Reduce.class);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));

				job.waitForCompletion(true);
			} // End of Movie Genre job
			else if (args[2].equals("TopTenZip")) {
				/*
				 * Chaining Jobs to compute the top ten zipcodes based on the user age
				 */
				Configuration conf = new Configuration();
				Job job = new Job(conf, "ageZipCode");
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(AgeAverageCountTuple.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(AgeAverageCountTuple.class);
				job.setJarByClass(MovieAnalyzer.class);
				job.setMapperClass(AgeZipCode.Map.class);
				job.setReducerClass(AgeZipCode.Reduce.class);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				/*
				 * Start the second job once the first job completes
				 */
				if(job.waitForCompletion(true)) {
					Configuration conf1 = new Configuration();
					Job job1 = new Job(conf1, "topTenZipCode");
					job1.setOutputKeyClass(NullWritable.class);
					job1.setOutputValueClass(AgeAverageCountTuple.class);
					job1.setJarByClass(MovieAnalyzer.class);
					job1.setNumReduceTasks(1); 
					job1.setMapperClass(TopTenZipCode.Map.class);
					job1.setReducerClass(TopTenZipCode.Reduce.class);
					 
					job1.setInputFormatClass(TextInputFormat.class);
					job1.setOutputFormatClass(TextOutputFormat.class);

					FileInputFormat.addInputPath(job1, new Path(args[1]));
					FileOutputFormat.setOutputPath(job1, new Path(args[3]));
					job1.waitForCompletion(true);
				}
			}// End of TopTenZip job
		}
		else {
			System.out.println("Expecting four input arguments! <Input Path><Output Path><UserRating | MovieGenre><'n value' | 'Movie Titles separated by ::'> \n (or) <Input Path><Output Path 1> TopTenZip <Output Path 2>");
		}
	}

}
