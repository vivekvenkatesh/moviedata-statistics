package moviejoins;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieJoinAnalyzer {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		if(args.length >= 5) {
			if(args[2].equals("RatingCount")) {
				Configuration conf = new Configuration();
				Job job = new Job(conf, "ratingsCount");

				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(IntWritable.class);
				job.setJarByClass(MovieJoinAnalyzer.class);
				job.setMapperClass(RatingsCount.RatingsCountMapper.class);
				job.setCombinerClass(RatingsCount.RatingsCountReducer.class);
				job.setReducerClass(RatingsCount.RatingsCountReducer.class);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				if(job.waitForCompletion(true)) {
					// Start the next mapper to perform the join and to get the top10 ratings count
					Configuration conf1 = new Configuration();
					Job job1 = new Job(conf1, "ratingsCountJoin");
					
					job1.setOutputKeyClass(NullWritable.class);
					job1.setOutputValueClass(UserTuple.class);
					job1.setJarByClass(MovieJoinAnalyzer.class);
					job1.setMapperClass(UserRatingJoin.UserRatingJoinMapper.class);
					job1.setNumReduceTasks(1);
					job1.setInputFormatClass(TextInputFormat.class);
					job1.setOutputFormatClass(TextOutputFormat.class);

					DistributedCache.addCacheFile(new Path(args[3]).toUri(), job1.getConfiguration());
					FileInputFormat.addInputPath(job1, new Path(args[1]));
					FileOutputFormat.setOutputPath(job1, new Path(args[4]));
					job1.waitForCompletion(true);
				}
			}
			else if(args[3].equals("MovieGenre")) {
				Configuration conf = new Configuration();
				Job job = new Job(conf, "movieGenre");

				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				job.setJarByClass(MovieJoinAnalyzer.class);
				
				
				MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieRatingJoin.UserIdMapper.class);
				
				MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MovieRatingJoin.RatingsMapper.class);
	
				job.setReducerClass(MovieRatingJoin.MovieRatingJoinReducer.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				FileOutputFormat.setOutputPath(job, new Path(args[4]));
				
				if(job.waitForCompletion(true)) {
					Configuration conf1 = new Configuration();
					Job job1 = new Job(conf1, "movieGenreChain");
					job1.setMapOutputKeyClass(IntWritable.class);
					job1.setMapOutputValueClass(Text.class);
					job1.setOutputKeyClass(NullWritable.class);
					job1.setOutputValueClass(Text.class);
					job1.setJarByClass(MovieJoinAnalyzer.class);
					
					MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MovieRatingJoin1.MovieMapper.class);
					MultipleInputs.addInputPath(job1, new Path(args[4]), TextInputFormat.class, MovieRatingJoin1.MovieRatingMapper.class);
				
					job1.setReducerClass(MovieRatingJoin1.MovieRatingJoin1Reducer.class);
					job1.setOutputFormatClass(TextOutputFormat.class);

					FileOutputFormat.setOutputPath(job1, new Path(args[5]));
					job1.waitForCompletion(true); 
				}

			}
		}
	} // End of Main

}
