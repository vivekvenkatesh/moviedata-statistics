/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Compute the total ratings of each user
 * 
 */
package moviejoins;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingsCount {
	
	/*
	 * Count the number of movies each user has rated and emit them (read the ratings.dat) 
	 * 
	 */
	public static class RatingsCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		/*
		 * Input Key - LongWritable
		 * Input Value - Text
		 * Output Key - IntWritable(UserId)
		 * Output Value - IntWritable (1) 
		 * 
		 */
		private IntWritable userIdOutput;
		private IntWritable countOutput; 
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("::");
			// UserId, MovieId, Rating, TimeStamp
			if(tokens.length == 4) {
				/*
				 * Check if the userId and the ratings are ratings are a valid number
				 */
				if(tokens[0].matches("^\\d+$")  && tokens[2].matches("^\\d+$")) {
					userIdOutput = new IntWritable(Integer.parseInt(tokens[0]));
					countOutput = new IntWritable(1); 
					context.write(userIdOutput, countOutput);
				}
			}
		} 
	} // End of Mapper
	
	public static class RatingsCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		/*
		 * Input Key - UserId
		 * Input Value - (Ratings Count for the User) 
		 * Output Key - UserId
		 * Output Key - Total number of movies rated by the User
		 * 
		 */
		public void reduce(IntWritable userId, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int ratingCount = 0;
			for (IntWritable val : values) {
				ratingCount += val.get();
			}
			context.write(userId, new IntWritable(ratingCount));
		}
	} // End of Reducer
}
