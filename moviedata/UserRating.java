/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Description
 * -----------
 * Find the users who have rated at least 'n' movies
 * 
 */
package moviedata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRating {
	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		/* 
		 * The Key here is the UserId and the value is the number of the movies that the user has rated
		 */
		
		private int userId; 
		private int ratingCount;
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString(); 
	    	String tokens[] = line.split("::"); 
	    	if(tokens.length == 4) { // Make sure the record is valid
	    		/*
	    		 * Make sure that the user id, movie id and ratings are all numbers
	    		 */
	    		if(tokens[0].matches("^\\d+$") && tokens[1].matches("^\\d+$") && tokens[2].matches("^\\d+$")) {
	    			// Check if the tokens are valid positive numbers
	    			int tempUserId = Integer.parseInt(tokens[0]); 
	    			
	    				userId = tempUserId;
	    				ratingCount = 1;
	    				context.write(new IntWritable(userId), new IntWritable(ratingCount));
	    		}
	    	}
	    }

	} // End of Mapper
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		/*
		 * Simply count the number of times each user has rated for movies
		 * If the count is greater than n (specified as command line argument), then write the output
		 */
		
		int n = 30;
		public void reduce(IntWritable userId, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			n =  Integer.parseInt(conf.get("n"));
			
			int ratingCount = 0;
			for (IntWritable val : values) {
				ratingCount += val.get();
			}
			if(ratingCount >= n ) {
				context.write(userId, new IntWritable(ratingCount));
			}
		}
	}
}
