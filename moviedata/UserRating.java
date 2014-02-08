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
	    	int tempUserId = Integer.parseInt(tokens[0]); 
	    	int tempMovieId = Integer.parseInt(tokens[1]);
	    	int tempRating = Integer.parseInt(tokens[2]); 
	    	
	    	/*
	    	 * Check if the record is a valid record
	    	 * User Ids are in the range 1 and 6040
	    	 * Movie Ids are in the range 1 and 3952
	    	 * Ratings are on a 5 star scale 
	    	 */
	    	if((tempUserId >=1 && tempUserId <=6040) && (tempMovieId >= 1 && tempMovieId <=3952) && (tempRating >= 0 && tempRating <=5)) {
	    		userId = tempUserId;
	    		ratingCount = 1;
	    		context.write(new IntWritable(userId), new IntWritable(ratingCount));
	    	}
	    }

	} // End of Mapper
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		/*
		 * Simply count the number of times each user has rated for movies
		 * If the count is greater than 30, then write the output
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
