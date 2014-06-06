/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Join User and Ratings File (Reduce Side Join)
 * 
 */
package moviejoins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingJoin {
	/*
	 * User Mapper will emit UserId as key and "U" + UserId as value
	 * Ratings Mapper will emit UserId as key and "R" + UserId, MovieId, Rating as value
	 * 
	 * Final Output will contain the list of movies rated by each male user and their ratings
	 */
	
	public static class UserIdMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/*
		 * Emit only UserIds that are Male
		 * Output Key: UserId 
		 * Output Value: U + UserId
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); 
			String tokens[] = line.split("::");
			if(tokens.length == 5) {
				if(tokens[0].matches("^\\d+$") && tokens[1].matches("[M]")) {
					int tempUserId = Integer.parseInt(tokens[0]);
					context.write(new IntWritable(tempUserId), new Text("U" + tokens[0]));
				}
			}
		} // End of Map Function
	} // End of UserIdMapper
	
	public static class RatingsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/*
		 * Output Key: UserId
		 * Output Value: R + UserId + MovieId + Rating
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("::");
			if(tokens.length == 4) {
				if(tokens[0].matches("^\\d+$") && tokens[1].matches("^\\d+$") && tokens[2].matches("^\\d+$")) {
					int tempUserId = Integer.parseInt(tokens[0]);
					context.write(new IntWritable(tempUserId), new Text("R" + tokens[0] + "::" + tokens[1] + "::" + tokens[2]));
				}
			}
		}
	} // End of Ratings Mapper
	
	public static class MovieRatingJoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		/*
		 * Input Key : IntWritable (UserId) 
		 * Output Key : IntWritable (MovieId)
		 * Output Value will contain MovieId (Key) and Rating as output value 
		 */
		Set<IntWritable> userTuple = new HashSet<IntWritable>(); 
		ArrayList<Text> ratingTuple = new ArrayList<Text>(); 
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
			userTuple.clear();
			ratingTuple.clear(); 
			for(Text val : values) {
				if(val.charAt(0) == 'U') {
					userTuple.add(key);
				}
				else if(val.charAt(0) == 'R') {
					ratingTuple.add(new Text(val.toString().substring(1))); 
				}
			}
			executeJoinLogic(context);
		} // End of Reduce Function
		
		private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			/*
			 * If both the arrayLists are not empty (inner join) 
			 */
			if(!userTuple.isEmpty() && !ratingTuple.isEmpty()) {
				for(Text R: ratingTuple) {
					String tokens[] = R.toString().split("::");
					if(tokens.length == 3) {
						context.write(new IntWritable(Integer.parseInt(tokens[1])), new Text(tokens[2]));
					}
				}
			}
		} // End of executeJoinLogic
	}
}
