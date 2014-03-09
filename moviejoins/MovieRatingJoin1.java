package moviejoins;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingJoin1 {
	public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/*
		 * MovieId Key
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("::");
			if(tokens.length == 3) {
				if(tokens[0].matches("^\\d+$")) {
					int tempMovieId = Integer.parseInt(tokens[0]);
					if(tokens[2].contains("Action") || tokens[2].contains("Drama")) {
						context.write(new IntWritable(tempMovieId), new Text("M" + line));
					}
				}
			}
		}
	} // End of MovieMapper
	
	public static class MovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("\t");
			if(tokens.length == 2) {
				if(tokens[0].matches("^\\d+$")){
					int tempMovieId = Integer.parseInt(tokens[0]);
					context.write(new IntWritable(tempMovieId), new Text("R" + tokens[1]));
				}
			}
		}
		
	} // End of MovieRatingMapper
	
	public static class MovieRatingJoin1Reducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		ArrayList<Text> movieTuple = new ArrayList<Text>();
		long ratingSum = 0;
		long ratingCount = 0;
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
			movieTuple.clear(); 
			ratingSum = 0;
			ratingCount = 0; 
			for(Text val : values) {
				if(val.charAt(0) == 'M') {
					movieTuple.add(new Text(val.toString().substring(1)));
				}
				else if(val.charAt(0) == 'R') {
					String tempTuple = val.toString().substring(1); 
					/*
					 * TODO: Handle Long Parsing exception
					 */
					ratingSum = ratingSum + Long.parseLong(tempTuple); 
					ratingCount = ratingCount + 1; 
				}
			}
			executeJoinLogic(context);
			
		}// End of Reduce Function
		
		private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if(!movieTuple.isEmpty() && ratingSum!=0 && ratingCount!=0) {
				for(Text M : movieTuple) {
					double averageRating = (double)ratingSum / (double)ratingCount; 
					/*
					 * Write if the average rating is >=4.4 and <=4.7
					 */
					if(averageRating >= 4.4 && averageRating <= 4.7) {
						String line = M.toString();
						String tokens[] = line.split("::"); 
						/*
						 * TODO: Check why this is working
						 */
						//if(tokens.length == 3) {
						context.write(NullWritable.get(), new Text(tokens[0] + "\t" + tokens[1] + "\t" + tokens[2] + "\t" + averageRating));
						//}
					}
				}
			}
		} // End of executeJoinLogicFunction
	}
}
