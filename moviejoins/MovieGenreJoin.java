package moviejoins;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import moviejoins.custom.KeyPair;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieGenreJoin {
	public static class UserIdMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
		/*
		 * Emit only UserIds that are Male
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); 
			String tokens[] = line.split("::");
			if(tokens.length == 5) {
				if(tokens[0].matches("^\\d+$") && tokens[1].matches("[M]")) {
					//int tempUserId = Integer.parseInt(tokens[0]);
					//context.write(new IntWritable(1), new Text("A" + tokens[0]));
					//context.write(NullWritable.get(), new Text("A" + tokens[0]));
					context.write(new KeyPair(1, "A"), new Text(tokens[0]));
				}
			}
		} // End of Map Function
	} // End of UserIdMapper
	
	public static class MovieMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
		/*
		 * MovieId Key
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("::");
			if(tokens.length == 3) {
				if(tokens[0].matches("^\\d+$")) {
					//int tempMovieId = Integer.parseInt(tokens[0]);
					if(tokens[2].contains("Action") || tokens[2].contains("Drama")) {
						//context.write(new IntWritable(2), new Text("B" + line));
						//context.write(NullWritable.get(), new Text("B" + line));
						context.write(new KeyPair(1, "B"), value);
					}
				}
			}
		}
	} // End of Movie Mapper
	
	public static class RatingsMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
		/*
		 * UserId, MoveId will form the key
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("::");
			if(tokens.length == 4) {
				if(tokens[0].matches("^\\d+$") && tokens[1].matches("^\\d+$")) {
					//int tempUserId = Integer.parseInt(tokens[0]);
					//context.write(new IntWritable(3), new Text("C" + tokens[0] + "::" + tokens[1] + "::" + tokens[2]));
					//context.write(NullWritable.get(), new Text("C" + tokens[0] + "::" + tokens[1] + "::" + tokens[2]));
					context.write(new KeyPair(1, "C"), new Text(tokens[0] + "::" + tokens[1] + "::" + tokens[2]));
				}
			}
		}
	} // End of Ratings Mapper
	
	public static class MovieGenreReducer extends Reducer<KeyPair, Text, NullWritable, Text> {
		private Set<Integer> userIdList = new HashSet<Integer>(); 
		private HashMap<Integer, String> moviesList = new HashMap<Integer, String>(); 
		private HashMap<Integer, MovieRatingTuple> moviesRating = new HashMap<Integer, MovieRatingTuple>();
		
		
		public void reduce(KeyPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
			//userIdList.clear();
			//moviesList.clear(); 
			//moviesRating.clear(); 
			if(key.tag.equals("A")) {
				for(Text val: values) {
					userIdList.add(Integer.parseInt(val.toString()));
				}
			}
			else if(key.tag.equals("B")) {
				for(Text val: values) {
					// This moviesLiss will only contain the list of Action or Drama movies
					String tokens[] = val.toString().split("::");
					moviesList.put(Integer.parseInt(tokens[0]), tokens[1] + "," + tokens[2]);
				}
			}
			else if(key.tag.equals("C")) {
				for(Text val: values) {
					// Check if the userId and MovieId is present in userIdList and moviesList tuple
					String tokens[] = val.toString().split("::");
					boolean tempUserIdPresent = userIdList.contains(Integer.parseInt(tokens[0]));
					boolean tempMovieIdPresent = moviesList.containsKey(Integer.parseInt(tokens[1])); 
					int tempMovieId = Integer.parseInt(tokens[1]); 
					long tempMovieRating = Long.parseLong(tokens[2]); 
					if(tempUserIdPresent && tempMovieIdPresent) {
						boolean tempPresent = moviesRating.containsKey(tempMovieId);
						if(tempPresent) {
							MovieRatingTuple tuple = moviesRating.get(tempMovieId); 
							tuple.setRatings(tempMovieRating);
							moviesRating.put(tempMovieId, tuple); 
						}
						else {
							MovieRatingTuple tuple = new MovieRatingTuple(); 
							tuple.setRatings(tempMovieRating);
							moviesRating.put(tempMovieId, tuple); 
						}
					}
				}
			}
			
			if(!userIdList.isEmpty() && !moviesList.isEmpty() && !moviesRating.isEmpty()) {
				// Execute the logic here
				Iterator<Integer> itr = moviesRating.keySet().iterator();
				while(itr.hasNext()) {
					Integer hashKey = itr.next();
					MovieRatingTuple hashValue = moviesRating.get(hashKey);
					if(hashValue.getAverageRating() >=4.4 && hashValue.getAverageRating() <=4.7) {
						String finalOutput;
						finalOutput = hashKey + "\t" + moviesList.get(hashKey) + "\t" + hashValue.getAverageRating();
						context.write(NullWritable.get(), new Text(finalOutput));
					}
				}
			}
		} // End of Reduce Function
	} // End of MovieGenreReducer
}
