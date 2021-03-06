/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Join the users and ratings file (Map Side Join) 
 * 
 */
package moviejoins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingJoin {
	/*
	 * Replicated Join
	 * ---------------
	 * This is an inner join as the users present in both ratings table and users table only has to be displayed
	 * Large DataSet : ratings.dat
	 * Smaller DataSet : users.dat
	 * 
	 * Load the smaller dataset into the distributed cache and give the larger dataset as input to the mapper
	 */
	public static class UserRatingJoinMapper extends Mapper<LongWritable, Text, NullWritable, UserTuple> {
		/*
		 * Use a hashmap to store the values of that will be read in the distributed cache (of that of the smaller dataset)
		 * Key will the UserId and the Value will contain the age and gender fields
		 * 
		 * UserId will be used as a foreign key for the Join to happen
		 */
		private HashMap<Integer, UserTuple> userInfo = new HashMap<Integer, UserTuple>();
		private TreeSet<UserTuple> top10UserRatings = new TreeSet<UserTuple>(); 
		
		public void setup(Context context) throws IOException, InterruptedException {
			/*
			 * Multiple small files can be read from the distributed cache
			 */
			Path[] filesToRead = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			// Read the files present in the distributed cache
			for(Path p : filesToRead) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(p.toString()))));
				String line = null;
				
				while((line = rdr.readLine()) != null) {
					String tokens[] = line.split("::");
					if(tokens.length == 5) {
						/*
						 * Check if it is a valid record
						 */
						if(tokens[0].matches("^\\d+$") && tokens[1].matches("[MF]") && tokens[2].matches("^\\d+$")) {
							int tempUID = Integer.parseInt(tokens[0]);
							UserTuple tempRecord = new UserTuple(tempUID, Double.parseDouble(tokens[2]), tokens[1].charAt(0)); 
							userInfo.put(new Integer(tempUID), tempRecord);
						}
					}
				}
				rdr.close(); 
			}
		} // End of Setup
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("\t");
			if(tokens.length == 2) {
				int tempUserId = Integer.parseInt(tokens[0]);
				int tempRatingsCount = Integer.parseInt(tokens[1]);
				UserTuple tempUserTuple = userInfo.get(tempUserId); 
				/*
				 * Check if the UserId exists in the UserInfo Table (Like InnerJoin. Has to be present in both tables)  
				 */
				if(tempUserTuple != null) {
					UserTuple finalRecord = new UserTuple(tempUserId, tempUserTuple.getUserAge(), tempUserTuple.getUserGender());
					finalRecord.incrementRatingCount(tempRatingsCount); 
					top10UserRatings.add(finalRecord);
					if(top10UserRatings.size() > 10) {
						/*
						 * CompareTo method in UserTuple has been modified to handle the following logic
						 */
						top10UserRatings.remove(top10UserRatings.last());
					}
				}
			}
		} // End of Map Function
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(UserTuple i: top10UserRatings) {
				context.write(NullWritable.get(), i);
			}
		}
		
	} // End of Mapper
	
	public static class UserRatingJoinReducer extends Reducer<NullWritable, UserTuple, NullWritable, UserTuple> {
		/*
		 * This reducer is needed because, if more than 1 map task is created, 
		 * We will have multiple top10s.
		 */
		private TreeSet<UserTuple> top10UserRatings = new TreeSet<UserTuple>();
		
		public void reduce(NullWritable key, Iterable<UserTuple> values, Context context)
				throws IOException, InterruptedException {
			for(UserTuple val: values) {
				UserTuple temp = new UserTuple(val.getUserId(),val.getUserAge(),val.getUserGender());
				temp.incrementRatingCount(val.getUserRatingsCount());
				top10UserRatings.add(temp); 
				if(top10UserRatings.size() > 10) {
					/*
					 * CompareTo method in UserTuple has been modified to handle the following logic
					 */
					top10UserRatings.remove(top10UserRatings.last());
				}
			}
		} // End of Reduce function
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(UserTuple i: top10UserRatings) {
				context.write(NullWritable.get(), i);
			}
		}
	} // End of Reducer
}
