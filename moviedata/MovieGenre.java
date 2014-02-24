/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Description
 * -----------
 * Find the movie genres of the titles specified by the users
 * 
 */
package moviedata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieGenre {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	String tokens[] = line.split("::");
	    	/*
	    	 * Check if it is a valid record of format MovieID::MovieTitle::Genre
	    	 */
	    	if(tokens.length == 3) {
	    		String tempTitle = tokens[1];
	    		String tempGenre = tokens[2];
	    		if(tempTitle.length() != 0 && tempGenre.length() != 0) {
	    			Configuration conf = context.getConfiguration();
	    			String userTitles[] = conf.get("title").split("::");

	    			for(int i = 0; i<userTitles.length ; i++) {
	    				//if(userTitles[i].equalsIgnoreCase(tempTitle)) {
	    				// if(tempTitle.contains(userTitles[i])) {
	    				/*
	    				 * Ignore the case entered by the user
	    				 */
	    				if(tempTitle.toUpperCase().contains(userTitles[i].toUpperCase())) {
		    				String[] tempList = tempGenre.split("\\|");
		    				for(int j = 0; j < tempList.length; j++) {
		    					context.write(new Text(tempList[j]), new IntWritable(0));
		    				}
		    				
	    					/*
	    					 * Come out once you have found the title's Genre
	    					 */
	    					break;
	    				}
	    			}
	    		}
	    	}
	    	
	    }
	} // End of Map
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> { 
		 
		 
		public void reduce(Text movieGenre, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
				
				context.write(movieGenre, NullWritable.get());
		}
	} // End of Reduce
	
	
}
