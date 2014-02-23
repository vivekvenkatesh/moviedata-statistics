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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieGenre {
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
		private NullWritable movieTitle;
		private Text movieGenre;
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	    	String tokens[] = line.split("::");
	    	/*
	    	 * Check if it is a valid record of format MovieID::MovieName::Genre
	    	 */
	    	if(tokens.length == 3) {
	    		String tempTitle = tokens[1];
	    		String tempGenre = tokens[2];
	    		Configuration conf = context.getConfiguration();
	    		String userTitles[] = conf.get("title").split("::");

	    		for(int i = 0; i<userTitles.length ; i++) {
	    			//if(userTitles[i].equalsIgnoreCase(tempTitle)) {
	    			if(tempTitle.contains(userTitles[i])) {
	    				movieTitle = NullWritable.get(); 
	    				movieGenre = new Text(tempGenre);
	    				context.write(movieTitle, movieGenre);
	    				/*
	    				 * Come out once you have found the title's Genre
	    				 */
	    				break;
	    			}
	    		}
	    	}
	    	
	    }
	} // End of Map
	
	public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> { 
		 
		Set<Text> genreList = new HashSet<Text>(); 
		public void reduce(NullWritable movieTitle, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			for (Text val: values) {
				String[] tempList = val.toString().split("\\|");
				for(int i = 0; i < tempList.length; i++) {
					genreList.add(new Text(tempList[i]));
				}
			}
			for (Text genre : genreList) {
				context.write(NullWritable.get(), genre);
			}
			genreList.clear();
		}
	} // End of Reduce
	
	
}
