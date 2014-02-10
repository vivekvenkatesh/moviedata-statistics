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
	    	String tempTitle = tokens[1];
	    	String tempGenre = tokens[2];
	    	Configuration conf = context.getConfiguration();
	    	String userTitles[] = conf.get("title").split(",");
	    	
	    	for(int i = 0; i<userTitles.length ; i++) {
	    		if(userTitles[i].equalsIgnoreCase(tempTitle)) {
	    			//movieTitle = new Text(tempTitle);
		    		movieTitle = NullWritable.get(); 
	    			movieGenre = new Text(tempGenre);
		    		context.write(movieTitle, movieGenre);
		    		break;
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
