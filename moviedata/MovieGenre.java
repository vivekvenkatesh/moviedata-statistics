package moviedata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieGenre {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text movieTitle;
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
	    			movieTitle = new Text(tempTitle);
		    		movieGenre = new Text(tempGenre);
		    		context.write(movieTitle, movieGenre);
		    		break;
	    		}
	    	}
	    	
	    }
	} // End of Map
	
	
}
