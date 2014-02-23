/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Description
 * -----------
 * First Map Reduce Job in Chaining to find the Top Ten Zipcodes. 
 */
package moviedata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AgeZipCode {
	public static class Map extends Mapper<LongWritable, Text, Text, AgeAverageCountTuple> {
		Text zipcode; // Output key
		AgeAverageCountTuple age = new AgeAverageCountTuple(); // Output value
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("::");
			if((!tokens[4].equals("")) && (!tokens[2].equals(""))) {
				zipcode = new Text(tokens[4]);
				age.setAge(Long.parseLong(tokens[2]));
				age.setCount(1);
				age.setAverage(((double)age.getAge()) / ((double)age.getCount()));
				age.setZipCode(zipcode);
				context.write(zipcode, age);
			}
			
		}
		
	} // End of Map
	
	public static class Reduce extends Reducer<Text, AgeAverageCountTuple, Text, AgeAverageCountTuple> { 
		
		public void reduce(Text zipcode, Iterable<AgeAverageCountTuple> values, Context context) 
				throws IOException, InterruptedException {
			AgeAverageCountTuple averageAgeZipCode = new AgeAverageCountTuple();
			for(AgeAverageCountTuple val : values) {
				averageAgeZipCode.setAge(averageAgeZipCode.getAge() + val.getAge());
				averageAgeZipCode.setCount(averageAgeZipCode.getCount() + val.getCount());
			}
			averageAgeZipCode.setAverage(((double)averageAgeZipCode.getAge()) / ((double)averageAgeZipCode.getCount()));
			averageAgeZipCode.setZipCode(zipcode);
			context.write(zipcode, averageAgeZipCode);
		}
	} // End of Reducer
}
