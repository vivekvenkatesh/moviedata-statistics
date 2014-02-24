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
import org.apache.hadoop.io.NullWritable;
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
			/*
			 * Read Each line from the input file and write
			 * Key is the zipcode and value is the Custom Writable AgeAverageCountTuple
			 * Make sure that the tokens length is 5
			 */
			if(tokens.length == 5) {
				if((tokens[4].length() !=0) && (tokens[2].length() != 0)) {
					zipcode = new Text(tokens[4]);
					age.setAge(Long.parseLong(tokens[2]));
					age.setCount(1);
					age.setAverage(((double)age.getAge()) / ((double)age.getCount()));
					age.setZipCode(zipcode);
					context.write(zipcode, age);
				}
			}
			
		}
		
	} // End of Map
	
	public static class Reduce extends Reducer<Text, AgeAverageCountTuple, NullWritable, AgeAverageCountTuple> { 
		
		public void reduce(Text zipcode, Iterable<AgeAverageCountTuple> values, Context context) 
				throws IOException, InterruptedException {
			AgeAverageCountTuple averageAgeZipCode = new AgeAverageCountTuple();
			for(AgeAverageCountTuple val : values) {
				averageAgeZipCode.setAge(averageAgeZipCode.getAge() + val.getAge());
				averageAgeZipCode.setCount(averageAgeZipCode.getCount() + val.getCount());
			}
			/*
			 * Find the average age of each zipcode
			 * Output Key is a NullWriable
			 * Output Value is the Custom Writable AgeAverageCountTuple (which contains the details of each zipcode) 
			 */
			averageAgeZipCode.setAverage(((double)averageAgeZipCode.getAge()) / ((double)averageAgeZipCode.getCount()));
			averageAgeZipCode.setZipCode(zipcode);
			context.write(NullWritable.get(), averageAgeZipCode);
		}
	} // End of Reducer
}
