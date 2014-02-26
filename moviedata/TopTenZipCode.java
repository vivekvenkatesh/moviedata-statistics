/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Description
 * -----------
 * Second Map reduce job in the chaining to find the Top Ten Zipcodes
 * 
 */
package moviedata;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenZipCode {
	public static class Map extends Mapper<LongWritable, Text, NullWritable, AgeAverageCountTuple> {
		private AgeAverageCountTuple age = new AgeAverageCountTuple();
		private TreeSet<AgeAverageCountTuple> top10Zip = new TreeSet<AgeAverageCountTuple>(); 
		
		/**
		 * Function to convert the text from mapper 1 to AgeAverageCountTuple
		 * @param word String value of Text coming from mapper 1
		 * @return Equivalent AgeAverageCountTuple
		 */
		public AgeAverageCountTuple transformStringToTuple(String word) {
			AgeAverageCountTuple output = new AgeAverageCountTuple();
			String[] tokens = word.split(",");
			output.setAge(Long.parseLong(tokens[0]));
			output.setCount(Long.parseLong(tokens[1]));
			output.setAverage(Double.parseDouble(tokens[2]));
			output.setZipCode(new Text(tokens[3]));
			return output;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			age = transformStringToTuple(line); 
			top10Zip.add(age); 
			if(top10Zip.size() > 10) {
				top10Zip.remove(top10Zip.last());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			/*
			 * Write the Top10 once at the end of each map task
			 *  
			 */
			for(AgeAverageCountTuple i : top10Zip) {
				context.write(NullWritable.get(), i);
			}
		}
	} // End of Mapper Class

	public static class Reduce extends Reducer<NullWritable, AgeAverageCountTuple, NullWritable, AgeAverageCountTuple> {

		TreeSet<AgeAverageCountTuple> top10 = new TreeSet<AgeAverageCountTuple>();
		
		public AgeAverageCountTuple transformStringToTuple(String word) {
			AgeAverageCountTuple output = new AgeAverageCountTuple();
			String[] tokens = word.split(",");
			output.setAge(Long.parseLong(tokens[0]));
			output.setCount(Long.parseLong(tokens[1]));
			output.setAverage(Double.parseDouble(tokens[2]));
			output.setZipCode(new Text(tokens[3]));
			output.setPrintFullTuple(false);
			return output;
		}
		/*
		 * Compute the top10 from the output of all mappers
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(NullWritable key, Iterable<AgeAverageCountTuple> values, Context context) 
				throws IOException, InterruptedException {
			// Get the top 10 youngest users zipcodes
			Iterator<AgeAverageCountTuple> itrr = values.iterator(); 
			 
			while(itrr.hasNext())  {
				AgeAverageCountTuple itr = itrr.next(); 
				Text temp = new Text(itr.toString());
				AgeAverageCountTuple temp1 = transformStringToTuple(temp.toString());
						
				top10.add(temp1);
				if(top10.size() > 10) {
					top10.remove(top10.last());
				}
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			/*
			 * Write the output in descending order
			 */

			for(AgeAverageCountTuple i : top10.descendingSet()) {
				context.write(NullWritable.get(), i);
			}
		}

	}
}// End of Reducer