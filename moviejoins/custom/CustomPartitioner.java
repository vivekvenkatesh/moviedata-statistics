/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Custom partitioner for customizing the reducer to which the key goes
 * 
 */
package moviejoins.custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<KeyPair, Text>{

	@Override
	public int getPartition(KeyPair key, Text value, int numPartitions) {
		
		return (key.getHashCode() % numPartitions);
	}

}
