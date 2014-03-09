/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Custom comparator for sorting the mapper output values that go to the reducer
 * 
 */
package moviejoins.custom;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomComparator extends WritableComparator{

	protected CustomComparator() {
		super(KeyPair.class, true);
		// TODO Auto-generated constructor stub
	}
	@SuppressWarnings("rawtypes")
	public int compare( WritableComparable a, WritableComparable b) {
		KeyPair one = (KeyPair) a;
		KeyPair two = (KeyPair) b;
		return one.compareTo(two);
	}
}
