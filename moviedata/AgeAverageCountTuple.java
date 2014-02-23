/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Description
 * -----------
 * Custom Writable Class 'AgeAverageCountTuple' for Average Age Count
 */
package moviedata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AgeAverageCountTuple implements WritableComparable<AgeAverageCountTuple> {
	private long age;
	private long count;
	private double averageAge;
	private Text zipcode; 
	private boolean printFullTuple; 
	
	public AgeAverageCountTuple() {
		setAge(0);
		setCount(0);
		setAverage(0.0);
		setZipCode(new Text(""));
		setPrintFullTuple(true);
	}
	
	public long getAge() {
		return age;
	}
	
	public void setAge(long ageArg) {
		this.age = ageArg;
	}
	
	public long getCount() {
		return count;
	}
	public void setCount(long countArg) {
		this.count = countArg;
	}
	
	public void setAverage(double avg) {
		this.averageAge = avg; 
	}
	
	public double getAverage() {
		return averageAge; 
	}
	
	public void setZipCode(Text zipcodeArg) {
		this.zipcode = zipcodeArg; 
	}
	
	public Text getZipCode() {
		return zipcode; 
	}
	
	public void setPrintFullTuple(boolean val) {
		this.printFullTuple = val; 
	}
	
	public boolean getPrintFullTupleVal() {
		return printFullTuple; 
	}
	
	/*public String finalOutput() {
		String temp = zipcode + "," + averageAge; 
		return temp;
	}*/
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		age = in.readLong();
		count = in.readLong();
		averageAge = in.readDouble();
		zipcode.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(age);
		out.writeLong(count);
		out.writeDouble(averageAge);
		zipcode.write(out);
	}
	
	public String toString() {
		if(getPrintFullTupleVal())
			return (Long.toString(age) + "," + Long.toString(count) + "," + Double.toString(averageAge) + "," + zipcode.toString());
		else
			return (Double.toString(averageAge) + "," + zipcode.toString());
		
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (age ^ (age >>> 32));
		long temp;
		temp = Double.doubleToLongBits(averageAge);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result + ((zipcode == null) ? 0 : zipcode.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AgeAverageCountTuple other = (AgeAverageCountTuple) obj;
		if (age != other.age)
			return false;
		if (Double.doubleToLongBits(averageAge) != Double
				.doubleToLongBits(other.averageAge))
			return false;
		if (count != other.count)
			return false;
		if (zipcode == null) {
			if (other.zipcode != null)
				return false;
		} else if (!zipcode.equals(other.zipcode))
			return false;
		return true;
	}
	

	@Override
	public int compareTo(AgeAverageCountTuple sp) {

		// TODO Auto-generated method stub
		// Compare based on the average age
		int cmp = Double.compare(this.averageAge, sp.averageAge);
		if(cmp == 0) {
			// If the average age is same compare based on the zipcode 
			cmp = this.zipcode.compareTo(sp.zipcode);
		}
		return cmp; 
	}
	
}
