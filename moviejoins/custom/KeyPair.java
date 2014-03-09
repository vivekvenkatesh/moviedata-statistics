/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Custom Key for Reduce Join Mapper
 * 
 */
package moviejoins.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements WritableComparable<KeyPair> {
	public Integer key;
	public String tag;
	
	public KeyPair() {
		key = new Integer(0);
		tag = "A"; 
	}
	public KeyPair(int key, String tag) {
		this.key = new Integer(key);
		this.tag = tag;
	}
	
	public int getHashCode() {
		return this.key.hashCode(); 
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.key = in.readInt();
		this.tag = in.readUTF(); 
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.key);
		out.writeUTF(this.tag); 
		
	}
	@Override
	public int compareTo(KeyPair sp) {
		// TODO Auto-generated method stub
		int cmp = this.tag.compareTo(sp.tag);
		return cmp; 
	}
	
	public boolean equals(KeyPair sp) {
		if(this.key == sp.key && this.tag.equals(sp.tag)) {
			return true;
		}
		else {
			return false; 
		}
	}
	@Override
	public String toString() {
		return "KeyPair [key=" + key + ", tag=" + tag + "]";
	}
	
}
