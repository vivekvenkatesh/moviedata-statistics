/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Custom Writable UserTuple
 * 
 */
package moviejoins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserTuple implements WritableComparable<UserTuple>{

	int userId;
	double age; 
	char gender;
	int ratingsCount; 
	
	public UserTuple() {
		userId = 0;
		age = 0.0;
		gender = 'M';
		ratingsCount = 0;
	}
	
	public UserTuple(int uid, double age, char gender) {
		this.userId = uid;
		this.age = age;
		this.gender = gender; 
		this.ratingsCount = 0; 
	}
	
	public int getUserId() {
		return this.userId; 
	}
	
	public double getUserAge() {
		return this.age;
	}
	
	public char getUserGender() {
		return this.gender; 
	}
	
	public int getUserRatingsCount() {
		return this.ratingsCount; 
	}
	
	public void setUserId(int uid) {
		this.userId = uid;
	}
	
	public void setUserAge(int age) {
		this.age = age;
	}
	
	public void setUserGender(char gender) {
		this.gender = gender; 
	}
	
	public void incrementRatingCount(int factor) {
		this.ratingsCount = this.ratingsCount + factor; 
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		userId = in.readInt(); 
		age = in.readDouble();
		gender = in.readChar();
		ratingsCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(userId);
		out.writeDouble(age);
		out.writeChar(gender);
		out.writeInt(ratingsCount);
	}

	@Override
	public int compareTo(UserTuple sp) {
		// TODO Auto-generated method stub
		/*
		 * Here top 10 users who have rated most number of movies
		 * Important
		 * =========
		 * By default,  int cmp = ratingsCount > sp.ratingsCount ? 1 : (ratingsCount < sp.ratingsCount) ? -1 : 0; will sort in ascending order of total counts
		 * So change it to int cmp = ratingsCount > sp.ratingsCount ? -1 : (ratingsCount < sp.ratingsCount) ? 1 : 0;
		 */
		int cmp = ratingsCount > sp.ratingsCount ? -1 : (ratingsCount < sp.ratingsCount) ? 1 : 0;
		if(cmp == 0) {
			cmp = userId > sp.userId ? 1 : (userId < sp.userId) ? -1 : 0; 
		}
		return cmp; 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + userId;
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
		UserTuple other = (UserTuple) obj;
		if (userId != other.userId)
			return false;
		return true;
	}
	
	public String toString() {
		return (userId + "\t" + age + "\t" + gender + "\t" + ratingsCount);
	}

}
