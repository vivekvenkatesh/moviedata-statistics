package moviejoins;

public class MovieRatingTuple {
	long ratingsSum;
	int ratingCount; 
	
	MovieRatingTuple() {
		ratingsSum = 0;
		ratingCount = 0;
	}
	
	public void setRatings(long val) {
		ratingsSum = ratingsSum + val; 
		ratingCount = ratingCount + 1; 
	}
	
	public double getAverageRating() {
		return ((double)ratingsSum / (double)ratingCount); 
	}
}
