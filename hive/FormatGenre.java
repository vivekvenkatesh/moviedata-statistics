/**
 * @author Vivek Venkatesh Ganesan
 * 
 * Hive UDF for Formatting Movie Genre
 */
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class FormatGenre extends UDF{
	public Text evaluate(Text input) {
		if(input == null || input.toString().length() == 0) {
			return null; 
		}
		try {
			String movieGenre = input.toString();
			String tokens[] = movieGenre.split("\\|"); 
			if(tokens.length > 0) {
				if(tokens.length == 1) {
					return new Text(tokens[0]); 
				}
				else {
					StringBuilder finalOutput = new StringBuilder();
					int tokensLength = tokens.length; 
					finalOutput.append(tokens[0]);
					for(int i=1;i<tokensLength-1;i++) {
						finalOutput.append(", ");
						finalOutput.append(tokens[i]); 
					}
					finalOutput.append(", and ");
					finalOutput.append(tokens[tokensLength-1]); 
					return new Text(finalOutput.toString()); 
				} // End of else part 
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
