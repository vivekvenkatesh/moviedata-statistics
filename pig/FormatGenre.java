/*
 * @author Vivek Venkatesh Ganesan
 * 
 * Pig UDF to Format Movie Genre
 * 
 */
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


public class FormatGenre extends EvalFunc <String>{

	@Override
	public String exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		try {
			if(input == null || input.size() == 0) {
				return null; 
			}
			
			String movieGenre = (String) input.get(0);
				String tokens[] = movieGenre.split("\\|"); 
				if(tokens.length > 0) {
					if(tokens.length == 1) {
						return tokens[0]; 
					}
					else {
						String lastVal = tokens[tokens.length - 1] ; 
						StringBuilder finalOutput = new StringBuilder(lastVal);
						int tokensLength = tokens.length; 
						for(int i=0;i<tokensLength-2;i++) {
							if(!tokens[i].equals(lastVal)) {
								finalOutput.append(", ");
								finalOutput.append(tokens[i]); 
							}
						}
						finalOutput.append(" and ");
						finalOutput.append(tokens[tokensLength-2]); 
						return finalOutput.toString(); 
					} // End of else part 
				}
		} catch (ExecException ex) {
			ex.printStackTrace(); 
		}
		return null;
	}

}
