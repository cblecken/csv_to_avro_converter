package cblecken;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleCSVParser {

	private static final int START_STATE = 1;	// start
	private static final int WITHIN_FIELD_STATE = 2;	// in field
	private static final int QUOTED_FIELD_STATE = 3;	// in quoted field
	private static final int LAST_CHAR_QUOTE_STATE = 4;	// last character was a quote
	private static final int END_OF_LINE_STATE = 0;	// end state, got full line
	
	File csvFile = null;

	BufferedReader buffReader = null;
	int lineNum = 0;
	String lineAsStr = null;
	Charset charset = null;
	
	public SimpleCSVParser(String filename, Map<String, List<String>> options) {
		// create the parser and open the file
		csvFile = new File(filename);
		
		List<String> columnNames = null;
		if (options!= null && options.containsKey("schema")) {
			columnNames = (List<String>) options.get("schema");
		}
		
		if (options!=null && options.containsKey("charSet")) {
			String set = options.get("charSet").get(0);
			if (set.equals("UTF-8"))
				charset = StandardCharsets.UTF_8;
		}
		if (charset==null)
			charset = Charset.defaultCharset();
		

	}
	
	public void init() throws Exception {
		try {
			openFile(charset);
		} catch (Exception e) {
			System.out.println("Exception " + e.toString());
			e.printStackTrace();
		}
	}
	
	private void openFile(Charset charSet) throws Exception {
		FileInputStream fs = new FileInputStream(csvFile) ;
		InputStreamReader isr ;
		try {
			CharsetDecoder decoder = Charset.forName(charSet.name()).
						 newDecoder() ;
			decoder.onMalformedInput(CodingErrorAction.REPORT) ;
			decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
			isr = new InputStreamReader((InputStream)fs, decoder) ;
		} catch (IllegalCharsetNameException e) {
			throw e;
		} catch (UnsupportedCharsetException e) {
			throw e;
		}
		buffReader = new BufferedReader(isr) ;
		lineNum = 1 ;
	}

	// limited by quotes and double quotes
	public String[] readRecord() throws Exception {
	    ArrayList<String> fields = new ArrayList<String>();
	    int state;
	    char c;
	    String nextLineAsStr = null ;		

	    if (buffReader == null) {
	    	return null ;
	    }
	    lineAsStr = null ;
	    try {
	    	lineAsStr = buffReader.readLine();
	    } catch (IOException e) {
		try { buffReader.close() ; } catch (Exception f) { ; }
			throw new Exception("IO Error: "+ e.getMessage() + 	 lineNum) ;
	    }
	    if (lineAsStr == null) {
	    	return null ;
	    }
	    lineNum++ ;

	    state = START_STATE;
	    int ind = 0, fieldStart = 0;
	    while (state != END_OF_LINE_STATE) {
			if (ind >= lineAsStr.length()) { // evaluate after buffer is being read : only options are 3 below
			    if (state == QUOTED_FIELD_STATE) {
					nextLineAsStr = readNewLine(nextLineAsStr);
					lineNum++;
					lineAsStr = lineAsStr + "\n" + nextLineAsStr ;
					ind++ ;	// step over \n
			    } else if (state == LAST_CHAR_QUOTE_STATE) { 
					fields.add(lineAsStr.substring(fieldStart,ind-1).replaceAll("\"\"","\""));
					state = END_OF_LINE_STATE ;
			    } else {
					fields.add(lineAsStr.substring(fieldStart,ind));
					state = END_OF_LINE_STATE;
			    }
			    continue ;
			}
			c = lineAsStr.charAt(ind) ;

			// normal flow
			if (state == START_STATE) {
			    if (c == '"') {
					state = QUOTED_FIELD_STATE;
					fieldStart = ind+1;
			    } else if (c == ',') {
					fields.add("");
					fieldStart = ind+1 ;
			    } else {
			    	state = WITHIN_FIELD_STATE;
			    }
			} else if (state == WITHIN_FIELD_STATE) {
			    if (c == ',') {
					fields.add(lineAsStr.substring(fieldStart,ind));
					state = START_STATE;
					fieldStart = ind+1;
			    }
			} else if (state == QUOTED_FIELD_STATE) {
			    if (c == '"') {
				state = LAST_CHAR_QUOTE_STATE;
			    }
			} else if (state == LAST_CHAR_QUOTE_STATE) {
			    if (c == ',') {
					fields.add(lineAsStr.substring(fieldStart, ind-1)
						       .replaceAll("\"\"","\""));
					state = START_STATE;
					fieldStart = ind+1;
			    } else if (c != '"') { 			   
			    	throw new Exception("Bad Quote in Quoted Field." + lineNum);
			    } else {
			    	state = QUOTED_FIELD_STATE;
			    }
			}
			ind = ind + 1;
		}
	    String[] ret = new String[fields.size()];
	    return (String[])fields.toArray(ret);
	}

	private String readNewLine(String nextLineAsStr) throws Exception {
		try {
		    nextLineAsStr = buffReader.readLine();  // read next line if necessary at end of line
		} catch (IOException e) {
		    try { buffReader.close() ; } catch (Exception f) { ; }
		    throw new Exception("IO Error: "+ e.getMessage() + 	 lineNum) ;
		}
		if (nextLineAsStr == null) {
		    throw new Exception("EOF in Quoted Field" +lineNum);
		}
		return nextLineAsStr;
	}

}
