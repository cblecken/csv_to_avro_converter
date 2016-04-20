package cblecken;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class Converter {
	
	public static String lineSep = System.getProperty("line.separator");

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Converter conv = new Converter();
		try {
			conv.readFile(args[0], args[1]);
		} catch (Exception e) {
			System.out.println("ERROR while converting " + e.toString());
			e.printStackTrace();
		}
	}

	private void readFile(String filePath, String filePathOut) throws Exception {
		SimpleCSVParser parser = new SimpleCSVParser(filePath, null);
		
		
		parser.init();
		
		long start = System.currentTimeMillis();
		
		String[] headerRow = parser.readRecord(); // initial row
		
		String fileName = getFileName(filePath);
		String jsonSchema = emitHeaderRowIntoJson(headerRow, fileName);
		Schema schema = new Schema.Parser().parse(jsonSchema);
		
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.fromString("bzip2"));
		
		dataFileWriter.create(schema, new File(filePathOut));
		
		String[] row = null;
		while ((row = parser.readRecord())!=null) {
			// go on
			GenericRecord crec = new GenericData.Record(schema);
			for (int in = 0; in < headerRow.length; in++) {
				crec.put(headerRow[in], row[in]);
			}
			dataFileWriter.append(crec);
		}
		dataFileWriter.close();
		System.out.println("Time :" + (System.currentTimeMillis() - start));
	}

	private String getFileName(String filePath) {
		int ind = 0;
		if ((ind = filePath.lastIndexOf("/")) > -1) {
			if (filePath.length()>ind) {
				String fileNameWithEnding = filePath.substring(ind+1);
				int dotind = -1;
				if ((dotind = fileNameWithEnding.indexOf(".")) > 0) {
					return fileNameWithEnding.substring(0, dotind);
				} else {
					return fileNameWithEnding;
				}
			} else {
				return "";
			}
		} else {
			return null;
		}
		
	}

	private String emitHeaderRowIntoJson(String[] row, String fileName) {
		// TODO Auto-generated method stub
		
//		SAMPLE
//		{
//			"type" : "record",
//			"namespace" : "cblecken",
//			"name" : "cust",
//			"fields" : [
//				{ "name" : "first" , "type" : "string"},
//				{ "name" : "middle" , "type" : "string"},
//				{ "name" : "last" , "type" : "string"},
//				{ "name" : "dob" , "type" : "string"},
//				{ "name" : "ssn" , "type" : "long"},
//				{ "name" : "gender" , "type" : "string"},
//				{ "name" : "metrics" , "type" : "string"}
//			]
//		}	
		StringBuilder jsonSchema = new StringBuilder();
		jsonSchema.append("{" + lineSep);  // Object open
		jsonSchema.append("\"type\" : \"record\"," + lineSep);  // type
		jsonSchema.append("\"namespace\" : \"csvconverted\"," + lineSep);  // namespace
		jsonSchema.append("\"name\" : \"" + fileName + "\"," + lineSep);  // name
		jsonSchema.append("\"fields\" : [" + lineSep);  // fields
		for (int ind = 0; ind < row.length; ind++) {
			String comma = (ind + 1 == row.length) ? "" : ",";
			jsonSchema.append("  { \"name\" : \"" + row[ind] + "\", \"type\" : \"string\" }" + comma + lineSep);
		}
		jsonSchema.append(" ]" + lineSep);  // end of fields
		jsonSchema.append("}" + lineSep);  // end of fields
		return jsonSchema.toString();
		
	}

}
