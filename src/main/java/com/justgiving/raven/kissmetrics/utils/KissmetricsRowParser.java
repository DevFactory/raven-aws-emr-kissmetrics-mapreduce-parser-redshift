/*
 * Copyright (c) 2014-2015 Giving.com, trading as JustGiving or its affiliates. All Rights Reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"). 
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located in the "license" file accompanying this file.
 * 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for 
 * the specific language governing permissions and limitations under the License.
 * 
 * @author Richard Freeman
 * 
 */

package com.justgiving.raven.kissmetrics.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import com.justgiving.raven.kissmetrics.KissmetricsConstants.TRACKING_COUNTER;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class KissmetricsRowParser {

	static final Logger logger = Logger.getLogger(KissmetricsRowParser.class);
	private static final int max_property_value_size = 1500;
	static DateFormat dateFormatter = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss"); // %Y-%m-%d %H:%M:%S
	private static JSONParser jsonParser = new JSONParser();
	private static String id = "";
	private static String emailaddress = "";
	private static String tsvRow = "";
	private static String propertyValue = "";
	private static String timestampValueOutput = "";
	private static String mobileTimestampValueOutput = "";
	private static String tTimestampValue = "";
	private static String serverTimestampValue = "";
	private static String p = "";
	private static String p2 = "";
	private static String s = "";
	private static String event = "";
	private static String decodedStrRaw = "";
	private static String decodedStrParsed = "";

	public static String getDefaultCharEncoding() {
		byte[] bArray = { 'w' };
		InputStream is = new ByteArrayInputStream(bArray);
		InputStreamReader reader = new InputStreamReader(is);
		String defaultCharacterEncoding = reader.getEncoding();
		return defaultCharacterEncoding;
	}

	/***
	 * This method is used to replace any Octal encoded character when the
	 * existing decoding is not working.
	 * Source: http://www.utf8-chartable.de/unicode-utf8-table.pl?utf8=oct&unicodeinhtml=dec
	 * 
	 * U+0000 to U+00FF - basic latin
	 * 
	 * @param input
	 * @return Octal replaced String
	 */
	public static String replaceOctalUft8Char(String input) {
		String output = input.replace("\\302\\241", "�")
				.replace("\\302\\242", "�").replace("\\302\\243", "�")
				.replace("\\302\\244", "�").replace("\\302\\245", "�")
				.replace("\\302\\246", "�").replace("\\302\\247", "�")
				.replace("\\302\\250", "�").replace("\\302\\251", "�")
				.replace("\\302\\252", "�").replace("\\302\\253", "�")
				.replace("\\302\\254", "�").replace("\\302\\255", "�")
				.replace("\\302\\256", "�").replace("\\302\\257", "�")
				.replace("\\302\\260", "�").replace("\\302\\261", "�")
				.replace("\\302\\262", "�").replace("\\302\\263", "�")
				.replace("\\302\\264", "�").replace("\\302\\265", "�")
				.replace("\\302\\266", "�").replace("\\302\\267", "�")
				.replace("\\302\\270", "�").replace("\\302\\271", "�")
				.replace("\\302\\272", "�").replace("\\302\\273", "�")
				.replace("\\302\\274", "�").replace("\\302\\275", "�")
				.replace("\\302\\276", "�").replace("\\302\\277", "�")
				.replace("\\303\\200", "�").replace("\\303\\201", "�")
				.replace("\\303\\202", "�").replace("\\303\\203", "�")
				.replace("\\303\\204", "�").replace("\\303\\205", "�")
				.replace("\\303\\206", "�").replace("\\303\\207", "�")
				.replace("\\303\\210", "�").replace("\\303\\211", "�")
				.replace("\\303\\212", "�").replace("\\303\\213", "�")
				.replace("\\303\\214", "�").replace("\\303\\215", "�")
				.replace("\\303\\216", "�").replace("\\303\\217", "�")
				.replace("\\303\\220", "�").replace("\\303\\221", "�")
				.replace("\\303\\222", "�").replace("\\303\\223", "�")
				.replace("\\303\\224", "�").replace("\\303\\225", "�")
				.replace("\\303\\226", "�").replace("\\303\\227", "�")
				.replace("\\303\\230", "�").replace("\\303\\231", "�")
				.replace("\\303\\232", "�").replace("\\303\\233", "�")
				.replace("\\303\\234", "�").replace("\\303\\235", "�")
				.replace("\\303\\236", "�").replace("\\303\\237", "�")
				.replace("\\303\\240", "�").replace("\\303\\241", "�")
				.replace("\\303\\242", "�").replace("\\303\\243", "�")
				.replace("\\303\\244", "�").replace("\\303\\245", "�")
				.replace("\\303\\246", "�").replace("\\303\\247", "�")
				.replace("\\303\\250", "�").replace("\\303\\251", "�")
				.replace("\\303\\252", "�").replace("\\303\\253", "�")
				.replace("\\303\\254", "�").replace("\\303\\255", "�")
				.replace("\\303\\256", "�").replace("\\303\\257", "�")
				.replace("\\303\\260", "�").replace("\\303\\261", "�")
				.replace("\\303\\262", "�").replace("\\303\\263", "�")
				.replace("\\303\\264", "�").replace("\\303\\265", "�")
				.replace("\\303\\266", "�").replace("\\303\\267", "�")
				.replace("\\303\\270", "�").replace("\\303\\271", "�")
				.replace("\\303\\272", "�").replace("\\303\\273", "�")
				.replace("\\303\\274", "�").replace("\\303\\275", "�")
				.replace("\\303\\276", "�").replace("\\303\\277", "�");
		return output;
	}

	/***
	 * Used to parse, escape and enrich Kissmetircs Json records
	 * 
	 * @param rawJsonRow
	 * @param fileNameInputToMapper
	 * @return
	 */
	public static KeyRowWrapper parseJsonRowToValidJson(Text rawJsonRow,
			String fileNameInputToMapper, String filePath) {

		String jsonString = "";
		boolean wasOctalParsingNeeded = false;

		try {
			System.setProperty("file.encoding", "UTF-8");
			s = rawJsonRow.toString();
			Charset charSet = Charset.forName("UTF-8");
			byte[] encoded = s.getBytes(charSet);
			decodedStrRaw = new String(encoded, charSet);

			// Test new Apache Lang3
			// decodedStr = StringEscapeUtils.unescapeJava(decodedStr);

			//Replace any remaining Octal encoded Characters
			decodedStrParsed = replaceOctalUft8Char(decodedStrRaw);
			if(decodedStrParsed.compareTo(decodedStrRaw) == 0){
				wasOctalParsingNeeded = false;
			}else{
				wasOctalParsingNeeded = true;
			}
						
			if (decodedStrParsed != null && decodedStrParsed != "") {
				JSONObject jsonObject = (JSONObject) jsonParser
						.parse(decodedStrParsed);

				// get email and user_id
				if (jsonObject.get("_p2") != null) {
					p2 = jsonObject.get("_p2").toString().toLowerCase();
					if (p2.contains("@")) {
						jsonObject.put("user_email", p2);
						jsonObject.put("user_email_back", p2);
					} else if (p2 != null && p2.length() > 0) {
						jsonObject.put("user_km_id", p2);
					}
				}
				// get email and user_id
				if (jsonObject.get("_p") != null) {
					p = jsonObject.get("_p").toString().toLowerCase();
					if (p.contains("@")) {
						jsonObject.put("user_email", p);
						jsonObject.put("user_email_back", p);
					} else if (p != null && p.length() > 0) {
						jsonObject.put("user_km_id", p);
					}
				}

				// Add Event
				if (jsonObject.get("_n") != null) {
					event = jsonObject.get("_n").toString();
					if (event != null) {
						jsonObject.put("event", event);
					}
				}

				// add unix timestamp and datetime
				long currentDateTime = System.currentTimeMillis();
				Date currentDate = new Date(currentDateTime);
				if (jsonObject.get("_t") == null) {
					return (new KeyRowWrapper(jsonString, null, TRACKING_COUNTER.INVALID_JSON_ROW, TRACKING_COUNTER.INVALID_DATE));
				}
				long kmTimeDateMilliSeconds;
				long kmTimeDateMilliSecondsMobile;
				try{
					tTimestampValue = (String) jsonObject.get("_t").toString();
					
					//See if new record with server timestamp
					if (jsonObject.get("_server_timestamp") != null) {
						serverTimestampValue = (String) jsonObject.get("_server_timestamp").toString();
					}else{
						serverTimestampValue = "0";
					}
					
					//Deal with mobile timedate cases
					if (jsonObject.get("_c") != null){
						if(serverTimestampValue.equals("0")){
							timestampValueOutput =tTimestampValue;		
							kmTimeDateMilliSecondsMobile = 0;
						}else{
							timestampValueOutput = serverTimestampValue;
							mobileTimestampValueOutput = tTimestampValue;		
							jsonObject.put("km_timestamp_mobile", mobileTimestampValueOutput);
							kmTimeDateMilliSecondsMobile = Long.parseLong(mobileTimestampValueOutput) * 1000;							
						}											
					}else{//Ignore server time
						//TODO Need a way to resolve mobile identify events
						serverTimestampValue = "0";
						timestampValueOutput = tTimestampValue;
						kmTimeDateMilliSecondsMobile = 0;
					}
					
					jsonObject.put("km_timestamp", timestampValueOutput);
					kmTimeDateMilliSeconds = Long.parseLong(timestampValueOutput) * 1000;
				}catch (Exception e) {					
					return (new KeyRowWrapper(jsonString, timestampValueOutput, TRACKING_COUNTER.INVALID_JSON_ROW, TRACKING_COUNTER.INVALID_DATE));
				}
				Calendar calendar = Calendar.getInstance();
				calendar.setTimeInMillis(kmTimeDateMilliSeconds);
				String event_timedate = dateFormatter
						.format(calendar.getTime());
				jsonObject.put("event_timedate", event_timedate);
				
				if(kmTimeDateMilliSecondsMobile > 0){
					calendar.setTimeInMillis(kmTimeDateMilliSecondsMobile);
					String event_timedate_mobile = dateFormatter
							.format(calendar.getTime());
					jsonObject.put("event_timedate_mobile", event_timedate_mobile);					
				}

				// add Map Reduce json_filename
				jsonObject.put("filename", fileNameInputToMapper);
				jsonString = jsonObject.toString();

				//Add bucket path
				jsonObject.put("bucket", filePath);
				jsonString = jsonObject.toString();
				
				// TODO add the time the record was processed by Mapper:
				//jsonObject.put("capturedDate", capturedDate);
				//jsonString = jsonObject.toString();
				
				return (new KeyRowWrapper(jsonString, timestampValueOutput, TRACKING_COUNTER.VALID_JSON_ROW, 
						wasOctalParsingNeeded ? null : TRACKING_COUNTER.OCTAL_PARSING_NEEDED ));
				

			}

		} catch (Exception e) {
			// System.err.println(e.getMessage());
			// e.printStackTrace();
			StringWriter errors = new StringWriter();
			e.printStackTrace(new PrintWriter(errors));
			logger.error(errors.toString());

			logger.error("log - file " + fileNameInputToMapper);
			System.out.println("file " + fileNameInputToMapper);

			logger.error("log - row content: "
					+ rawJsonRow.toString().replace("\t", ""));
			System.err.println("row content: "
					+ rawJsonRow.toString().replace("\t", ""));

			System.err.println("Error skipping row");
			logger.error("Log - Error skipping row");
		}
		return null;
	}

	public static String runOnStringJson(Text rawJsonRow, String output_filename)
			throws FileNotFoundException {

		String fileNameInputToMapper = "pathtocurrentfile";
		//String capturedDate = getCurrentDate();
		KeyRowWrapper newValidJson = KissmetricsRowParser
				.parseJsonRowToValidJson(rawJsonRow, fileNameInputToMapper, output_filename);
		//logger.info(newValidJson.jsonrow);
		return newValidJson.jsonrow;
	}
	
	//static DateFormat dateFormatter = new SimpleDateFormat(
//			"yyyy-MM-dd HH:mm:ss"); // %Y-%m-%d %H:%M:%S
	
	public static String getCurrentDate(){
		Calendar calendar = Calendar.getInstance();
		String event_timedate = dateFormatter.format(calendar.getTime());
		return event_timedate;
	}
	


	public static void runonfileValidJson(String input_filename,
			String output_filename) throws IOException {
		InputStream fis;
		BufferedReader bufferdReader;
		String line;

		try {
			File file = new File(output_filename);
			if (file.createNewFile()) {
				logger.warn("File has been created");
			}//else {
			//	logger.info("File already exists.");
			//}
			// if (!file.getParentFile().mkdirs())
			// throw new IOException("Unable to create " +
			// file.getParentFile());

			FileWriter fileWriter = new FileWriter(output_filename, false);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			String parsedLine;
			fis = new FileInputStream(input_filename);
			bufferdReader = new BufferedReader(new InputStreamReader(fis,
					Charset.forName("UTF-8")));
			while ((line = bufferdReader.readLine()) != null) {
				parsedLine = runOnStringJson(new Text(line), output_filename) + "\n";
				bufferedWriter.write(parsedLine);
			}
			bufferedWriter.close();
			bufferdReader.close();
		} catch (IOException e) {
			logger.error("Error writing to file '" + output_filename + "'");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("Ouput written to " + output_filename);
	}

	private static void processFolder(String inputFolder, String outputFolder)
			throws IOException {

		File file = new File(outputFolder);
		if (!file.exists()) {
			if (file.mkdirs())
				logger.info("Directory successfully created");
			else
				logger.error("Failed to create directory");
		}

		File folder = new File(inputFolder);
		File[] listOfFiles = folder.listFiles();
		for( File currentFile : listOfFiles){
			if(currentFile.isFile()){
				logger.info("File " + currentFile.getName());
				runonfileValidJson(Paths.get(inputFolder, currentFile.getName()).toString(),
						   Paths.get(outputFolder, currentFile.getName()).toString());				
			}else if (currentFile.isDirectory()){
				//System.out.println("Directory " + currentFile.getName());
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		for (String s : args) {
			System.out.println(s);
		}

		 String inputFile ="D:\\datasets\\kissmetrics\\input\\2250.json";
		 String outputFile ="D:\\datasets\\kissmetrics\\output\\2250.json";
		// String inputFile ="D:\\datasets\\kissmetrics\\input\\";
		//String inputFile = "D:\\datasets\\kissmetrics\\input5\\";
		//String outputFile = "D:\\datasets\\kissmetrics\\output5\\";

		if (args.length == 2) {
			try {
				inputFile = args[0];
				outputFile = args[1];
			} catch (Exception e) {
				System.err.println("Error unable to extract arguments, valid arguments are inputFilePath inputFilePath");
				System.exit(1);
			}
		} else if (args == null || args.length == 0){
			logger.info("using defaul values for inputFile=" + inputFile + " outputFile=" + outputFile);
		}
		
		String logConfigPath = Paths.get(System.getProperty("user.dir"),
				"log4j.properties").toString();
		
		File f = new File(logConfigPath);
		if(f.exists() && !f.isDirectory()) { 
			System.out.println("log config file used: " + logConfigPath);
			PropertyConfigurator.configure(logConfigPath);
			logger.info("log config file used: " + logConfigPath);
		}else{
			System.out.println("no log file detected, please copy the log4j.properties to the same folder as the JAR");			
		}
		
		if (inputFile.endsWith("\\")) {
			logger.info("Detected folder");
			processFolder(inputFile, outputFile);
		} else {
			logger.info("Detected file");
			runonfileValidJson(inputFile, outputFile);
		}
	}
}
