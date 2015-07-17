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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class KissmetricsLocalSchemaExtractor {
	
	static final Logger logger = Logger.getLogger(KissmetricsLocalSchemaExtractor.class);
	
	/****
	 * This function parses all the json record files in a folder and returns a counts of the total occurrences of keys
	 * in all files
	 * 
	 * @param inputFolder
	 * @param outputFolder
	 * @throws IOException
	 */
	private static void countKeysInJsonRecordsFolder(String inputFolder, String outputFile) throws IOException{
		File folder = new File(inputFolder);
		File[] listOfFiles = folder.listFiles();
		KeyValueCounter totalKeyValueCounter = new KeyValueCounter();
		KeyValueCounter currentKeyValueCounter = new KeyValueCounter();
		for( File currentFile : listOfFiles){
			if(currentFile.isFile()){
	        	logger.info("Processing file: " + currentFile.getName());
	        	currentKeyValueCounter = countKeysInJsonRecordsFile(Paths.get(inputFolder, currentFile.getName()).toString());	        
	        	totalKeyValueCounter = deepMergeKeyValueCounter(totalKeyValueCounter, currentKeyValueCounter);
	        }
	        else if (currentFile.isDirectory()) {
	        	logger.warn("Sub-directory folders are currently ignored");
	      }
	    }
	    //System.out.println(totalKeyCounter.toString());
	    logger.info("---------------");
	    logger.info(sortOutputByKey(totalKeyValueCounter));
	    logger.info("saving output to file: ");
	    File outpuFile = new File(outputFile);
	    outpuFile.getParentFile().mkdirs();
	    PrintWriter out = new PrintWriter(outputFile);
	    out.print(sortOutputByKey(totalKeyValueCounter));
	    out.close();
	}
	
	
	public static KeyValueCounter deepMergeKeyValueCounter(KeyValueCounter originalMap, KeyValueCounter newMapToAdd) {
	KeyValueCounter outputValueCounter = new KeyValueCounter();
		outputValueCounter.keyCounter.putAll(originalMap.keyCounter);
		outputValueCounter.deepMergeHashMapsAddition(newMapToAdd.keyCounter);
		outputValueCounter.valueLength.putAll(originalMap.valueLength);
		outputValueCounter.deepMergeHashMapsMaxium(newMapToAdd.valueLength);
		return outputValueCounter;
	}
	
	
	/****
	 * This function counts the total occurrences of keys, in the json records files
	 *  
	 * @param input_filename path to a json file
	 * @return a HashMap with the keys / total counts pairs
	 */
	private static KeyValueCounter countKeysInJsonRecordsFile(String input_filename){
		InputStream    fis;
		BufferedReader bufferedReader;
		String         line;
		JSONParser jsonParser = new JSONParser();
		KeyValueCounter keyValueCounter = new KeyValueCounter();
		String jsonValue = "";
		try{
			fis = new FileInputStream(input_filename);
			bufferedReader = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
			while ((line = bufferedReader.readLine()) != null) {
				JSONObject jsonObject = (JSONObject) jsonParser.parse(line);
				Set<String> keyset = jsonObject.keySet();			
				for(String jsonkey : keyset)
				{
					if ( jsonObject.get(jsonkey) != null){
						jsonValue = (String) jsonObject.get(jsonkey).toString();
						if (jsonValue == null || jsonValue == ""){ jsonValue = "";   }
						int lenValue =  jsonValue.length();					 
						keyValueCounter.incrementKeyCounter(jsonkey);
						keyValueCounter.putValueLength(jsonkey, lenValue);
					}else{
						if (jsonkey.compareTo("user_agent")!= 0){
							logger.error("Errot typing to get jsonkey " + jsonkey);
						}
						
					}
				}
			}
			bufferedReader.close();
		} catch (ParseException e) {
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(keyCounter.toString());
		//System.out.println(sortHashByKey(keyCounter));		
		return keyValueCounter;		
	}
	
	/***
	 * This function sorts the HashMap values by key and returns the key/value pairs as a string
	 * @param hashMap the input hashMap
	 * @return the return string of the sorted key/value pairs
	 */
	private static String sortOutputByKey(KeyValueCounter outputKetValueCounter){
		
		Set<String> set = outputKetValueCounter.keyCounter.keySet();
		ArrayList<String> list = new ArrayList<String>();
		list.addAll(set);
		Collections.sort(list);
		StringBuilder sb = new StringBuilder();
		
		for (String key : list) {
		    sb.append(key).append("\t")
		    			  .append(outputKetValueCounter.keyCounter.get(key))
		    			  .append("\t")
		    			  .append(outputKetValueCounter.valueLength.get(key))
		    			  .append("\n");
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
        for (String s: args) {
            System.out.println(s);
        }
        
        //String inputFolder ="D:\\datasets\\kissmetrics\\input\\2250.json";
        //String outputFile ="D:\\datasets\\kissmetrics\\output\\2250.json";
        //String inputFolder ="D:\\datasets\\kissmetrics\\input\\";
        //String inputFolder ="D:\\ouptuts\\km\\input\\";
        //String inputFolder ="D:\\datasets\\kissmetrics\\input4\\revisions\\";
        //String inputFolder ="D:\\datasets\\kissmetrics\\input5\\";
        //String outputFile ="D:\\datasets\\kissmetrics\\output\\";
        //String inputFolder ="D:\\datasets\\kinesis\\input2\\";
        //String outputFile ="D:\\datasets\\kissmetrics\\output\\schema2.txt";
        //String inputFolder ="D:\\datasets\\kissmetrics\\stg\\input\\";
        //String outputFile ="D:\\datasets\\kissmetrics\\stg\\ouput\\schema1.txt";
        
        String inputFolder ="D:\\datasets\\kinesis\\stg\\input2\\";
        String outputFile ="D:\\datasets\\kinesis\\stg\\output2\\schema-kinesis.txt";
        //String inputFolder ="D:\\datasets\\kissmetrics\\prd\\input1\\";
        //String outputFile ="D:\\datasets\\kissmetrics\\prd\\output1\\schema-kissmetrics.txt";
        
        if(args.length != 2){
        	System.out.println("No arguments provided, using default values");
        	System.out.println("InputFolder/File: " + inputFolder);
        	System.out.println("OutputFile: " + outputFile);
        }else{
        	inputFolder = args[0];
        	outputFile = args[1];
        }
        
        if((new File(outputFile)).isDirectory() ){
        	System.err.println("Error output file cannot be a directory");
        	return;
        }
        
		String logConfigPath = Paths.get(System.getProperty("user.dir"), "log4j.properties").toString();
		System.out.println("log config file used: " + logConfigPath);
		PropertyConfigurator.configure(logConfigPath);
		logger.info("log config file used: " + logConfigPath);
		if(inputFolder.endsWith("\\")){
			logger.info("Detected source folder");
			countKeysInJsonRecordsFolder(inputFolder, outputFile);			
		}else{
			logger.info("Detected source file");
			countKeysInJsonRecordsFile(inputFolder);
		}
	}
}


