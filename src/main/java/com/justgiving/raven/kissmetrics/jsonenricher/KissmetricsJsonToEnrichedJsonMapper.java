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

package com.justgiving.raven.kissmetrics.jsonenricher;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.justgiving.raven.kissmetrics.utils.KeyRowWrapper;
import com.justgiving.raven.kissmetrics.utils.KissmetricsRowParser;
import com.justgiving.raven.kissmetrics.KissmetricsConstants.TRACKING_COUNTER;

import org.apache.log4j.Logger;

/****
 * This mapper takes in a json rows, parses the elements based on a predefined schema into a tab sperated file
 * it emits the email and/ID as key and the full tsv as value
 * 
 * @author rfreeman
 *
 */
public class KissmetricsJsonToEnrichedJsonMapper extends Mapper<LongWritable, Text, Text, Text> {

	static DateFormat dateFormatter = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss"); // %Y-%m-%d %H:%M:%S
	
	public String getCurrentDate(){
		Calendar calendar = Calendar.getInstance();
		String event_timedate = dateFormatter.format(calendar.getTime());
		return event_timedate;
	}
	
	@Override
	public void map(LongWritable rowKey, Text rawJsonRow, Context context) throws IOException, InterruptedException {
			final Logger logger = Logger.getLogger(KissmetricsJsonToEnrichedJsonMapper.class);

			String fileNameInputToMapper = "";
			String filePath = "";
			try{
				fileNameInputToMapper = ((FileSplit) context.getInputSplit()).getPath().getName();
				filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			} catch (Exception e) {
				logger.info("unable to get file inputpath");
				System.out.println("unable to get file inputpath");				
			}
			//TODO: String capturedDate = getCurrentDate();
			KeyRowWrapper keyRow = KissmetricsRowParser.parseJsonRowToValidJson(rawJsonRow, fileNameInputToMapper, filePath);
			
			if(keyRow != null){
				if(keyRow.getKey() != null && keyRow.getJsonrow() != null &&
						keyRow.getReturnCode() == TRACKING_COUNTER.VALID_JSON_ROW){
					context.getCounter(TRACKING_COUNTER.VALID_JSON_ROW).increment(1);
					
					//TODO: Monitoring use of Octal Decoder
					if(keyRow.getMonitoringCode() == TRACKING_COUNTER.OCTAL_PARSING_NEEDED){
						context.getCounter(TRACKING_COUNTER.OCTAL_PARSING_NEEDED).increment(1);
					}
					
					context.write(new Text(keyRow.getKey()), new Text(keyRow.getJsonrow()));
				}else if (keyRow.getMonitoringCode() == TRACKING_COUNTER.INVALID_DATE){
					logger.info("Error on row parsing, skipping row");
					System.out.println("Error on row parsing");
					System.out.println("Skipped row with contents: " + rawJsonRow.toString());
					System.out.println("--------------------------------");
					context.getCounter(TRACKING_COUNTER.INVALID_JSON_ROW).increment(1);
					context.getCounter(TRACKING_COUNTER.INVALID_DATE).increment(1);
				}else { // Assume it's an invalid
					logger.info("Error on row parsing, skipping row");
					System.out.println("Error on row parsing");
					System.out.println("Skipped row with contents: " + rawJsonRow.toString());
					System.out.println("--------------------------------");
					context.getCounter(TRACKING_COUNTER.INVALID_JSON_ROW).increment(1);
				}
			}
			else{
				logger.info("Error on row parsing, skipping row");
				System.out.println("Error on row parsing");
				System.out.println("Skipped row with contents: " + rawJsonRow.toString());
				System.out.println("--------------------------------");
				context.getCounter(TRACKING_COUNTER.INVALID_JSON_ROW).increment(1);
			}
						
	}
}
