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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class KissmetricsJsonToEnrichedJsonDriver {
	static final Logger logger = Logger.getLogger(KissmetricsJsonToEnrichedJsonDriver.class);
	
    public static String getDefaultCharEncoding(){
        byte [] bArray = {'w'};
        InputStream is = new ByteArrayInputStream(bArray);
        InputStreamReader reader = new InputStreamReader(is);
        String defaultCharacterEncoding = reader.getEncoding();
        return defaultCharacterEncoding;
    }

	public static void main(String[] args) throws Exception {
		
		logger.info("Logger - Converting Kissmetrics Json to Valid Json files");
		System.out.println("Converting Kissmetrics Json to Valid Json files");
		System.out.println("defaultCharacterEncoding by property: " + System.getProperty("file.encoding"));
        System.out.println("defaultCharacterEncoding by code: " + getDefaultCharEncoding());
        System.out.println("defaultCharacterEncoding by charSet: " + Charset.defaultCharset());
		
		Job job = Job.getInstance();
		job.setJarByClass(KissmetricsJsonToEnrichedJsonDriver.class);
		job.setJobName("Kissmetrics Json to valid and enriched Json files");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Add number of reducers
		int numberOfReducers = 2;
		if (args.length > 2 && args[2] != null){
			numberOfReducers = Integer.parseInt(args[2]);
			if (numberOfReducers <= 0 ){
				numberOfReducers = 2;
			}
		}
		
		job.setMapperClass(com.justgiving.raven.kissmetrics.jsonenricher.KissmetricsJsonToEnrichedJsonMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(com.justgiving.raven.kissmetrics.jsonenricher.KissmetricsJsonToEnrichedJsonReducer.class);
		job.setNumReduceTasks(numberOfReducers);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
