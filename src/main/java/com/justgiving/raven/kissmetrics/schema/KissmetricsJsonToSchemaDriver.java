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

package com.justgiving.raven.kissmetrics.schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KissmetricsJsonToSchemaDriver {

	public static void main(String[] args) throws Exception {

		int numberOfReducers = 1;
		if (args.length > 2 && args[2] != null){
			numberOfReducers = Integer.parseInt(args[2]);
			if (numberOfReducers <= 0 ){
				numberOfReducers = 1;
			}
		}
		
		System.out.println("Kissmetrics Json Schema Extrator");
						
		Job job = Job.getInstance();
		job.setJarByClass(KissmetricsJsonToSchemaDriver.class);
		job.setJobName("Kissmetrics Json Schema Extrator");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(com.justgiving.raven.kissmetrics.schema.KissmetricsJsonToSchemaMapper.class);
		job.setReducerClass(com.justgiving.raven.kissmetrics.schema.KissmetricsJsonToSchemaReducer.class);
		job.setNumReduceTasks(numberOfReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
