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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/****
 * This reducer class takes all the json values from the the reducer and enriches it with the 
 * email and IDs where missing
 * 
 * If the ID and email address co-occur (the identify event) then the email is populated backward for all 
 * events.
 * 
 * @author rfreeman
 *
 */
public class KissmetricsJsonToEnrichedJsonReducer extends Reducer<Text, Text, Text, NullWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		//int total = 0;
		for (Text value : values) {
		//	total += value.get();
			context.write(value, NullWritable.get());

		}
		
	}
}
