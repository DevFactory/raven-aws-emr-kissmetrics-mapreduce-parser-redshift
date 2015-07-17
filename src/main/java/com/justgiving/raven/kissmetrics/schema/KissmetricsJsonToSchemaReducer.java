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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KissmetricsJsonToSchemaReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int total = 0;
		int maxLen = 0;
		int currentLen = 0;
		List<String> valueList = null;
		for (Text value : values) {
			valueList = Arrays.asList(value.toString().split("\t"));
			total += Integer.valueOf(valueList.get(0));			
			if(valueList.size() > 1){
				currentLen = Integer.valueOf(valueList.get(1));
				if(maxLen < currentLen){
					maxLen = currentLen;
				}
			}
		}
		context.write(key, new Text(String.valueOf(total) + "\t" + String.valueOf(maxLen)));
	}
}
