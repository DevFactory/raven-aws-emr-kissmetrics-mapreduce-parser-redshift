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
 */

package com.justgiving.raven.kissmetrics.schema;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KissmetricsJsonToSchemaMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String s = value.toString();
		JSONParser jsonParser = new JSONParser();
		try {
			JSONObject jsonObject = (JSONObject) jsonParser.parse(s);
			Set<String> keyset = jsonObject.keySet();		
			String jsonValue = "";
			for(String jsonkey : keyset)
			{				
				jsonValue = (String) jsonObject.get(jsonkey).toString();
				if (jsonValue == null || jsonValue == ""){ jsonValue = "";   }
				String lenValue =  String.valueOf(jsonValue.length());
				if (lenValue == null || lenValue == ""){ lenValue = "0";   } 
				context.write(new Text(jsonkey), new Text("1\t" + lenValue));
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
 