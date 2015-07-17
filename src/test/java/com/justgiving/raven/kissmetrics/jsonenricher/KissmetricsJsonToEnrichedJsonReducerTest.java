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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.justgiving.raven.kissmetrics.utils.KissmetricsJsonRowBuilder;

public class KissmetricsJsonToEnrichedJsonReducerTest extends KissmetricsJsonToEnrichedJsonTestBase {
	KissmetricsJsonRowBuilder rowbuilder = new KissmetricsJsonRowBuilder();


	@Test
	public void reducer_keyAndjson_json() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"_t\":1397577453}"));
		//values.add(new IntWritable(1));
		
		//mapDriver.withInput(new LongWritable(1), new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"_t\":1397577453}"));
				
		reduceDriver.withInput(new Text("1397577453"), values);
		NullWritable nullKey = NullWritable.get();
		reduceDriver.withOutput(new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"_t\":1397577453}"), nullKey);//, NullWritable);
		reduceDriver.runTest(true);
	}
	
	
	@Test
	public void reducer_keyAndjson2_json() throws IOException, JSONException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text(rowbuilder.toString()));
				
		//values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("1397577453"), values);
		
		String expectedJSON = rowbuilder.toString();
		
		List<Pair<Text, NullWritable>> output = reduceDriver.run();	
		String actualJSON = output.get(0).getFirst().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
		
		//mapDriver.withInput(new LongWritable(1), new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"_t\":1397577453}"));
				
		/*reduceDriver.withInput(new Text("1397577453"), values);
		NullWritable nullKey = NullWritable.get();
		reduceDriver.withOutput(new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"_t\":1397577453}"), nullKey);//, NullWritable);
		reduceDriver.runTest(true);*/
	}

}
