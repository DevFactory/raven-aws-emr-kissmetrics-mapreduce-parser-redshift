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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class KissmetricsJsonToSchemaReducerTest extends KissmetricsJsonToSchemaTestBase {

	@Test
	public void testReducer_NoPair_OneCountEventNoLength() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1"));	
		reduceDriver.withInput(new Text("_n"), values);
		reduceDriver.withOutput(new Text("_n"), new Text("1\t0"));
		reduceDriver.runTest(true);
	}
	
	@Test
	public void testReducer_NoEvent_OneCountEventNoLength() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1\t0"));	
		reduceDriver.withInput(new Text("_n"), values);
		reduceDriver.withOutput(new Text("_n"), new Text("1\t0"));
		reduceDriver.runTest(true);
	}
	
	@Test
	public void testReducer_OneTimestamp_OneCountTimestampWithLength() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1\t10"));	
		reduceDriver.withInput(new Text("_t"), values);
		reduceDriver.withOutput(new Text("_t"), new Text("1\t10"));
		reduceDriver.runTest(true);
	}

	@Test
	public void testReducer_TwoTimestamps_OneCountTimestamp() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1\t10"));
		values.add(new Text("1\t10"));	
		reduceDriver.withInput(new Text("_t"), values);
		reduceDriver.withOutput(new Text("_t"), new Text("2\t10"));
		reduceDriver.runTest(true);
	}

	

}
