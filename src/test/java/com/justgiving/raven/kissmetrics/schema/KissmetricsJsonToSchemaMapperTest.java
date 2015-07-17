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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class KissmetricsJsonToSchemaMapperTest extends KissmetricsJsonToSchemaTestBase {
	
	
	@Test
	public void testMapper_WithOneValidjsonRow__OneCountOfProperties() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"useragent\":\"Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0\",\"_t\":1397577453}"));
		mapDriver.withOutput(new Text("_n"), new Text("1\t13"));
		mapDriver.withOutput(new Text("_p"), new Text("1\t11"));
		mapDriver.withOutput(new Text("useragent"), new Text("1\t72"));
		mapDriver.withOutput(new Text("_t"), new Text("1\t10"));
		mapDriver.runTest(false);
	}
	
	
	@Test
	public void testMapper_WithOneValidjsonRowUrl__OneCountOfProperties() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("{\"_n\":\"viewed signup\",\"_p\":\"bob@bob.com\",\"url\":\"www.bob.com\",\"useragent\":\"Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0\",\"_t\":1397577453}"));
		mapDriver.withOutput(new Text("_n"), new Text("1\t13"));
		mapDriver.withOutput(new Text("_p"), new Text("1\t11"));
		mapDriver.withOutput(new Text("useragent"), new Text("1\t72"));
		mapDriver.withOutput(new Text("_t"), new Text("1\t10"));
		mapDriver.withOutput(new Text("url"), new Text("1\t11"));
		mapDriver.runTest(false);
		
	}


}
