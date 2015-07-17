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

import java.nio.file.Paths;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.justgiving.raven.kissmetrics.KissmetricsConstants;
import com.justgiving.raven.kissmetrics.schema.KissmetricsJsonToSchemaMapper;
import com.justgiving.raven.kissmetrics.schema.KissmetricsJsonToSchemaReducer;

public abstract class KissmetricsJsonToEnrichedJsonTestBase {
	
	final Logger logger = Logger.getLogger(KissmetricsJsonToEnrichedJsonMapper.class);

	MapReduceDriver<LongWritable, Text, Text, Text, Text, NullWritable> mapReduceDriver;
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, NullWritable> reduceDriver;

	@Before
	public void setUp() {
		KissmetricsJsonToEnrichedJsonMapper mapper = new KissmetricsJsonToEnrichedJsonMapper();
		KissmetricsJsonToEnrichedJsonReducer reducer = new KissmetricsJsonToEnrichedJsonReducer();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text, Text, Text, NullWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, NullWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
		String logConfigPath = Paths.get(System.getProperty("user.dir"), "log4j.properties").toString();
		//System.out.println("log config file used: " + logConfigPath);
		PropertyConfigurator.configure(logConfigPath);
		
	}
	
    protected long getCounter(KissmetricsConstants.TRACKING_COUNTER counter) {

        if(mapReduceDriver.getClass().isAssignableFrom(ReduceDriver.class))
            return ((ReduceDriver)reduceDriver).getCounters().findCounter(counter).getValue();
        else {
            return ((MapDriver)mapDriver).getCounters().findCounter(counter).getValue();
        }
    }

}
