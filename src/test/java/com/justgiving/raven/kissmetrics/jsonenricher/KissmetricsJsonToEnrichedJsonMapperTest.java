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
import java.util.List;

import junit.framework.Assert;

import org.json.JSONException;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.json.simple.parser.ParseException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.apache.hadoop.mrunit.internal.util.Errors;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import com.justgiving.raven.kissmetrics.KissmetricsConstants;
import com.justgiving.raven.kissmetrics.KissmetricsConstants;
import com.justgiving.raven.kissmetrics.utils.KissmetricsJsonRowBuilder;
import com.justgiving.raven.kissmetrics.utils.KissmetricsRowParser;

	
public class KissmetricsJsonToEnrichedJsonMapperTest extends KissmetricsJsonToEnrichedJsonTestBase {
	
	KissmetricsJsonRowBuilder jsonRowbuilder = new KissmetricsJsonRowBuilder();
	
	//Counter Tests

	@Test
	public void mapper_validRecord_IncrementsIncludedCounter() throws IOException {
		
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453");
		String expectedJSON = jsonRowbuilder.toString();
				
		mapDriver.run();
	    Assert.assertEquals(1, getCounter(KissmetricsConstants.TRACKING_COUNTER.VALID_JSON_ROW)); 
	}
	

	@Test
	public void mapper_validRecord_IncrementsIncludedCounterTwice() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453");
		String secondJSON = jsonRowbuilder.toString();
				
		mapDriver.withInput(new LongWritable(1), new Text(secondJSON));
		mapDriver.run();
	    Assert.assertEquals(2, getCounter(KissmetricsConstants.TRACKING_COUNTER.VALID_JSON_ROW));
	}
	

	@Test
	public void mapper_invalidRecord_IncrementsInvalidJsonRowCounter() throws IOException {
			mapDriver.withInput(new LongWritable(1), new Text("{not valid json"));
			mapDriver.run();
	       	Assert.assertEquals(1, getCounter(KissmetricsConstants.TRACKING_COUNTER.INVALID_JSON_ROW));
	}
		
	@Test
	public void mapper_invalidRecord_DoesNotIncrementValidJsonRowCounter() throws IOException {
			mapDriver.withInput(new LongWritable(1), new Text("{not valid json"));
			mapDriver.run();
	       Assert.assertEquals(0, getCounter(KissmetricsConstants.TRACKING_COUNTER.VALID_JSON_ROW));
	}
	
	@Test
	public void mapper_InvalidDate_IncrementsInvalidDateRowCounter() throws IOException {
		jsonRowbuilder.setValue("_t","I am not a UNIX timestamp");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));
		mapDriver.run();
		Assert.assertEquals(1, getCounter(KissmetricsConstants.TRACKING_COUNTER.INVALID_DATE));
	}
	
	@Test
	public void mapper_InvalidDate_IncrementsInvalidJsonRowCounter() throws IOException {
		jsonRowbuilder.setValue("_t","I am not a UNIX timestamp");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));
		mapDriver.run();
		Assert.assertEquals(1, getCounter(KissmetricsConstants.TRACKING_COUNTER.INVALID_JSON_ROW));
	}	
			
	@Test
	public void mapper_ValidRow_EnrichWithDateAndFileUsed() throws IOException,JSONException {
		
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
		
	//// ID and Email tests///////////////
		
	@Test
	public void mapper_EmailAndID_emailIdData() throws IOException, JSONException {
		jsonRowbuilder.setValue("_p", "just@gmail.com")
					  .setValue("_p2", "22wlxqlulqe24q/jl4aqlibdfdfe=");

		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));
		jsonRowbuilder.setValue("event", "viewed signup")
				  .setValue("user_email", "just@gmail.com")
				  .setValue("user_email_back", "just@gmail.com")
  				  .setValue("user_km_id", "22wlxqlulqe24q/jl4aqlibdfdfe=")
				  .setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("filename","somefile")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
				
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);	
	}
	
	@Test
	public void mapper_EmailAndIDInverted_emailIdData() throws IOException, JSONException {
		jsonRowbuilder.setValue("_p2", "just@gmail.com")
					  .setValue("_p", "22wlxqlulqe24q/jl4aqlibdfdfe=");

		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));
		jsonRowbuilder.setValue("event", "viewed signup")
				  .setValue("user_email", "just@gmail.com")
				  .setValue("user_email_back", "just@gmail.com")
  				  .setValue("user_km_id", "22wlxqlulqe24q/jl4aqlibdfdfe=")
				  .setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("filename","somefile")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
				
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);	
	}
	
	@Test
	public void mapper_NoEmailAndID_IdData() throws IOException, JSONException {
		jsonRowbuilder.removePair("_p2")
					  .setValue("_p", "22wlxqlulqe24q/jl4aqlibdfdfe=");

		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));
		jsonRowbuilder.setValue("event", "viewed signup")
  				  .setValue("user_km_id", "22wlxqlulqe24q/jl4aqlibdfdfe=")
				  .setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("filename","somefile")
				  .setValue("bucket","somefile")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
				
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);	
	}
	
	@Test
	public void mapper_NoEmailAndIDP2_IdData() throws IOException, JSONException {
		jsonRowbuilder.removePair("_p")
					  .setValue("_p2", "22wlxqlulqe24q/jl4aqlibdfdfe=");

		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));
		jsonRowbuilder.setValue("event", "viewed signup")
  				  .setValue("user_km_id", "22wlxqlulqe24q/jl4aqlibdfdfe=")
				  .setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("filename","somefile")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
				
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);	
	}
	
	@Test
	public void mapper_ValidRowEmailNoID_EmailEnrichWithDateAndFileUsed() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p")
					  .setValue("_p2", "just@gmail.com");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")				  
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("km_timestamp", "1397577453")
				  .setValue("user_email", "just@gmail.com")
				  .setValue("user_email_back", "just@gmail.com")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}

	@Test
	public void mapper_ValidRowEmailUpperCase_EmailLowerCaseEnrichWithDateAndFileUsed() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p")
					  .setValue("_p2", "JusT@gmail.com");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")				  
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("km_timestamp", "1397577453")
				  .setValue("user_email", "just@gmail.com")
				  .setValue("user_email_back", "just@gmail.com")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	@Test
	public void mapper_ValidRowPEmailNoID_EmailEnrichWithDateAndFileUsed() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p2")
					  .setValue("_p", "just@gmail.com");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")				  
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("km_timestamp", "1397577453")
				  .setValue("user_email", "just@gmail.com")
				  .setValue("user_email_back", "just@gmail.com")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	@Test
	public void mapper_ValidRowNoEmailNoID_DateAndFileUsed() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p").removePair("_p2");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")				  
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	//Test mobile date
	@Test
	public void mapper_ValidRowWithSameTAndMobileDeviceTime_validDates() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p")
					  .removePair("_p2")
					  .setValue("_server_timestamp", "1397577453")
					  .setValue("_c", "mobile_app")
					  .setValue("_t", "1397577453");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("filename","somefile")
				      .setValue("event", jsonRowbuilder.getValue("_n"))
				      .setValue("km_timestamp", "1397577453")
  				      .setValue("km_timestamp_mobile", "1397577453")
				      .setValue("event_timedate", "2014-04-15 16:57:33")
				      .setValue("event_timedate_mobile", "2014-04-15 16:57:33")
				      .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	@Test
	public void mapper_ValidRowWithDifferentTAndMobileDeviceTime_validDates() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p")
					  .removePair("_p2")
					  .setValue("_t", "1397577453")
					  .setValue("_server_timestamp", "1397577456")
					  .setValue("_c", "mobile_app");
		
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("filename","somefile")
				      .setValue("event", jsonRowbuilder.getValue("_n"))
				      .setValue("km_timestamp", "1397577456")
  				      .setValue("km_timestamp_mobile", "1397577453")
				      .setValue("event_timedate", "2014-04-15 16:57:36")
  				      .setValue("event_timedate_mobile", "2014-04-15 16:57:33")
				      .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	@Test
	public void mapper_ValidRowWithMobileDeviceTimeNoC_validDate() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p")
					  .removePair("_p2")
					  .setValue("_t", "1397577453")
					  .setValue("_server_timestamp", "1397577456");
		
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("filename","somefile")
				      .setValue("event", jsonRowbuilder.getValue("_n"))
				      .setValue("km_timestamp", "1397577453")
				      .setValue("event_timedate", "2014-04-15 16:57:33")
				      .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	@Test
	public void mapper_ValidRowWithNoMobileDeviceTimeAndC_validDate() throws IOException,JSONException {
		
		jsonRowbuilder.removePair("_p")
					  .removePair("_p2")
					  .setValue("_c", "mobile_app")
					  .setValue("_t", "1397577453");
		
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("filename","somefile")
				      .setValue("event", jsonRowbuilder.getValue("_n"))
				      .setValue("km_timestamp", "1397577453")
				      .setValue("event_timedate", "2014-04-15 16:57:33")
				      .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);
	}
	
	
	///Character Tests//
	
	@Test
	public void mapper_PoundSignInRow_PoundSignOuputCorrectly() throws IOException, JSONException {
		jsonRowbuilder.setValue("amount_raised","\302\243121.00");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);		
	}
	
	
	@Test
	public void mapper_AccentEncoding_AccentResolved() throws IOException, JSONException {
		jsonRowbuilder.setValue("title","R BROWNE is fundraising for M\303\251decins Sans Fronti\303\250res (UK)");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("title", "R BROWNE is fundraising for MÃ©decins Sans FrontiÃ¨res (UK)")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);		
	}
	
	@Test
	public void mapper_UserAgentSlashChar_UserAgent() throws IOException, JSONException {
		jsonRowbuilder.setValue("user_agent","Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("user_agent", "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);		
	}
	
	@Test
	public void mapper_testMapperNonLatinChar_EscapedOutputrow() throws IOException, JSONException {
		jsonRowbuilder.setValue("page_title","\346\215\220\350\264\210\347\265\246sumatranelephantemergency - JustGiving");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("page_title","æè´çµ¦sumatranelephantemergency - JustGiving")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);		
	}
	
	
	@Test
	public void mapper_JsonEscapedRowsURL_parsedrow() throws IOException, JSONException {
		jsonRowbuilder.setValue("url","https://www.justgiving.com/BelCenMission/donate/?utm_source=website_cid247466&utm_medium=buttons&utm_content=BelCenMission&utm_campaign=donate_whiteC:\\\\Users\\\\gordonj\\\\Documents\\\\2011-2012");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("url","https://www.justgiving.com/BelCenMission/donate/?utm_source=website_cid247466&utm_medium=buttons&utm_content=BelCenMission&utm_campaign=donate_whiteC:\\\\Users\\\\gordonj\\\\Documents\\\\2011-2012")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);		
	}
	
	

	@Test
	public void mapper_JsonEscapedRowsInUserAgent_parsedrow() throws IOException, JSONException {
		jsonRowbuilder.setValue("user_agent","Mozilla/5.0 (Windows NT 6.1; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; Tablet PC 2.0; .NET CLR 1.1.4322; BRI/2; Burton Primary School\\\\; rv:11.0) like Gecko");
		mapDriver.withInput(new LongWritable(1), new Text(jsonRowbuilder.toString()));		
		
		jsonRowbuilder.setValue("event_timedate","2014-04-15 16:57:33")
				  .setValue("filename","somefile")
				  .setValue("event", jsonRowbuilder.getValue("_n"))
				  .setValue("user_email", "justgiving@gmail.com")
				  .setValue("user_email_back", "justgiving@gmail.com")
				  .setValue("user_agent","Mozilla/5.0 (Windows NT 6.1; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; Tablet PC 2.0; .NET CLR 1.1.4322; BRI/2; Burton Primary School\\\\; rv:11.0) like Gecko")
				  .setValue("user_km_id", "3lwlxqlulqe24q/jl4aqlibrtte=")
				  .setValue("km_timestamp", "1397577453")
				  .setValue("bucket","somefile");
		String expectedJSON = jsonRowbuilder.toString();
		
		List<Pair<Text, Text>> output = mapDriver.run();	
		String actualJSON = output.get(0).getSecond().toString();
		
		JSONAssert.assertEquals(expectedJSON, actualJSON, true);		
	}
	

}
