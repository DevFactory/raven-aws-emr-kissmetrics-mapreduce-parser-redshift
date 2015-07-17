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

package com.justgiving.raven.kissmetrics.utils;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

public class KissmetricsJsonRowBuilder {
	
	JSONObject jsonObject = new JSONObject();
	static final Logger log = Logger.getLogger(KissmetricsJsonRowBuilder.class);
	
	public KissmetricsJsonRowBuilder(){
		jsonObject.put("_p", "3lwlxqlulqe24q/jl4aqlibrtte=");
		jsonObject.put("_p2", "justgiving@gmail.com");
		jsonObject.put("_n","viewed signup");
		jsonObject.put("_t","1397577453");		
	}
	
	public String toString(){
		return jsonObject.toString();	
	}
		
	public KissmetricsJsonRowBuilder setValue(String name, String value) {
		jsonObject.put(name, value);		
		return this;
	}
	
	public String getValue (String name){
		
		if(jsonObject.get(name) != null){
			return jsonObject.get(name).toString();
		}
		else
		{
			return "";
		}
	}
	
	public KissmetricsJsonRowBuilder removePair(String name) {
		jsonObject.remove(name);		
		return this;
	}
	
}
