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

import com.justgiving.raven.kissmetrics.KissmetricsConstants.TRACKING_COUNTER;

public class KeyRowWrapper {
	public String getJsonrow() {
		return jsonrow;
	}
	public void setJsonrow(String jsonrow) {
		this.jsonrow = jsonrow;
	}
	public String getKey() {
		return this.key;
	}
	
	public TRACKING_COUNTER getMonitoringCode(){
		return this.monitoringCode;
	}
	
	public TRACKING_COUNTER getReturnCode() {
		return this.returnCode;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public void setReturnCode(TRACKING_COUNTER code){
		this.returnCode = code;
	}
	
	String jsonrow;
	String key;
	TRACKING_COUNTER returnCode;
	TRACKING_COUNTER monitoringCode;
	
	public KeyRowWrapper(String jsonrow, String key, TRACKING_COUNTER code, TRACKING_COUNTER monitoring) {
		super();
		this.jsonrow = jsonrow;
		this.key = key;
		this.returnCode = code;
		this.monitoringCode = monitoring;
	}
}



