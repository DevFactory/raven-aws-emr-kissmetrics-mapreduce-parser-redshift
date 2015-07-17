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

import java.util.HashMap;
import java.util.Map;

public class KeyValueCounter {
	HashMap<String, Integer> keyCounter = new HashMap<String, Integer>();
	HashMap<String, Integer> valueLength = new HashMap<String, Integer>();
	
	public void putKeyCounter(String key, int count){
		int currentCount = keyCounter.containsKey(key) ? keyCounter.get(key) : 0;
		keyCounter.put(key, count + currentCount);		
	}
	
	public void incrementKeyCounter(String key){
		int currentCount = keyCounter.containsKey(key) ? keyCounter.get(key) : 0;
		keyCounter.put(key, currentCount + 1);		
	}
	
	public void putValueLength(String key, int length){		
		int currentLength = (Integer) (valueLength.containsKey(key) ? valueLength.get(key) : 0);
		valueLength.put(key, length > currentLength ? length : currentLength);		
	}
	
	/***
	 * Deep copy function to add the keyCounter values to the existing HashMap
	 * 
	 * @param newMapToAdd the values to be added to the existing HashMap
	 * @return
	 */
	public HashMap<String, Integer> deepMergeHashMapsAddition(HashMap<String, Integer> newMapToAdd) {
		
		for (Map.Entry<String, Integer> newEntry : newMapToAdd.entrySet())
		{ 
			int count = (Integer) (keyCounter.containsKey(newEntry.getKey()) ? keyCounter.get(newEntry.getKey()) : 0);
			keyCounter.put(newEntry.getKey(), count + newEntry.getValue());
		} 
		return keyCounter;
	}
	
	/*****
	 * Deep copy function to record the maximum length between two of the HashMaps for all keys
	 * 
	 * @param newMapToAdd the values to be added to the existing HashMap if they are larger
	 * @return
	 */
	public HashMap<String, Integer> deepMergeHashMapsMaxium(HashMap<String, Integer> newMapToAdd) {
	 
		for (Map.Entry<String, Integer> newEntry : newMapToAdd.entrySet())
		{ 
			int count = (Integer) (valueLength.containsKey(newEntry.getKey()) ? valueLength.get(newEntry.getKey()) : 0);
			valueLength.put(newEntry.getKey(), count > newEntry.getValue() ? count : newEntry.getValue());
		} 
		return valueLength;
		
	}
	
}
