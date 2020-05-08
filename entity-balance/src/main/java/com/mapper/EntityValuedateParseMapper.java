package com.mapper;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;



public class EntityValuedateParseMapper implements KeyValueMapper<String,String, KeyValue<String,String>>{

	public KeyValue<String, String> apply(String key, String value) {
		// TODO Auto-generated method stub
		
		String a[] = key.split(":");
		
		String temp = key +":" + value;
		 
		return new KeyValue<String, String>(a[0], temp );
	}
}