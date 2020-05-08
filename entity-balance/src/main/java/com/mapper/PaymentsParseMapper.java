package com.mapper;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class PaymentsParseMapper implements KeyValueMapper<String,String, KeyValue<String,Long>>{

	public KeyValue<String, Long> apply(String key, String value) {
		// TODO Auto-generated method stub
		
		String a[] = value.split(":");
		
		//System.out.println(a[0] + " " + a[1]);
		
		String temp = a[0] +":" + a[2];
		
		return new KeyValue<String, Long>(temp, Long.parseLong(a[1]));
		
		
		
	}

}
