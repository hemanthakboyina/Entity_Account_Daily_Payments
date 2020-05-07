package com.example;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class JoinTransactionParseMapper implements KeyValueMapper<String,String, KeyValue<String,Long>>{

	public KeyValue<String, Long> apply(String key, String value) {
		// TODO Auto-generated method stub
		
		String a[] = value.split(":");
		
		String temp = a[4]  +":" + a[2] + ":" + a[5] ;
		
		return new KeyValue<String, Long>(temp, Long.parseLong(a[1]));
		
	}

}