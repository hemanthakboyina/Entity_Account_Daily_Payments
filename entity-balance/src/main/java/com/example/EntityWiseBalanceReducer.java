package com.example;

import org.apache.kafka.streams.kstream.Reducer;

public class EntityWiseBalanceReducer implements Reducer<Long> {

	@Override
	public Long apply(Long value1, Long value2) {
		// TODO Auto-generated method stub
		return value1 + value2;
	}

}
