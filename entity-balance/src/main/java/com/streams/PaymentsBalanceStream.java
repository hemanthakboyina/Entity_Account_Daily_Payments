package com.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import com.mapper.PaymentsParseMapper;
import com.mapper.ValuedateParseMapper;
import com.reducer.DateWiseBalanceReducer;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import joptsimple.util.KeyValuePair;

public class PaymentsBalanceStream {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payments7");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://192.168.0.105:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> paymentsStream = builder.stream("payments");

		KStream<String, Long> paymentStreamParsed = paymentsStream.map(new
		 PaymentsParseMapper());

		 KGroupedStream<String, Long> paymentGroupedStream =  paymentStreamParsed.groupByKey(Grouped.with(
       	      Serdes.String(), /* key */
       	      Serdes.Long()) );
		KTable<String, Long> accountBalanceDate = paymentGroupedStream.reduce(new DateWiseBalanceReducer());

		
		KStream<String, Long> accountBalanceDateStream = accountBalanceDate.toStream();
		
		KStream<String, String> accountBalanceDateMapValues = accountBalanceDateStream.mapValues(value -> String.valueOf(value));
		
		KStream<String, String> accountBalanceDateParsed = accountBalanceDateMapValues.map(new
				 ValuedateParseMapper());
		
		accountBalanceDateParsed.to("AccountDailyPayments");

		accountBalanceDateParsed.foreach((key, value) -> System.out.println(key + " " + value));
		
		
		 
		final Topology topology = builder.build();

		System.out.println(topology.describe());

		final KafkaStreams streams = new KafkaStreams(topology, props);

		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);

	}

}
