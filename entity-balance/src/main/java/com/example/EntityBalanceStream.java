package com.example;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class EntityBalanceStream {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "entity4");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://192.168.0.105:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> paymentsStream = builder.stream("payments");

		GlobalKTable<String, String> accountGlobalTable = builder.globalTable("accountlookup");

		KStream<String, String> entityBalanceJoined = paymentsStream.join(accountGlobalTable, (key, value) -> key,
				(payments, accountInfo) -> payments + ":" + accountInfo);

		KStream<String, Long> entityBalanceJoinParsed = entityBalanceJoined.map(new JoinTransactionParseMapper());

		KGroupedStream<String, Long> entityBalanceGroupedStream = entityBalanceJoinParsed
				.groupByKey(Grouped.with(Serdes.String(), /* key */
						Serdes.Long()));
		KTable<String, Long> entityBalance = entityBalanceGroupedStream.reduce(new EntityWiseBalanceReducer());
		
		KStream<String, Long> entituBalanceDateStream = entityBalance.toStream();
		
		KStream<String, String> entituBalanceDateStringValue = entituBalanceDateStream.mapValues(value -> String.valueOf(value));
		
		KStream<String, String> entityBalanceDateParsed = entituBalanceDateStringValue.map(new
				 EntityValuedateParseMapper());
		
		entityBalanceDateParsed.to("EntityDailyPayments");
		
		entityBalanceDateParsed.foreach((key, value) -> System.out.println(key + " " + value));
		
			
		 
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
