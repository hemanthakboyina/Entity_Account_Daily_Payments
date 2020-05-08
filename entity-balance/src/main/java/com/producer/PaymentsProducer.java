package com.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PaymentsProducer {
 
 public static void main(String[] args) throws Exception{
    
    String paymentsTopic = "payments";
    
    
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.0.105:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", 
       "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", 
       "org.apache.kafka.common.serialization.StringSerializer");
    
    Producer<String, String> producer = new KafkaProducer
       <String, String>(props);
          
		
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "hemanth", "hemanth:300:04/05/2020")).get();
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "hemanth", "hemanth:500:05/05/2020")).get();
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "hemanth", "hemanth:400:06/05/2020")).get();
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "hemanth", "hemanth:300:06/05/2020")).get();
	
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "kumar", "kumar:100:03/05/2020")).get();
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "kumar", "kumar:200:04/05/2020")).get();
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "kumar", "kumar:250:04/05/2020")).get();
	producer.send(new ProducerRecord<String, String>(paymentsTopic, "kumar", "kumar:200:03/05/2020")).get();
	
	System.out.println("Message sent successfully");
             producer.close();
 }
}