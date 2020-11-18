package com.quang.code.tutorial;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	public static void main(String[] args) {

		
	     Properties props = new Properties();
	     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

		
		try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {
			for(int i =0; i<= 100; i++) {
				Random random = new Random();
				int randomInteger = random.nextInt(100);
			    ProducerRecord<String, Integer> record = new ProducerRecord<>("WELCOME-TOPIC", Integer.toString(randomInteger), randomInteger);
			    producer.send(record);
			    System.out.println("Send record: " + record.toString());
			    TimeUnit.SECONDS.sleep(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
