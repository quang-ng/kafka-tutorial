package com.quang.code.tutorial;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	public static void main(String[] args) {
		Properties kafkaProps = new Properties();
		
		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "GROUP-ID-QUANG");
		kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		
		KafkaConsumer<String, Integer> consumer = new KafkaConsumer<String, Integer>(kafkaProps);
		consumer.subscribe(Collections.singletonList("WELCOME-TOPIC"));
		
		try {
			while(true) {
				ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, Integer> record: records) {
					System.out.printf("offset = %d, key = %s, value = %s\n",
					        record.offset(), record.key(), record.value());
				}
			}
		} finally {
			consumer.close();
		}
	}

}
