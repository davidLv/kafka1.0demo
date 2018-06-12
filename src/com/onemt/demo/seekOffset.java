package com.onemt.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class seekOffset {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "ger-frankfurt-kafka-core-001:9092,ger-frankfurt-kafka-core-002:9092");
		props.put("group.id","chengang_group1");
		props.put("enable.auto.commit", "false");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		TopicPartition topicPartition = new TopicPartition("cgtest", 0);
		consumer.assign(Arrays.asList(topicPartition));
		//指定起始offset
		consumer.seek(topicPartition, 999594);
		int i = 0;
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				//只取100条记录
				if(i>=100){
					break;
				}
				i++;
				System.out.println("offset=="+record.offset()+",value=="+record.value());
			}
			if(i>=100){
				break;
			}
		}
		consumer.close();
	}
}
