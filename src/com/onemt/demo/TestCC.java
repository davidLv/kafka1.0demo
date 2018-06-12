package com.onemt.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestCC {

	
	public static void main(String[] args) {
//		System.setProperty("java.security.auth.login.config", "C:\\Users\\Administrator\\Downloads\\kafka_server_jaas.conf");
//		System.setProperty("java.security.krb5.conf", "C:\\Users\\Administrator\\Downloads\\krb5.conf");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
		props.put("group.id","chengang_group1");
		props.put("enable.auto.commit", "false");
//		props.put("sasl.kerberos.service.name", "kafka");
//        props.put("sasl.mechanism", "GSSAPI");
//        props.put("security.protocol", "SASL_PLAINTEXT");
		// props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");//latest
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("demo_kafka_topic_1"));
		
		try {
			while (true) {
				try {
					Thread.sleep(1000);
					ConsumerRecords<String, String> records = consumer.poll(1);
					
					for (ConsumerRecord<String, String> consumerRecord : records) {
						
						
							System.out.println("topic=="+consumerRecord.topic()+"--offset=="+ consumerRecord.offset()+"--timestamp=="+consumerRecord.timestamp()+"---key=="+consumerRecord.key()+"--value=="+consumerRecord.value());
			        }
					
					consumer.commitSync();
					
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			consumer.close();
		}
		
	}

}
