package com.onemt.test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.utils.ShutdownableThread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

public class KafkaC extends ShutdownableThread{
	private  final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private static Logger logger = Logger.getLogger(KafkaC.class);
	public KafkaC(String topic,Properties props) {
    	super("KafkaConsumerExample", false);
		
		props.put("enable.auto.commit", "false");
		//max.poll.interval.ms  300000
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");//latest
		consumer = new KafkaConsumer<Integer, String>(props);
		this.topic = topic;
	} 

	@Override
	public void doWork() {

		consumer.subscribe(Arrays.asList(topic));
		try {
			while (!closed.get()) {
				try {
					ConsumerRecords<Integer, String> records = consumer.poll(10);
					for (ConsumerRecord<Integer, String> record : records) {
						System.out.println("value=="+record.value());
						logger.info("Received message: (" + record.value() + ", " + ") at offset " + record.offset() +" in partition "+record.partition());
			        }
					consumer.commitSync();
				} catch(Exception e){
					logger.error("topic cgtest Consumer is fail");
				}
			}
		} catch (Exception e) {
			logger.error("topic cgtest Consumer is fail");
		}finally{
			consumer.close();
		}
		
	}

}
