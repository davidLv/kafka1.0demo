package com.onemt.test;

import java.util.Properties;

public class KafkaMain {

	public static void main(String[] args) {
		
		String topic = "cgtest";
		Properties props = new Properties();
		props.put("bootstrap.servers","47.254.81.23:9092,47.254.68.76:9092");
		long sleeptime = Long.valueOf(1000);
		KafkaP kafkap = new KafkaP(topic,props,sleeptime);
		kafkap.start();
		props.put("group.id","4");
		KafkaC kafkac = new KafkaC(topic,props);
		kafkac.start();

	}

}
