package com.onemt.test;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TimeStampPrependerInterceptorss implements ProducerInterceptor<String, String>{

	@Override
	public void configure(Map<String, ?> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		//先于用户的callback，对ack响应进行预处理
		if(metadata != null){
			System.out.println("time=="+System.currentTimeMillis()+"--metadata=="+metadata.toString());
		}
	}

	@Override
	public ProducerRecord<String,String> onSend(ProducerRecord<String,String>  record) {
		//对消息进行拦截或修改
		return new ProducerRecord<String,String>(
                record.topic(), record.partition(), record.timestamp(), record.key(), System.currentTimeMillis() + "," + record.value().toString());
	}

}
