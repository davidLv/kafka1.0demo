package com.onemt.demo;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.common.AuthorizationException;
import kafka.common.KafkaException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class TransactionsPC {

	//To use the transactional producer and the attendant APIs, you must set the transactional.id configuration property.
	//If the transactional.id is set, idempotence is automatically enabled along with the producer configs which idempotence depends on. 
	//Further, topics which are included in transactions should be configured for durability. 
	//In particular, the replication.factor should be at least 3, and the min.insync.replicas for these topics should be set to 2. 
	//Finally, in order for transactional guarantees to be realized from end-to-end, 
	//the consumers must be configured to read only committed messages as well.
	public static void main(String[] args) throws IOException{

		 //Producer写入的数据Topic以及记录Comsumer Offset的Topic会被写入相同的Transactin Marker，所以这一组读操作与写操作要么全部COMMIT要么全部ABORT。
		 //这意味着消息格式数据会多出一条对用户不可见的记录。可以在kafka—manager上看出一个topic每个分区都会多出一个offset
	
		 String consumerGroup="cg3";
		 String topic = "dpi_for_skw12";
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
		 props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		 props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		
		 props.put("acks", "all");
		 props.put("retries", Integer.MAX_VALUE);
		 //max.in.flight.requests.per.connection <=5
//		 props.put("max.in.flight.requests.per.connection", 3);
//		 props.put("enable.idempotence","true");
		
		 //transcational.id
		 props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"20");
		 
		 Producer<String, String> producer = new KafkaProducer<String, String>(props,new StringSerializer(),new StringSerializer());
		 
		 props.put("enable.auto.commit", "false");
		 props.put("auto.offset.reset", "earliest");//latest
		 props.put("group.id",consumerGroup);
		 Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		 
		 consumer.subscribe(Arrays.asList("dpi_for_skw11"));
		
		 // 初始化事务，包括结束该Transaction ID对应的未完成的事务（如果有）
		// 保证新的事务在一个正确的状态下启动
		 producer.initTransactions();

		 try {
			 while(true){
				
				 // 消费数据
				 ConsumerRecords<String, String> records = consumer.poll(100);
				 
				 if(!records.isEmpty()){
					 // 开始事务
					 producer.beginTransaction();
					 
					 Map<TopicPartition,OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
					 
					 for (ConsumerRecord<String, String> consumerRecord : records) {
						 	
						 	TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
						 	//记得offset+1
						 	OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset()+1);
						 	
						 	offsets.put(topicPartition, offsetAndMetadata);
							
						 	System.out.println("topic=="+consumerRecord.topic()+"--offset=="+ consumerRecord.offset()+"--partition=="+consumerRecord.partition()+"---key=="+consumerRecord.key()+"--value=="+consumerRecord.value());
							
							producer.send(new ProducerRecord<String, String>(topic, consumerRecord.key(), consumerRecord.value()));
					 }
					
					 producer.sendOffsetsToTransaction(offsets,consumerGroup);
					 // 数据发送及Offset发送均成功的情况下，提交事务
					 producer.commitTransaction();
				 }
			
			 }
			
				
		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			 // We can't recover from these exceptions, so our only option is to close the producer and exit.
		     producer.close();
		 } catch (KafkaException e) {
		     // For all other exceptions, just abort the transaction and try again.
		     producer.abortTransaction();
		}finally{
			producer.close();
			consumer.close();
		}
		 
		System.out.println("success!!!!");

		
	}


}
