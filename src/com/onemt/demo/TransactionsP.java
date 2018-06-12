package com.onemt.demo;

import java.io.IOException;
import java.util.Properties;

import kafka.common.AuthorizationException;
import kafka.common.KafkaException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class TransactionsP {

	//To use the transactional producer and the attendant APIs, you must set the transactional.id configuration property.
	//If the transactional.id is set, idempotence is automatically enabled along with the producer configs which idempotence depends on. 
	//Further, topics which are included in transactions should be configured for durability. 
	//In particular, the replication.factor should be at least 3, and the min.insync.replicas for these topics should be set to 2. 
	//Finally, in order for transactional guarantees to be realized from end-to-end, 
	//the consumers must be configured to read only committed messages as well.
	public static void main(String[] args) throws IOException{

		//只要是通过事务发送的，就会在每个分区多加一条记录。
		 String topic = "dpi_for_skw14";
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
//		 props.put("acks", "all");
//		 props.put("retries", Integer.MAX_VALUE);
		 //max.in.flight.requests.per.connection <=5
//		 props.put("max.in.flight.requests.per.connection", 3);
//		 props.put("enable.idempotence","true");
		
		 //transcational.id
		 props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"34");
		 
		 Producer<String, String> producer = new KafkaProducer<String, String>(props,new StringSerializer(),new StringSerializer());

		 producer.initTransactions();

		 int i = 0;
		 try {
			 producer.beginTransaction();
			 
//			 for (int i = 0; i < 100; i++)
//		         producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
			 while(i<10000){
					
					producer.send(new ProducerRecord<String, String>(topic, Integer.toString(1), Integer.toString(1)));
					
					if(i%100==0 && i != 0){
						
//						System.out.println("i2=="+i);
//						
//						producer.commitTransaction();
						
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
//						producer.beginTransaction();
					
					}
					
					i++;
			}
			System.out.println("i2=="+i);
				
			producer.commitTransaction();
				
		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			 // We can't recover from these exceptions, so our only option is to close the producer and exit.
		     producer.close();
		 } catch (KafkaException e) {
		     // For all other exceptions, just abort the transaction and try again.
		     producer.abortTransaction();
		}
		 
		System.out.println("success!!!!");

		producer.close();
	}


}
