package com.onemt.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;


public class ConsumerDemo extends Thread{
	 private final AtomicBoolean closed = new AtomicBoolean(false);
	 private KafkaConsumer<String, String> consumer;
	 static long startingOffset = 3;
	
	 private Properties props;
	 private String topic;
	 private long thread_id;
	 private int recvCnt = 0;
	 private long recCnt=0;
	 
	 public ConsumerDemo(Properties props, String topic) {
		this.props =props;
		this.topic = topic;
	}
	 public ConsumerDemo(Properties props,long recCnt, String topic,long thread_id) {
		this.props =props;
		this.recCnt = recCnt;
		this.topic = topic;
		this.thread_id = thread_id;
		
	}
	@Override
	public void run() {
		consumers();
//		getOffsetFormTime(1482892499000L);
	}
	public static Properties createProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.8.243:9092,192.168.8.244:9092,192.168.8.246:9092");
		props.put("group.id", "cgkafka");
		props.put("enable.auto.commit", "false");
		// props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

		 props.put("auto.offset.reset", "earliest");//latest
		// session.timeout.ms //无法在session.timeout.ms的持续时间内发送心跳，则消费者将被视为死亡，并且其分区将被重新分配
		// props.put("max.poll.records", "500");//：使用此设置可限制从单个调用返回到轮询的总记录。这可以使预测在每个轮询间隔内必须处理的最大值更容易。通过调整此值，您可以减少轮询间隔，这将减少组重新平衡的影响。
		// max.poll.interval.ms：通过增加预期轮询之间的间隔，您可以为消费者提供更多时间来处理从poll（long）返回的一批记录
		
		return props;
	}

	/**
	 * 手动控制偏移量
	 */
	public void consumers() {
		System.out.println("begin to consumer thread_ID="+thread_id+"---recevice data count =="+recCnt);
		 consumer = new KafkaConsumer<String, String>(props);
		 consumer.subscribe(Arrays.asList(topic));
		 
//		  Map<TopicPartition, Long> offsetFormTime = getOffsetFormTime(1482892499146L,consumer);
//		
//		 
//		 for (TopicPartition topicPartition : offsetFormTime.keySet()) {
//			 System.out.println("22222222");
//			 consumer.assign(Arrays.asList(topicPartition));
//			 consumer.seek(topicPartition, offsetFormTime.get(topicPartition));
//		 }
		 //Kafka允许使用seek（TopicPartition，long）来指定新位置。
		 //用于寻找服务器维护的最早和最新偏移的特殊方法也是可用的（分别是seekToBeginning（Collection）和seekToEnd（Collection））。
		try {
			 while (!closed.get()) {
				 //poll()方法的参数控制当Consumer在当前Position等待记录时，它将阻塞的最大时长。当有记录到来时，Consumer将会立即返回。但是，在返回前如果没有任何记录到来，Consumer将等待直到超出指定的等待时长。
			     ConsumerRecords<String, String> records = consumer.poll(500);//等待消息的时间(读取超时时间为500ms) 如果没有数据就等待500ms。如果有就读取。

			     for (ConsumerRecord<String, String> record : records){
			    	 recvCnt+=1;
			    	
			    		 System.out.printf("thread_id=%d,partition = %d, offset = %d, key = %s, value = %s,timestamp = %d", thread_id,record.partition(),record.offset(), record.key(), record.value(),record.timestamp());
				         System.out.println("\n"+recvCnt);
			    	 
//			    	 if(recvCnt == recCnt){
//			    		 try {
//			    			 TopicPartition topicPartition = new TopicPartition(topic, record.partition());
			    			
//			    			 consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(record.offset() + 1)));
//						     consumer.commitAsync(new OffsetCommitCallback() {
//									
//									@Override
//									public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
//											Exception exception) {
//										if(exception == null){
//											
//											for (Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
//												
//												System.out.println("commit offset: partition ="+ offset.getKey()+", offset ="+offset.getValue().offset()); 
//											}
//										}else{
//											
//										}
//									}
//								});
//						} catch (Exception e) {
//							System.out.println("commit fail.........");
//						}finally{
//							closed.set(true);
//							
//						}
//			    		 break;
//			    	 }
			     }
			     
		    	 if(recvCnt >= recCnt){
		    		 try {
		    			 consumer.commitSync();
					} catch (CommitFailedException e) {
						System.out.println("CommitFailed !!!");
					}finally{
						closed.set(true); 
					}
				 		
				 }


//			     if(!records.isEmpty()){
//			    	try {
//					     consumer.commitAsync(new OffsetCommitCallback() {
//								
//								@Override
//								public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
//										Exception exception) {
//									// TODO Auto-generated method stub
//									for (Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
//										System.out.println("commit offset: partition ="+ offset.getKey()+", offset ="+offset.getValue().offset()); 
//									}
//								}
//							});
//					} catch (Exception e) {
//						System.out.println("commit fail.........");
//					}finally{
//					}
//
//			     }
			 }
		} catch (WakeupException e) {
			if (!closed.get()) throw e;
		}finally{
			consumer.close();
		}
	}
	
	/** 
     * 手工精确控制每个分区的偏移量 
     */  
    public  void consumerExactly() {
		 consumer = new KafkaConsumer<String, String>(props);
		 
		 consumer.subscribe(Arrays.asList(topic));
		
		 try {
			 while (!closed.get()) {
				 //超时为Long.MAX_VALUE，这基本上意味着Consumer将无期限地阻塞，直到下一条记录达到
			     ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			     for (TopicPartition partition : records.partitions()) {//遍历分区
			    	
			    	 List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
			    	 
			    	 for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {//分区中的数据
			    		 
			    		 System.out.printf("offset = %d, key = %s, value = %s,partition = %d,timestamp = %d", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value(),consumerRecord.partition(),consumerRecord.timestamp());
				         System.out.println("\n");
					 }
			    	 
			    	 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset(); //取得当前读取到的最后一条记录的offset
			    	
			    	
			    	 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));// 提交offset，记得要 + 1
			    	
			    	
				}
			 }
		} catch (WakeupException e) {
			if (!closed.get()) throw e;
		}finally{
			consumer.close();
		}

    }
    

    
    public void shutdown() {
    	System.out.println("11111111111111111111111");
        closed.set(true);
        consumer.wakeup();
    }
	
    public static void main(String[] args) {
		Properties props = ConsumerDemo.createProperties();
		String topic ="cgtest";

		final int threads = 1;
		long numrecords = 100;
		
		final ExecutorService executor = Executors.newFixedThreadPool(threads);
		final List<ConsumerDemo> consumers = new ArrayList<ConsumerDemo>();
		
		for (int i = 0; i < threads; i++) {
			ConsumerDemo consumerDemo = new ConsumerDemo(props,numrecords/threads,topic,i);
			consumers.add(consumerDemo);
			executor.submit(consumerDemo);		
			
		}
		Runtime.getRuntime().addShutdownHook(new Thread(){
			 @Override
			public void run() {
				for (ConsumerDemo thread : consumers) {
					thread.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		 });
	}

}
