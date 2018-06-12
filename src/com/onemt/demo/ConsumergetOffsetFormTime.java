package com.onemt.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * 手动控制偏移量
 */
public class ConsumergetOffsetFormTime extends Thread{
//	 private final AtomicBoolean closed = new AtomicBoolean(false);
	 private KafkaConsumer<String, String> consumer;
	 static long startingOffset = 3;
	
	 private Properties props;
	 private String topic;
	 private long thread_id;
//	 private int recvCnt = 0;
	 private long recCnt=0;
	 
	 public ConsumergetOffsetFormTime(Properties props, String topic) {
		this.props =props;
		this.topic = topic;
	}
	 public ConsumergetOffsetFormTime(Properties props,long recCnt, String topic,long thread_id) {
		this.props =props;
		this.recCnt = recCnt;
		this.topic = topic;
		this.thread_id = thread_id;
	}
	@Override
	public void run() {
		consumers();
	}
	public static Properties createProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.8.243:9092,192.168.8.244:9092,192.168.8.246:9092");
		props.put("group.id", "cgkafka2");
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
		 
//		 consumer.subscribe(Arrays.asList(topic));
		 //从指定时间戳开始消费
		  Map<TopicPartition, Long> offsetFormTime = getOffsetFormTime(1482892499146L,consumer);
		 
//		  List<TopicPartition> partitions = new ArrayList<TopicPartition>();  
//	    	List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
//	    	for (PartitionInfo partitionInfo : partitionsFor) {
//	    		TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
//	    		partitions.add(topicPartition);
//			}
//		  consumer.assign(partitions);
		  
		 for (TopicPartition topicPartition : offsetFormTime.keySet()) {
			 
			 AtomicBoolean closed = new AtomicBoolean(false);
			 int recvCnt = 0;
			 
			 consumer.assign(Arrays.asList(topicPartition));
			 
			 consumer.seek(topicPartition, offsetFormTime.get(topicPartition));
				try {
					 while (!closed.get()) {
						 //poll()方法的参数控制当Consumer在当前Position等待记录时，它将阻塞的最大时长。当有记录到来时，Consumer将会立即返回。但是，在返回前如果没有任何记录到来，Consumer将等待直到超出指定的等待时长。
					     ConsumerRecords<String, String> records = consumer.poll(50);//等待消息的时间,并不是一条一条数据获取。

					     for (ConsumerRecord<String, String> record : records){
					    	 recvCnt+=1;
					    	 System.out.printf("thread_id=%d,partition = %d, offset = %d, key = %s, value = %s,timestamp = %d", thread_id,record.partition(),record.offset(), record.key(), record.value(),record.timestamp());
						     System.out.println("\n"+recvCnt);
						     
						     
						     if(recvCnt >= recCnt){
					    		 System.out.println("recvCnt=="+recvCnt);
					    		 try {
					    			 consumer.commitSync();
								} catch (Exception e) {
									// TODO: handle exception
								}finally{
									closed.set(true); 
								}
							 	break;	
							 }
					     }
					     
					 }
				} catch (WakeupException e) {
					if (!closed.get()) throw e;
				}
//				finally{
//					consumer.close();
//				}
		 }
		 consumer.close();
		 //Kafka允许使用seek（TopicPartition，long）来指定新位置。
		 //用于寻找服务器维护的最早和最新偏移的特殊方法也是可用的（分别是seekToBeginning（Collection）和seekToEnd（Collection））。
	}
    
    public Map<TopicPartition, Long> getOffsetFormTime(Long time,KafkaConsumer<String, String> consumer){
    	Map<TopicPartition, Long> resultMap = new HashMap<TopicPartition, Long>();
//    	consumer = new KafkaConsumer<String, String>(props);
//    	consumer.subscribe(Arrays.asList(topic));

    	Map<TopicPartition, Long> map = new HashMap<TopicPartition, Long>();
    	Long timestamp =time; //1482892499146L
    	List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
    	for (PartitionInfo partitionInfo : partitionsFor) {
    		TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    		map.put(topicPartition, timestamp);
		}

    	Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(map);
    	for (TopicPartition topicPartition : offsetsForTimes.keySet()) {
    		long offsetNew = offsetsForTimes.get(topicPartition).offset();
    		
    		resultMap.put(topicPartition, offsetNew);
    		System.out.println("partition==="+topicPartition.partition()+"----offset==="+offsetNew);
		}
    	return resultMap;
    }
    
    public void shutdown() {
    	System.out.println("11111111111111111111111");

        consumer.wakeup();
    }
	
    public static void main(String[] args) {
		Properties props = ConsumergetOffsetFormTime.createProperties();
		String topic ="cgtest";

		final long threads = 1;
		long numrecords = 10;
		 final List<ConsumergetOffsetFormTime> consumers = new ArrayList<ConsumergetOffsetFormTime>();
		for (int i = 0; i < threads; i++) {
			ConsumergetOffsetFormTime consumerDemo = new ConsumergetOffsetFormTime(props,numrecords/threads,topic,i);
			consumerDemo.start();
			
			consumers.add(consumerDemo);
			
		}
		Runtime.getRuntime().addShutdownHook(new Thread(){
			 @Override
			public void run() {
				for (ConsumergetOffsetFormTime thread : consumers) {
					thread.shutdown();
				}
			}
		 });
	}

}
