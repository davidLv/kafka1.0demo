package com.onemt.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


/**
 * Kafka Java consumer动态修改topic订阅
 * @author cg
 *
 */
public class ConsumerDemo3 {
	
	public static void main(String[] args) {
		
		  String filename = "C:/Users/Administrator/Desktop/ns_abtes_deviceid.txt";
		  
		  ConcurrentLinkedQueue<String> subscribedTopics = new ConcurrentLinkedQueue<>();
		 
		  ConsumerDemo3Thread consumerDemo3Thread = new ConsumerDemo3Thread(subscribedTopics,filename);
		  
		  consumerDemo3Thread.start();
	 
	        Properties props = new Properties();
	        props.put("bootstrap.servers", "10.0.0.65:9092");
	        props.put("group.id", "my-group2");
	        props.put("auto.offset.reset", "earliest");
	        props.put("enable.auto.commit", "false");
	        props.put("auto.commit.interval.ms", "1000");
	        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	        
	        //最开始的订阅列表：atopic、btopic
	        consumer.subscribe(Arrays.asList("dpi_for_skw"));
	        while (true) {
	            ConsumerRecords<String,String> records = consumer.poll(1000); //表示每2秒consumer就有机会去轮询一下订阅状态是否需要变更
	            for (ConsumerRecord<String, String> consumerRecord : records) {
					System.out.println("topic=="+consumerRecord.topic()+"--offset=="+ consumerRecord.offset()+"--partition=="+consumerRecord.partition());
				}
	            consumer.commitSync();
	            try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	            if (!subscribedTopics.isEmpty()) {
	                Iterator<String> iter = subscribedTopics.iterator();
	                List<String> topics = new ArrayList<String>();
	                while (iter.hasNext()) {
	                    topics.add(iter.next());
	                }
	                subscribedTopics.clear();
	                consumer.subscribe(topics); // 重新订阅topic
	                //consumer.subscribe(topics,getConsumerRebalanceListener(consumer));
	            }
	        }
	        // 本例只是测试之用，使用了while(true)，所以这里没有显式关闭consumer
//	        consumer.close();
	}
    private static ConsumerRebalanceListener getConsumerRebalanceListener(final KafkaConsumer<String, String> consumer) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // set initial position so we don't need a lookup
                for (TopicPartition partition : partitions){
                	//consumer.assign(Arrays.asList(partition));
                	//Kafka允许使用seek（TopicPartition，long）来指定新位置。有关如何确定新位置可以参考ConsumerDemo2
                    consumer.seek(partition, 0);
                }
                	
            }
        };
    }

}
