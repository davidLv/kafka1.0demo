package com.onemt.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class OffsetMonitor {
	
	public static Properties createProperties(String groupID,String bootstrap_server) {
		Properties props = new Properties();
		props.put("group.id", groupID);
		props.put("bootstrap.servers", bootstrap_server);
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

		return props;
	}
	
	public static void main(String[] args) {
		Map<Integer,Long> endOffset=new HashMap<Integer,Long>();
		Map<Integer,Long> commitOffset=new HashMap<Integer,Long>();
		String topic = "100000168_contentEnter"; 
		String groupID= "cgkafka";
		String bootstrap_server= "kafka1:9092,kafka2:9092,kafka3:9092";
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createProperties(groupID,bootstrap_server));
		List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
		List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
		for (PartitionInfo partitionInfo : partitionsFor) {
			TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
			topicPartitions.add(topicPartition);
		}
		
		//Get the last offset for the given partitions.
		Map<TopicPartition,Long> endOffsets = consumer.endOffsets(topicPartitions);
		
		for (TopicPartition partitionInfo : endOffsets.keySet()) {
			endOffset.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
		}
		
		
		for (TopicPartition topicAndPartition : topicPartitions) {
			
			//Get the last committed offset for the given partition (whether the commit happened by this process or another).
			OffsetAndMetadata committed = consumer.committed(topicAndPartition);
			
			commitOffset.put(topicAndPartition.partition(), committed.offset());
		}
		long sum=0l;
		if(endOffset.size()==commitOffset.size()){
			for (Integer partition : endOffset.keySet()) {
				long endof = endOffset.get(partition);
				long commitof = commitOffset.get(partition);
				long difOffset = endof - commitof;
				sum += difOffset;
				System.out.println("Topic:"+topic+" groupID:"+groupID+" partition:"+partition+" endOffset:"+endof+" commitOffset:"+commitof+" difOffset:"+difOffset);
			}
			System.out.println("Topic:"+topic+" groupID:"+groupID+" LAG:"+sum);
		}else{
			System.out.println("this topic partitions lost");
		}

		consumer.close();
	}
}
