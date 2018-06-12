package com.onemt.tools;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

/**
 * Java API获取topic所占磁盘空间(Kafka 1.0.0)
 * 统计的是 ****.log 文件大小
 * @author Administrator
 *
 */
public class TopicDiskSizeSummary {

	private static AdminClient admin;
	
	 private static void initialize(String bootstrapServers) {
	        Properties props = new Properties();
	        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	        admin = AdminClient.create(props);
	    }
	
	   public static long getTopicDiskSizeForSomeBroker(String topic, int brokerID) throws ExecutionException, InterruptedException {
	        
		   long sum = 0;
	        
	        DescribeLogDirsResult ret = admin.describeLogDirs(Collections.singletonList(brokerID));
	        
	        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> tmp = ret.all().get();
	        
	        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> entry : tmp.entrySet()) {
	           
	        	Map<String, DescribeLogDirsResponse.LogDirInfo> tmp1 = entry.getValue();
	            
	        	for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> entry1 : tmp1.entrySet()) {
	            
	        		DescribeLogDirsResponse.LogDirInfo info = entry1.getValue();
	                
	        		Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoMap = info.replicaInfos;
	                
	        		for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicas : replicaInfoMap.entrySet()) {
	        			
	                    if (topic.equals(replicas.getKey().topic())) {
	                    
	                    	sum += replicas.getValue().size;
	                    
	                    }
	                }
	            }
	        }
	        return sum;
	    }
	 
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		
		String topic = "cgtest";
		
		String brokers = "10.0.0.65:9092,10.0.0.66:9092";
		
		initialize(brokers);
		
		long topic1InBroker65 = getTopicDiskSizeForSomeBroker(topic, 65);
		
		System.out.println("topicInBroker65=="+topic1InBroker65);
		
	}
}
