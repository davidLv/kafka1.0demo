package com.onemt.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

import scala.collection.JavaConversions;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;

public class GetAllGroupsForTopic {
	
	//根据topic查找存储在kafka中的groupid

	public static void main(String[] args) {

		String brokerListUrl = "10.0.0.65:9092,10.0.0.66:9092";
		
		String topic = "cgtest";
		
		AdminClient client = AdminClient.createSimplePlaintext(brokerListUrl);
		
		List<GroupOverview> allGroups = JavaConversions.seqAsJavaList(client.listAllGroupsFlattened().toSeq());
		
		Set<String> groups = new HashSet<String>();
		
		for (GroupOverview overview : allGroups) {
			
			String groupID = overview.groupId();
			
			Map<TopicPartition, Object> offsets = JavaConversions.mapAsJavaMap(client.listGroupOffsets(groupID));
			
			Set<TopicPartition> partitions = offsets.keySet();
			
			for (TopicPartition tp: partitions) {
				
                if (tp.topic().equals(topic)) {
                
                	groups.add(groupID);
                
                }
            }
			
		}
		
		client.close();
		
		for (String group : groups) {
			
			System.out.println("group=="+group);
		}
	}

}
