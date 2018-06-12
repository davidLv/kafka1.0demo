package com.onemt.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.cluster.Broker;
import kafka.cluster.Cluster;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import com.sun.xml.internal.ws.api.addressing.WSEndpointReference.Metadata;


public class TestPP {

	public static void main(String[] args) throws Exception {

		 String topic = "demo_kafka_topic_1";
		 Properties props = new Properties();
//		 props.put("sasl.kerberos.service.name", "kafka");
//         props.put("sasl.mechanism", "GSSAPI");
//         props.put("security.protocol", "SASL_PLAINTEXT");
		 props.put("bootstrap.servers", "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		 //拦截器
//		 List<String> interceptors = new ArrayList<String>();
//		 interceptors.add("com.onemt.test.TimeStampPrependerInterceptorss");
//		 props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


		 KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
//		 List<PartitionInfo> partitionsFor = producer.partitionsFor(topic);
//		 for (PartitionInfo partitionInfo : partitionsFor) {
//			System.out.println("partition==" + partitionInfo.partition()
//					+ "--id==" + partitionInfo.leader().id() + "--replicas=="
//					+ partitionInfo.replicas().length +"--inSyncReplicas=="+partitionInfo.inSyncReplicas() );
//			Node[] inSyncReplicas = partitionInfo.inSyncReplicas();
//			for (Node node : inSyncReplicas) {
//				System.out.println(""+node.host()+":"+node.port()+":"+node.toString());
//			}
//		}
		for (int i = 0; i < 100000; i++)
	         producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i),"qqqqq"),new DemoCallBack(Integer.toString(i),"qqqqq"));

//		 File file = new File("C:\\Users\\Administrator\\Desktop\\AB.txt");  
//		 BufferedReader reader = new BufferedReader(new FileReader(file));
//			
//		 String readLine = reader.readLine();
//		 
//		 while(readLine != null){
//				
//			String[] content = readLine.split("=");
//			System.out.println("key=="+content[0].hashCode()%2+"---value=="+content[1]);
//			producer.send(new ProducerRecord<String, String>(topic, content[0], content[1]));
//			
//			readLine = reader.readLine();
//		}
		System.out.println("success!!!!");

		producer.close();
	}

}

class DemoCallBack implements Callback {
	public  static Logger logger = Logger.getLogger(DemoCallBack.class);
    private final String key;
    private final String message;

    public DemoCallBack(String key, String message) {

        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        if (metadata != null) {
        	System.out.println("key=="+key+"--offset=="+metadata.offset()+"--timestamp"+metadata.timestamp()+"--time=="+System.currentTimeMillis());
        	logger.info(
                "message(" + key + ", " + message +") sent to partition(" + metadata.partition() +"), " +
                    "offset(" + metadata.offset() + ") in ");
        } else {
        	logger.error("topic cgtest producer is fail metadata" ,exception);
        }
        if(exception != null){
        	logger.error("topic cgtest producer is fail" ,exception);
        }
    }
}
