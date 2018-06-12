package com.onemt.streaming;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class test2 {
	public static void main(String[] args) {
		
        Properties config = new Properties();
        //Kafka Stream将APPLICATION_ID_CONFI作为隐式启动的Consumer的Group ID
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-test-cg5");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 3);
//        config.put("consumer." + ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
//        config.put("producer." + ProducerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> textLines = builder.stream("TextLinesTopics");
        //用于修改key值
        KStream<String, String> selectKey = textLines.selectKey(new KeyValueMapper<String, String, String>() {

			@Override
			public String apply(String key, String value) {
//				if(Integer.valueOf(key)>90){
//					return key+".cg";
//				}else{
//					return "null";
//				}
				return "cg";
			}
		});
        KGroupedStream<String, String> groupByKey = selectKey.groupByKey();
        
         KTable<String, String> reduce = groupByKey.reduce(new Reducer<String>() {
			
			@Override
			public String apply(String value1, String value2) {
				
				if (Integer.parseInt(value1) > Integer.parseInt(value2))
                    return value1;
                else
                    return value2;
			}
		});
        reduce.toStream().foreach(new ForeachAction<String, String>() {

			@Override
			public void apply(String key, String value) {

				System.out.println("reduce=="+key+"--reduce=="+value);
			}
		});
//        KStream<String, String> filter = reduce.toStream().filter(new Predicate<String, String>() {
//
//			@Override
//			public boolean test(String key, String value) {
//				
//				return Integer.valueOf(value)> 100;
//			}
//		});
//        filter.print();
//        filter.foreach(new ForeachAction<String, String>() {
//
//			@Override
//			public void apply(String key, String value) {
//
//				System.out.println("filter=="+key+"--filter=="+value);
//			}
//		});
        
        System.out.println("--------------------");
        
        
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        CountDownLatch latch = new CountDownLatch(1);
        
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook"){
        	
        	public void run() {
        		streams.close(10, TimeUnit.SECONDS);
        		latch.countDown();
        	};
        });
        try {
        	 streams.start();
             latch.await();
		} catch (Exception e) {
			System.exit(1);
		}
        System.exit(0);
	}

}
