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

public class test {
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
        
        KStream<String, String> map = textLines.map(new KeyValueMapper<String, String, KeyValue<? extends String,? extends String>>() {

			@Override
			public KeyValue<? extends String, ? extends String> apply(String key,String value) {
				
				if( key != null && !"null".equals(key) && Integer.valueOf(key)>=90){
					
					return new KeyValue<String, String>(value, value+"---cg");
				}else{
					
					return new KeyValue<String, String>(null, null);
				}
				
			}
		}).filter(new Predicate<String, String>() {

			@Override
			public boolean test(String key, String value) {
				
				return key != null;
			}
		});
        
        map.foreach(new ForeachAction<String, String>() {

			@Override
			public void apply(String key, String value) {

				System.out.println("key2=="+key+"--value2=="+value);
			}
		});
        
        KGroupedStream<String, String> groupByKey = map.groupByKey();
      
        KTable<String, Long> kTable = groupByKey.count();
        
        kTable.toStream().to("WordsWithCountsTopics", Produced.with(Serdes.String(), Serdes.Long()));
        
        //through的作用是用topic中取出来,结果跟topic的分区数有关.具体区别请接下去看
        KStream<String, Long> through = kTable.toStream().through("WordsWithCountsTopics",Produced.with(Serdes.String(), Serdes.Long()));
        
        KStream<String, Long> map2 = through.map(new KeyValueMapper<String, Long, KeyValue<? extends String,? extends Long>>() {

			@Override
			public KeyValue<? extends String, ? extends Long> apply(
					String key, Long value) {
				
				return new KeyValue<String, Long>(key+"---cg",value);
			}
		});
        
        map2.foreach(new ForeachAction<String, Long>() {

			@Override
			public void apply(String key, Long value) {
//				key3==q,w,e---cg---value3==210
//				key3==q,w,e---cg---value3==210
				System.out.println("key3=="+key+"---value3=="+value);
			}
		});
        
        KStream<String, Long> map3 = kTable.toStream().map(new KeyValueMapper<String, Long, KeyValue<? extends String,? extends Long>>() {

			@Override
			public KeyValue<? extends String, ? extends Long> apply(
					String key, Long value) {
				
				return new KeyValue<String, Long>(key+"---cg",value);
			}
		});
        
        map3.foreach(new ForeachAction<String, Long>() {

			@Override
			public void apply(String key, Long value) {
//				key4==q,w,e---cg---value4==210
				System.out.println("key4=="+key+"---value4=="+value);
			}
		});
        
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
