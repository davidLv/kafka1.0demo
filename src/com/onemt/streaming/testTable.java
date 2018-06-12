package com.onemt.streaming;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

public class testTable {
	public static void main(String[] args) {
		
        Properties config = new Properties();
        //Kafka Stream将APPLICATION_ID_CONFI作为隐式启动的Consumer的Group ID
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-test-cg20");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 3);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        config.put("consumer." + ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
//        config.put("producer." + ProducerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
        StreamsBuilder builder = new StreamsBuilder();
        
        KTable<String, String> kTable = builder.table("TextLinesTopics");
        KGroupedTable<String, String> groupBy = kTable.groupBy(new KeyValueMapper<String, String, KeyValue<String,String>>() {

			@Override
			public KeyValue<String, String> apply(String key, String value) {
				// TODO Auto-generated method stub
				return new KeyValue<String, String>(value, value);
			}
		});
        KTable<String, Long> count = groupBy.count();
        KTable<String, Long> kTable2 = count.filter(new Predicate<String, Long>() {

			@Override
			public boolean test(String key, Long count) {
				// TODO Auto-generated method stub
				return count >= 2;
			}
		});
        kTable2.toStream().filter(new Predicate<String, Long>() {

			@Override
			public boolean test(String key, Long value) {
				// TODO Auto-generated method stub
				return value != null;
			}
		}).foreach(new ForeachAction<String, Long>() {

			@Override
			public void apply(String key, Long value) {

				System.out.println("key=="+key+"--value=="+value);
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
