package com.onemt.streaming;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCountApplication {

//	Kafka Stream的并行模型完全基于Kafka的分区机制和Rebalance机制，实现了在线动态调整并行度
//	同一Task包含了一个子Topology的所有Processor，使得所有处理逻辑都在同一线程内完成，避免了不必的网络通信开销，从而提高了效率。
//	through方法提供了类似Spark的Shuffle机制，为使用不同分区策略的数据提供了Join的可能
//	log compact提高了基于Kafka的state store的加载效率
//	state store为状态计算提供了可能
//	基于offset的计算进度管理以及基于state store的中间状态管理为发生Consumer rebalance或Failover时从断点处继续处理提供了可能，并为系统容错性提供了保障
//	KTable的引入，使得聚合计算拥用了处理乱序问题的能力
	
	public static void main(final String[] args) throws Exception {
        Properties config = new Properties();
        //Kafka Stream将APPLICATION_ID_CONFI作为隐式启动的Consumer的Group ID
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-applicationsssss");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.65:9092,10.0.0.66:9092,10.0.0.22:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
 
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> textLines = builder.stream("TextLinesTopics");
//        KTable<String, Long> wordCounts = textLines
//            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
//            .groupBy((key, word) -> word)
//            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));
 
        KStream<String, String> flatMapValues = textLines.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(",")));
//        textLines.flatMapValues(new ValueMapper<String, Iterable<? extends String>>() {
//
//			@Override
//			public Iterable<? extends String> apply(String textLine) {
//				
//				return Arrays.asList(textLine.toLowerCase().split(","));
//			}
//		});
        KStream<String, String> mapValues = flatMapValues.mapValues(new ValueMapper<String,String>() {

			@Override
			public String apply(String value) {

				return value+"--cg";
			}
		});
        KStream<String, String> kStreams = mapValues.filter(new Predicate<String, String>() {

			@Override
			public boolean test(String key, String value) {
				
				return key != null && Integer.valueOf(key) > 90;
			}
		});

        //返回的是<v,value>
        KGroupedStream<String, String> groupBy = kStreams.groupBy(new KeyValueMapper<String,String,String>() {
			@Override
			public String apply(String key, String v) {
				
				return v;
			}
		});
        
        //不设置 state store。程序会默认设置个state store：KSTREAM-REDUCE-STATE-STORE-0000000005
        KTable<String, String> reduce = groupBy.reduce(new Reducer<String>() {
			
			@Override
			public String apply(String v1, String v2) {
				
				return v1+","+v2;
			}
		});
        reduce.toStream().foreach(new ForeachAction<String, String>() {

			@Override
			public void apply(String k, String v) {
				// TODO Auto-generated method stub
				System.out.println("k=="+k+"--v=="+v);
			}
		});
        
        // Materialize the result into a KeyValueStore named "counts-store".
        // The Materialized store is always of type <Bytes, byte[]> as this is the format of the inner most store
//        Kafka Streams 确保对于local state stores失败情况也具有健壮性。对于每个state store，保持一个可复制的changelog Kafka topic用于跟踪state的任何变更。
//        这些changelog topic同样是被分区的.在changelog topic中会开启日志压缩，比较老的数据可以被安全的清理，防止topic的无限制的增长。
//        如果运行在一台主机上的task失败了，并且在另一台机器上被重新启动，Kafka Streams可以确保恢复他们相关联的state stores到失败之前的内容，通过在新启动的task恢复处理之前重播对应的changelog topic。
//        因此，失败处理对于最终用户来说是完全透明的
        //topic会多出两个
        //wordcount-applicationsssss-cg_sotre-repartition:该topic保存的是groupBy后的key-value对
        //wordcount-applicationsssss-cg_sotre-changelog:该topic保存的是所有count后的结果
        KTable<String, Long> kTable = groupBy.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("cg_sotre"));
        
        // WordsWithCountsTopic --config cleanup.policy=compact  
        // WordsWithCountsTopics过期策略是压缩
        kTable.toStream().to("WordsWithCountsTopics", Produced.with(Serdes.String(), Serdes.Long()));
        
        kTable.toStream().foreach(new ForeachAction<String, Long>() {

			@Override
			public void apply(String word, Long count) {
				
				System.out.println("word=="+word+"--count=="+count);
				
			}
		});
       
        System.out.println(".......................");
        
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
