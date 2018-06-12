package com.onemt.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class KafkaP extends Thread{

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private long sleeptime=5000;
    private static Logger logger = Logger.getLogger(KafkaP.class);
	public KafkaP(String topic,Properties props,long sleeptime) {
		 props.put("acks", "all");
		 props.put("retries", Integer.MAX_VALUE); //Ĭ��Ϊ0������0.���ܻ������Ϣ˳��һ�����⣬����Ϣ�����Ե�һ������
		 props.put("linger.ms", 1);//��Ϣ��С������£��������Ӹ�ֵ��Ĭ��Ϊ0
//		 List<String> interceptors = new ArrayList<String>();
//		 interceptors.add("test.TimeStampPrependerInterceptor");
//		 props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
		 
		 //max.block.ms = 60000
		 //max.in.flight.requests.per.connection = 1  限制客户端在单个连接上能够发送的未响应请求的个数
		 //unclean.leader.election.enable=false  关闭unclean leader选举，即不允许非ISR中的副本被选举为leader，以避免数据丢失 。 默认是true
		 //保证replication.factor > min.insync.replicas  如果两者相等，当一个副本挂掉了分区也就没法正常工作了。通常设置replication.factor = min.insync.replicas + 1即可
		 //props.put("reconnect.backoff.ms ", 20000);
         //props.put("retry.backoff.ms", 20000);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
		 producer = new KafkaProducer<Integer, String>(props);
		 this.topic = topic;
		 this.sleeptime = sleeptime;
	}
	
	@Override
	public void run() {
		 int messageNo = 1;
		    String[] msgs = { 
		    	      "UDP\005192.173.1.11\0058080\005192.168.1.23\00580\005dfsdfsfsdfsdfsdfsdfsdf1230000000\005www8.tok2.com\005http://dev.mysql.com/doc/\00520140310111213\005Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;Trident/5.0)\005s_vi=[CS]v1|2982DD3F850116FE-4000010060009F58[CE]", 
		    	      "TCP\005192.168.134.12\0058080\005112.168.1.28\00580\005dfsdfsfsdfsdfsdfsdfsdf1230000001\005www6.miami.edu\005http://php.weather.sina.com.cn/iframe_weather.php?type=w\00520140310111213\005Mozilla/4.0(compatible;MSIE7.0;WindowsNT6.1;Trident/5.0;SLCC2;.NETCLR2.0.50727;.NETCLR3.5.30729;.NETCLR3.0.30729;MALN;InfoPath.2;.NET4.0C)\005v=\"1CHgJoSFCB9MJ8Vixgwg|::\";u=1392885439036|1392885439036||1392885439036|1392885439038|", 
		    	      "UDP\005192.148.113.32\0058080\005192.134.1.29\00580\005dfsdfsfsdfsdfsdfsdfsdf1230000001\005http://www.baidu.com/s?wd=%E5%8F%98%E5%BD%A2%E9%87%91%E5%88%9A&rsv_bp=0&tn=baidu&rsv_spt=3&ie=utf-8&rsv_sug3=4&rsv_sug4=66&rsv_sug2=0&inputT=3619\005http://kmustjwcxk12.kmust.edu.cn/jwweb\00520140310111213\005Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;Trident/5.2)\005", 
		    	      "TCP\005137.188.1.14\0057777\005192.168.34.30\00580\005dfsdfsfsdfsdfsdfsdfsdf1230000001\005www6.cityu.edu.hk\005http://edu.tudou.com/\00520140310111213\005Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;Trident/5.3)\005UID=ebd6aef-96.17.199.16-1392459336;UIDR=1392459336", 
		    	      "TCP\005192.168.34.30\0054578\005192.168.1.46\00580\005dfsdfsfsdfsdfsdfsdfsdf1230000019\005http://www.baidu.com/?wd=%E5%A4%A7%E4%BC%97&rsv_bp=0&tn=baidu&rsv_spt=3&ie=utf-8&rsv_sug3=3&rsv_sug4=41&rsv_sug1=4&rsv_sug2=0&inputT=3629\005http://wot.duowan.com/s/tank_box/index.html?from=WOTBox&Version=1.5.9\00520140310111213\005Mozilla/5.0(compatible;MSIE9.0;WindowsNT6.1;Trident/5.19)\005HMACCOUNT=B54BFC96F075A482;BAIDUID=7EFEEBE9BF2B3E3A3889B73C6A06DA77:FG=1" 
		    	      };
		 long startTime = System.currentTimeMillis();
		 while(true){
			 //int index=(int)(Math.random()*msgs.length);
			 //String v = msgs[index];
			 String v = String.valueOf(messageNo);
			 try {
				 producer.send(new ProducerRecord<Integer, String>(topic, messageNo, v), new DemoCallBack(startTime, messageNo, v));
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("topic cgtest producer is fail");
				break;
			}finally{
				try {
					Thread.sleep(sleeptime);
					
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				++messageNo;
			}
		 }
		 producer.close();
	}

	class DemoCallBack implements Callback {

	    private final long startTime;
	    private final int key;
	    private final String message;

	    public DemoCallBack(long startTime, int key, String message) {
	        this.startTime = startTime;
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
	        long elapsedTime = System.currentTimeMillis() - startTime;
	        if (metadata != null) {
	        	logger.info(
	                "message(" + key + ", " + message +") sent to partition(" + metadata.partition() +"), " +
	                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
	        } else {
	        	logger.error("topic cgtest producer is fail metadata" ,exception);
	        }
	        if(exception != null){
	        	logger.error("topic cgtest producer is fail" ,exception);
	        }
	    }
	}
	public static void main(String[] args) {
		System.out.println(Integer.MAX_VALUE);
	}

}
