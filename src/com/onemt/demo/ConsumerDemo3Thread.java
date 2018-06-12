package com.onemt.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConsumerDemo3Thread extends Thread{

	ConcurrentLinkedQueue<String> subscribedTopics;
	File file;
	
	public ConsumerDemo3Thread(ConcurrentLinkedQueue<String> subscribedTopics,String fileName) {
		
		this.subscribedTopics = subscribedTopics;
		this.file = new File(fileName);
	}
	@Override
	public void run() {
		
		while(true){
			try {
	             Thread.sleep(3000);
	         } catch (InterruptedException e) {
	             // swallow it.
	         }
			List<String> list = new ArrayList<String>();
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				String readLine = reader.readLine();
				while(readLine != null){
					if(readLine.contains("topic=")){
						String topic = readLine.split("=")[1];
						list.add(topic);
					}
					readLine = reader.readLine();
				}
				//变更为订阅topic
				subscribedTopics.addAll(list);
				
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	         // 变更为订阅topic： btopic， ctopic
	         //subscribedTopics.addAll(Arrays.asList("btopic", "ctopic"));
		}
		 
	}

}
