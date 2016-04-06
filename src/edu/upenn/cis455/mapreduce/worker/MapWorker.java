package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import edu.upenn.cis455.mapreduce.Job;

public class MapWorker extends Thread{
	
	Job job;
	BlockingQueue<File> queue;
	WorkerServlet workerServlet;
	WorkerContext context;
	
	public MapWorker(Job job, BlockingQueue<File> queue, WorkerServlet workerServlet, WorkerContext context){
		this.job = job;
		this.queue = queue;
		this.workerServlet = workerServlet;
		this.context = context;
	}

	@Override
	public void run() {
		while(!queue.isEmpty()){
			try {
				File file = queue.dequeue();
			
				//read key value pair
				BufferedReader reader = new BufferedReader(new FileReader(file));
				HashMap<String, String> keyValueStore = new HashMap<>();
				String line = reader.readLine();
				while(line != null){
					String[] pair = line.split("\t");
					//pair[0] is the key
					//pair[1] is the value
					if(pair.length > 1){
						keyValueStore.put(pair[0].trim(), pair[1].trim());
						workerServlet.keysRead++;
					}
					line = reader.readLine();
				}
				reader.close();
				
				//invoke map function for each pair
				Set<String> keys = keyValueStore.keySet();
				for(String key : keys){
					job.map(key, keyValueStore.get(key), context);
					workerServlet.keysWritten++;
				}
			} catch (InterruptedException e) {
				continue;
			} catch (FileNotFoundException e) {
				continue;
			} catch (IOException e) {
				continue;
			}
		}
	}

}
