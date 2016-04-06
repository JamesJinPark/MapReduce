package edu.upenn.cis455.mapreduce.worker;

import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Job;

public class ReduceWorker extends Thread{
	Job job;
	WorkerContext context;
	WorkerServlet servlet;
	HashMap<String, String[]> keyValuePairs;
	BlockingQueue<String> queue;
	
	public ReduceWorker(Job job, WorkerServlet servlet, WorkerContext context, 
			BlockingQueue<String> queue, HashMap<String, String[]> keyValuePairs){
		this.job = job;
		this.context = context;
		this.servlet = servlet;
		this.keyValuePairs = keyValuePairs;
		this.queue = queue;
	}
	
	public void run(){
		while(!queue.isEmpty()){
			String key;
			try {
				key = queue.dequeue();
				job.reduce(key, keyValuePairs.get(key), context);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
