package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class WorkerStatusUpdater implements Runnable{
	WorkerServlet workerServlet;
	
	public WorkerStatusUpdater(WorkerServlet workerServlet){
		this.workerServlet = workerServlet;
	}
	
	public void sendStatus(){
		String url = "http://" + this.workerServlet.masterIP + ":" + this.workerServlet.masterPort 
				+ "/workerstatus"; 
		System.out.println(url);
		String urlParameters = 
				"port=" + workerServlet.port + 
				"&status=" + workerServlet.status  + 
				"&job=" + workerServlet.jobName + 
				"&keysRead=" + workerServlet.keysRead + 
				"&keysWritten=" + workerServlet.keysWritten;

		try{
			URL obj = new URL(url +  "?" + urlParameters);
			HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("User-Agent", "WorkerThread");
			connection.setDoOutput(true);
			System.out.println("Sending worker status for " + this.workerServlet.port);
			int responseCode = connection.getResponseCode();
			System.out.println("Response Code: " + responseCode);
		} catch(IOException e){
		}
	}

	@Override
	public void run() {		
		while(true){
			sendStatus();
			try{
				Thread.sleep(10000);
			} catch(InterruptedException e){
				continue;
			}
		}
	}

}
