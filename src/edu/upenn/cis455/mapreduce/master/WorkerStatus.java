package edu.upenn.cis455.mapreduce.master;

import java.util.Date;

public class WorkerStatus {
	String workerIPAddress;
	int port;
	String job;
	String status;
	int keysRead;
	int keysWritten;
	Date timestamp;
	
	public WorkerStatus(){
		status = "idle";
		keysRead = 0; 
		keysWritten = 0; 
	}
	
	public WorkerStatus(String workerIPAddress, int port, String job, String status, int keysRead, int keysWritten){
		this.workerIPAddress = workerIPAddress;
		this.port = port;
		this.job = job;
		this.status = status;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		timestamp = new Date();
	}
	
	public Date getTimeStamp(){
		return timestamp;
	}
	
	public int getPort(){
		return port;
	}
	
	public String getJob(){
		return job;
	}
	
	public int getKeysRead(){
		return keysRead;
	}

	public void setKeysRead(int keysRead){
		this.keysRead = keysRead;
	}

	public int getKeysWritten(){
		return keysWritten;
	}

	public void setKeysWritten(int keysWritten){
		this.keysWritten= keysWritten;
	}
	
	public String getStatus(){
		return status;
	}
	
	public void setStatus(String status){
		this.status = status;
	}
	
	public String getWorkerIPAddress(){
		return workerIPAddress;
	}

}
