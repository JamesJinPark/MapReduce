package edu.upenn.cis455.mapreduce.master;

import java.util.Date;

public class WorkerStatus {
	int workerIPAddress;
	int port;
	String job;
	String status;
	int keysRead;
	int keysWritten;
	Date timestamp;
	
	public WorkerStatus(int workerIPAddress, int port, String job, String status, int keysRead, int keysWritten){
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

}
