package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;

public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	int port;
	String status = "idle";
	int masterPort;
	String masterIP;
	String jobName;
	String storageDir;
	int keysRead;
	int keysWritten;
	WorkerStatusUpdater statusUpdater;	
	HashMap<String, String> workers;
	
	public void init(ServletConfig config) throws ServletException{
		super.init(config);
		this.jobName = "None";
		this.keysRead = 0;
		this.keysWritten = 0;
		this.status = "idle";
		this.storageDir = config.getInitParameter("storagedir");
		this.workers = new HashMap<>();
		
		String masterIPandPort= config.getInitParameter("master");
		this.masterIP = masterIPandPort.split(":")[0];
		this.masterPort = Integer.valueOf(masterIPandPort.split(":")[1]);

		this.port = Integer.valueOf(config.getInitParameter("port"));
		statusUpdater = new WorkerStatusUpdater(this);
		Thread thread = new Thread(statusUpdater);
		thread.start();
		System.out.println("Successfully initialized worker servlet.");
	}
	public void doGet (HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException {
		System.out.println("Worker Servlet received GET!");
	}
	
	public void doPost (HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException {
		System.out.println("Worker Servlet received POST!");
		String path = request.getRequestURI();

		switch(path){
			case "/runmap": 		this.keysWritten = 0;
									runmapHandler(request, response);			
									break;
			case "/pushdata": 		pushdataHandler(request, response);
									break;
			case "/runreduce": 		runreduceHandler(request, response);
									break;
		}
		
	}
	
	public void runmapHandler(HttpServletRequest request, HttpServletResponse response){
		this.status = "mapping";
		this.jobName = request.getParameter("job");
		String input = request.getParameter("input");
		String numThreads = request.getParameter("numMapThreads");
		String numWorkers = request.getParameter("numWorkers");
		
		System.out.println("Job: " + jobName);
		System.out.println("InputDir: " + input);
		System.out.println("Num threads: " + numThreads);
		System.out.println("Num workers: " + numWorkers);
		
		int count = 1;
		while(request.getParameter("worker" + count) != null){
			workers.put("worker" + count, request.getParameter("worker" + count));
			count++; 
		}
		
		//create spool in directory
		File fSpoolInDir = new File(storageDir + "/spool_in");
		if(fSpoolInDir.exists()){
			dirDelete(fSpoolInDir);
		}
		fSpoolInDir.mkdir();

		//create spool out directory
		File fSpoolOutDir = new File(storageDir + "/spool_out");
		if(fSpoolOutDir.exists()){
			dirDelete(fSpoolOutDir);
		}
		fSpoolOutDir.mkdir();
		
		//create context to send mapworkers
		WorkerContext mapContext = new WorkerContext(true, false, fSpoolInDir, fSpoolOutDir, 
				numWorkers);
		
		//class loader
		Job currentJob = null;
		try {
			System.out.println(this.jobName);
			currentJob = (Job)Class.forName(this.jobName).newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		//create a queue of files to be read by the threads
		File inputDir = new File(storageDir + "/" + input);
		if(!inputDir.exists()){
			System.out.println(inputDir.toPath());
		}
		File[] files = inputDir.listFiles();
		BlockingQueue<File> queue = new BlockingQueue<>(files.length);
		for(File file : files){
			queue.enqueue(file);
		}
		
		//make threads
		ArrayList<MapWorker> threads = new ArrayList<MapWorker>();
		for(int i = 0; i < Integer.valueOf(numThreads); i++ ){
			MapWorker mapWorker = new MapWorker(currentJob, queue, this, mapContext);
			mapWorker.start();
			threads.add(mapWorker);
		}
		
		while(!workingDone(threads)){};
		System.out.println("Worker threads all finished Map!!");

		this.status = "waiting";
		sendPushPost();
		
		//send status after finishing
		statusUpdater.sendStatus();
	}
	
	public boolean workingDone(ArrayList<?> threads){
		for(Object thread : threads){
			if(((Thread) thread).isAlive()) return false;
		}
		return true;
	}
	
	public void sendPushPost(){
		//get output files from spool out directory
		File fSpoolOutDir = new File(storageDir + "/spool_out");
		File[] outputFiles = fSpoolOutDir.listFiles();
		
		//for each file in outputFiles, send push to workers
		for(File file: outputFiles){
			try {
				
				String workerIPandPort = workers.get(file.getName().toString());

				String url = "http://" + workerIPandPort + "/pushdata"; 
				URL obj = new URL(url);
				HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
				connection.setRequestMethod("POST");
				connection.setRequestProperty("User-Agent", "WorkerServlet");
				connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
				BufferedReader reader = new BufferedReader(new FileReader(file));
				StringBuilder builder = new StringBuilder();
				String line = reader.readLine();
				while(line != null){
					builder.append(line + "\n");
					line = reader.readLine();
				}
				reader.close();
				connection.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
				wr.writeBytes("body=" + builder.toString());
				wr.flush();
				wr.close();				
				int responseCode = connection.getResponseCode();
				System.out.println("Sending push: " + responseCode);
			} catch (IOException e) {
			}
		}
	}

	public void pushdataHandler(HttpServletRequest request, HttpServletResponse response){
		System.out.println("Received push data!");
		File fSpoolInDir = new File(storageDir + "/spool_in");
		if(!fSpoolInDir.exists()){
			fSpoolInDir.mkdir();
		}
		Random randomGen = new Random();

		int count = randomGen.nextInt(100);

		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fSpoolInDir + "/" + count, true)));
			String body = request.getParameter("body");
			BufferedReader reader = new BufferedReader(new StringReader(body));
			String line = reader.readLine();
			while(line != null){
				out.println(line);
				line = reader.readLine();
			}
			out.close();
			reader.close();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public void runreduceHandler(HttpServletRequest request, HttpServletResponse response){
		this.status = "reducing";
		String output = request.getParameter("output");
		String numThreads = request.getParameter("numReduceThreads");
		String numWorkers = request.getParameter("numWorkers");
		
		//get spool-in dir
		File fSpoolInDir = new File(storageDir + "/spool_in");
		
		//create spool out directory
		File fSpoolOutDir = new File(storageDir + "/spool_out");
		if(fSpoolOutDir.exists()){
			dirDelete(fSpoolOutDir);
		}
		fSpoolOutDir.mkdir();

		//create output directory and if output directory exists, delete
		File outputDir = new File(this.storageDir + "/" + output);
		if(outputDir.exists()){
			dirDelete(outputDir);
		}
		outputDir.mkdir();

		File[] spoolInFiles = fSpoolInDir.listFiles();

		//sort key-value pairs in the spool-in 		
		for(File file : spoolInFiles){
			try {
				Process p = Runtime.getRuntime().exec("sort " + file + " -o " + file);
				p.waitFor();
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		//create context to send reduceworkers
		WorkerContext reduceContext = new WorkerContext(false, true, fSpoolInDir, fSpoolOutDir, 
				numWorkers);

		reduceContext.setOutputDir(outputDir);

		//class loader
		Job currentJob = null;
		try {
			currentJob = (Job) Class.forName(this.jobName).newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		//sort key value pairs to send to threads
		File[] files = fSpoolInDir.listFiles();
		HashMap<String, String[]> keyValuePairs = new HashMap<>();
		for(File file : files){
			BufferedReader reader;
			try {
				reader = new BufferedReader(new FileReader(file));
				String line = reader.readLine();
				while(line != null){
					String[] pairs = line.split("\t");
					if(keyValuePairs.get(pairs[0]) == null){
						String[] value = new String[1];
						value[0] = pairs[1];
						keyValuePairs.put(pairs[0], value);
					} else {
						String[] oldValue = keyValuePairs.get(pairs[0]);
						int length = oldValue.length;
						String[] newValue = new String[length + 1];
						for(int i = 0; i < length; i++){
							newValue[i] = oldValue[i];
						}
						newValue[length] = pairs[1];
						keyValuePairs.put(pairs[0], newValue);
					}
					line = reader.readLine();
				}
				reader.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Set<String> keys = keyValuePairs.keySet();
		BlockingQueue<String> queue = new BlockingQueue<>(keys.size());
		for(String key : keys){
			queue.enqueue(key);
		}

		//make threads
		ArrayList<ReduceWorker> threads = new ArrayList<ReduceWorker>();
		for(int i = 0; i < Integer.valueOf(numThreads); i++ ){
			ReduceWorker reduceWorker = new ReduceWorker(currentJob, this, reduceContext, queue, keyValuePairs);
			reduceWorker.start();
			threads.add(reduceWorker);
		}
		while(!workingDone(threads)){};		
		this.status = "idle";
		this.keysRead = 0;
		this.jobName = "None";
		statusUpdater.sendStatus();		
	}

	public void dirDelete(File f){
		for(File file : f.listFiles()){
			file.delete();
		}
		f.delete();
	}
}

