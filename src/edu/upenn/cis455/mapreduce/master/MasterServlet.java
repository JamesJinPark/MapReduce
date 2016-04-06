package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Set;

import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {
	static final long serialVersionUID = 455555001;
	HashMap<String, WorkerStatus> workerStatuses = new HashMap<>();
	JobContext currentJobContext = new JobContext();

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException  {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		
		String path = request.getRequestURI();
		StringBuffer buffer = new StringBuffer();
		
		switch(path){
			case "/workerstatus": 	System.out.println("Received workerstatus GET request");
									buffer = workerStatusHandler(request, response);
									break;
			case "/status": 		buffer = sendStatus(request, response);
									break;
		}

		out.println(buffer);
		out.flush();
		out.close();
	}
	
	public StringBuffer workerStatusHandler(HttpServletRequest request, HttpServletResponse response) throws IOException{
		String workerIPAddress = request.getRemoteAddr();

		int port = Integer.valueOf(request.getParameter("port"));

		String job = request.getParameter("job");
		
		String status = request.getParameter("status");
		
		int keysRead = Integer.valueOf(request.getParameter("keysRead"));
		//if status==mapping || reducing, keysRead should be # of keys that have read so far 
		//if status==waiting, keysRead should be # of keys that were read by the last map
		//if status==idle keysRead should be 0 

		int keysWritten = Integer.valueOf(request.getParameter("keysWritten"));
		//if status==mapping || reducing, keysWritten should be # of keys that have written so far 
		//if status==waiting, keysWritten should be # of keys that were written by the last map
		//if status==idle keysWritten should be # of keys that were written by the last reduce
		//if node has never run any jobs, return 0
		
		workerStatuses.put(workerIPAddress + ":" + port, new WorkerStatus(workerIPAddress, port, job, status, 
				keysRead, keysWritten));
	
		if(checkAllWorkerStatuses()){
			sendReduce(request);
		}
		
		StringBuffer htmlBuffer = new StringBuffer();
		htmlBuffer.append("This is worker IP: " + workerIPAddress);
		htmlBuffer.append("This is " + port);
		htmlBuffer.append("This is " + job);
		htmlBuffer.append("This is " + status);
		htmlBuffer.append("This is " + keysRead);
		htmlBuffer.append("This is " + keysWritten);

		return htmlBuffer;
	}
	
	public void sendReduce(HttpServletRequest request){
		try{
			String url = "http://" + request.getServerName() + ":" + request.getServerPort()  + "/runreduce"; 
			URL obj = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("User-Agent", "MasterServlet");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			String urlParameters = "masterServlet=sendingReduce";
			connection.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
			connection.getResponseCode();
		} catch(IOException e){
			
		}
	}

	public StringBuffer sendStatus(HttpServletRequest request, HttpServletResponse response) throws IOException{		
		StringBuffer htmlBuffer = new StringBuffer();
		
		htmlBuffer.append("<html>");
		htmlBuffer.append("<h1>CIS 555 XPathServlet</h1>");
		htmlBuffer.append("<p>Full Name: James Jin Park</p>");
		htmlBuffer.append("<p>SEAS Login Name: jamespj</p>");

		htmlBuffer.append("<h2>Worker Statuses</h2>");
		htmlBuffer.append("<table>");
		
		htmlBuffer.append("<tr>");
		htmlBuffer.append("<th>Worker IP address and Port</th>");
		htmlBuffer.append("<th>Status</th>");
		htmlBuffer.append("<th>Job Name</th>");
		htmlBuffer.append("<th>Keys Read</th>");
		htmlBuffer.append("<th>Keys Written</th>");
		htmlBuffer.append("</tr>");
		
		Set<String> keys = workerStatuses.keySet();
		
		for(String key : keys){
			if(workerStatuses.get(key).isActive()){
				htmlBuffer.append("<tr>");		
				htmlBuffer.append("<td>" +  key + "</td>");
				htmlBuffer.append("<td>" +  workerStatuses.get(key).getStatus() + "</td>");
				htmlBuffer.append("<td>" +  workerStatuses.get(key).getJob() + "</td>");
				htmlBuffer.append("<td>" +  workerStatuses.get(key).getKeysRead()+ "</td>");
				htmlBuffer.append("<td>" +  workerStatuses.get(key).getKeysWritten() + "</td>");
				htmlBuffer.append("</tr>");			
			}
		}
		htmlBuffer.append("</table>");

		
		htmlBuffer.append("<form method=\"POST\" action=\"/runmap\">");
		
		htmlBuffer.append("<h2>Enter Class Name of MapReduce Job</h2>");
		htmlBuffer.append("<input type=\"text\" name=\"jobName\" id=\"jobName\">");
		
		htmlBuffer.append("<h2>Enter Input directory, relative to Storage directory</h2>");
		htmlBuffer.append("<input type=\"text\" name=\"inputDir\" id=\"inputDir\"><br><br>");
		
		htmlBuffer.append("<h2>Enter Output directory, relative to Storage directory</h2>");
		htmlBuffer.append("<input type=\"text\" name=\"outputDir\" id=\"outputDir\"><br><br>");
		
		htmlBuffer.append("<h2>Enter number of map threads to run on each worker</h2>");
		htmlBuffer.append("<input type=\"text\" name=\"numMapThreads\" id=\"numMapThreads\"><br><br>");

		htmlBuffer.append("<h2>Enter number of reduce threads to run on each worker</h2>");
		htmlBuffer.append("<input type=\"text\" name=\"numReduceThreads\" id=\"numReduceThreads\"><br><br>");

		htmlBuffer.append("<input type=\"submit\" value=\"Submit Form\"/>");
		
		htmlBuffer.append("</form>");
		htmlBuffer.append("</html>");
		
		return htmlBuffer;
	}
	
	public void doPost (HttpServletRequest request, HttpServletResponse response) throws java.io.IOException  {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");

		String path = request.getRequestURI();
		StringBuffer buffer = new StringBuffer();
		
		String jobName = request.getParameter("jobName");
		String inputDir = request.getParameter("inputDir");
		String outputDir = request.getParameter("outputDir");
		String numMapThreads = request.getParameter("numMapThreads");
		String numReduceThreads = request.getParameter("numReduceThreads");
		
		int numMap = 0;
		int numReduce = 0;
		if(numMapThreads != null){
			numMap = Integer.valueOf(numMapThreads);
		}
		if(numReduceThreads != null){
			numReduce = Integer.valueOf(numReduceThreads);
		}
		
		if(jobName != null) this.currentJobContext.setJobName(jobName);
		if(inputDir != null) this.currentJobContext.setInputDir(inputDir);			
		if(outputDir != null) this.currentJobContext.setOutputDir(outputDir);			
		if(numMapThreads != null) this.currentJobContext.setNumMapThreads(numMap);
		if(numReduceThreads != null) this.currentJobContext.setNumReduceThreads(numReduce);
		
//		this.currentJobContext = new JobContext(jobName, inputDir, outputDir, numMap, numReduce);

		switch(path){
			case "/runmap": 		buffer = runMap();
									break;
			case "/runreduce": 		buffer = runReduce();
									break;
		}
	
		out.println(buffer);
		out.println();
		out.flush();
		out.close();

	}
	
	public StringBuffer runMap() throws IOException{
		System.out.println("RUNNING MAP!");
		StringBuffer htmlBuffer = new StringBuffer();
		htmlBuffer.append("<html>");
		htmlBuffer.append("<h1>Running MapReduce!</h1>");

		for(String workerIPandPort: workerStatuses.keySet()){
			String url = "http://" + workerIPandPort + "/runmap"; 
			URL obj = new URL(url);
			System.out.println("Sending POST to " + workerIPandPort + "/runmap!");
			HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("User-Agent", "MasterServlet");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

			String urlParameters = "job=" + currentJobContext.getJobName() + "&input=" + 
				currentJobContext.getInputDir() + "&numMapThreads=" + currentJobContext.getNumMapThreads() + 
				"&numWorkers=" + workerStatuses.size();
			
			System.out.println(urlParameters);

			String temp = "";
			int count = 1;
			for(String key : workerStatuses.keySet()){
				temp += "&worker" + count + "=" + key;
				count++;
			}
			urlParameters += temp;
			connection.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
			int responseCode = connection.getResponseCode();
			htmlBuffer.append("<p>Received the response code " + responseCode + " from "+ url  + "</p>");
		}
		htmlBuffer.append("</html>");

		return htmlBuffer;
	}
	
	public StringBuffer runReduce() throws IOException{
		StringBuffer htmlBuffer = new StringBuffer();
		htmlBuffer.append("<html>");
		htmlBuffer.append("<h1>Running MapReduce!</h1>");

		for(String workerIPandPort: workerStatuses.keySet()){
			String url = "http://" + workerIPandPort + "/runreduce"; 
			URL obj = new URL(url);
			System.out.println("Sending POST to " + workerIPandPort + "/runreduce!");

			HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("User-Agent", "MasterServlet");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			
			String urlParameters = "job=" + currentJobContext.getJobName() + "&output=" + 
					currentJobContext.getOutputDir() + "&numReduceThreads=" + currentJobContext.getNumReduceThreads() + 
					"&numWorkers=" + workerStatuses.size();
			
			connection.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
			int responseCode = connection.getResponseCode();
			htmlBuffer.append("<p>Received the response code " + responseCode + " from "+ url  + "</p>");

		}
		htmlBuffer.append("</html>");
		return htmlBuffer;
	}
	
	public boolean checkAllWorkerStatuses(){
		for(String key : workerStatuses.keySet()){
			if(!(workerStatuses.get(key).getStatus().equals("waiting"))){
				return false;
			}
		}
		return true;
	}
}