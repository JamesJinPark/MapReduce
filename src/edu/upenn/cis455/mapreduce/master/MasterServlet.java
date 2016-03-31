package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {
	static final long serialVersionUID = 455555001;
	HashMap<String, WorkerStatus> workerStatuses = new HashMap<>();
	String currentJob = "None";

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException  {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		
		String path = request.getRequestURI();
		StringBuffer buffer = new StringBuffer();
		
		switch(path){
			case "/workerstatus": 	buffer = workerStatusHandler(request, response);			
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
			runReduce(String job, String );
		}
		
		StringBuffer htmlBuffer = new StringBuffer();
		htmlBuffer.append("This is " + port);
		htmlBuffer.append("This is " + job);
		htmlBuffer.append("This is " + status);
		htmlBuffer.append("This is " + keysRead);
		htmlBuffer.append("This is " + keysWritten);

		return htmlBuffer;
	}

	public StringBuffer sendStatus(HttpServletRequest request, HttpServletResponse response) throws IOException{		
		// need table with status info about workers
		StringBuffer htmlBuffer = new StringBuffer();
		
		htmlBuffer.append("<html>");
		htmlBuffer.append("<h1>CIS 555 XPathServlet</h1>");
		htmlBuffer.append("<p>Full Name: James Jin Park</p>");
		htmlBuffer.append("<p>SEAS Login Name: jamespj</p>");
		
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
		
		this.currentJob = jobName; //saves the current job 

		switch(path){
			case "/runmap": 		runMap(jobName, inputDir, numMapThreads);

			buffer.append("This is output Dir " + outputDir);
			buffer.append("This is numReduceThreads " + numReduceThreads);
			
									break;
									

		}
	
		out.println(buffer);
		out.println();
		out.flush();
		out.close();

	}
	
	public void runMap(String jobName, String inputDir, String numMapThreads) throws IOException{
		for(String workerIPandPort: workerStatuses.keySet()){
			String url = "http://" + workerIPandPort + "/runmap"; 
			URL obj = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
			
			connection.setRequestMethod("POST");
			connection.setRequestProperty("User-Agent", "MasterServlet");
			String urlParameters = "job=" + jobName + "inputDir=" + inputDir + "numThreads=" + numMapThreads  + "numWorkers=" + 
					workerStatuses.size();
			connection.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
		}
	}
	public void runReduce(){
		for(String workerIPandPort: workerStatuses.keySet()){
			String url = "http://" + workerIPandPort + "/runmap"; 
			URL obj = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
			
			connection.setRequestMethod("POST");
			connection.setRequestProperty("User-Agent", "MasterServlet");
			String urlParameters = "job=" + currentJob + "outputDir=" + outputDir + "numThreads=" + numMapThreads  + "numWorkers=" + 
					workerStatuses.size();
			connection.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
		}
		
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