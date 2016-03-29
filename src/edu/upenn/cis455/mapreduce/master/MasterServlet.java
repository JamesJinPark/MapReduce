package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {
	static final long serialVersionUID = 455555001;
	HashMap<String, WorkerStatus> workerStatuses = new HashMap<>();

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException  {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		
		String path = request.getRequestURI();
		StringBuffer buffer = new StringBuffer();
		
		switch(path){
			case "/workerstatus": 	buffer = sendWorkerStatus(request, response);			
									break;
			case "/status": 		buffer = sendStatus(request, response);
									break;
		}

		out.println(buffer);
		out.flush();
		out.close();
	}
	
	public StringBuffer sendWorkerStatus(HttpServletRequest request, HttpServletResponse response) throws IOException{
		int workerIPAddress = Integer.valueOf(request.getRemoteAddr());

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
		
		StringBuffer htmlBuffer = new StringBuffer();
		htmlBuffer.append("<html>");
		htmlBuffer.append("<h1>CIS 555 XPathServlet</h1>");

		
		return htmlBuffer;
	}

	public StringBuffer sendStatus(HttpServletRequest request, HttpServletResponse response) throws IOException{
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		// need table with status info about workers
		// webform
		StringBuffer htmlBuffer = new StringBuffer();
		
		htmlBuffer.append("<html>");
		htmlBuffer.append("<h1>CIS 555 XPathServlet</h1>");
		htmlBuffer.append("<p>Full Name: James Jin Park</p>");
		htmlBuffer.append("<p>SEAS Login Name: jamespj</p>");
		
		htmlBuffer.append("<form method=\"POST\" action=\"/servlet/runmap\">");
		
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

}

