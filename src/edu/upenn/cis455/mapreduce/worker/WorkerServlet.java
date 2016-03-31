package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;

	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException {
		

		
		String storageDir = getServletContext().getInitParameter("storageDir");

		String masterIPandPort= getServletContext().getInitParameter("master");
		int masterIP = Integer.valueOf(masterIPandPort.split(":")[0]);
		int masterPort= Integer.valueOf(masterIPandPort.split(":")[1]);

		String workerPort = getServletContext().getInitParameter("port");

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}
	
	public void doPost (HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException {
		
	}
}

