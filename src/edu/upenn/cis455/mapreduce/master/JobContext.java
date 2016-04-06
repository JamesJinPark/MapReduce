package edu.upenn.cis455.mapreduce.master;

public class JobContext{
	String jobName;
	String inputDir;
	String outputDir;
	int numMapThreads;
	int numReduceThreads;	
	
	public JobContext(){
		this.jobName = "None";
		this.inputDir = "None";
		this.outputDir = "None";
		this.numMapThreads = 0;
		this.numReduceThreads = 0;
	}

	public JobContext(String jobName, String inputDir, String outputDir, int numMapThreads, int numReduceThreads){
		this.jobName = jobName;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numMapThreads = numMapThreads;
		this.numReduceThreads = numReduceThreads;
	}
	
	public String getJobName(){
		return this.jobName;
	}
	
	public String getInputDir(){
		return this.inputDir;
	}
	
	public String getOutputDir(){
		return this.outputDir;
	}
	
	public int getNumMapThreads(){
		return this.numMapThreads;
	}
	
	public int getNumReduceThreads(){
		return this.numReduceThreads;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setInputDir(String inputDir) {
		this.inputDir = inputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public void setNumMapThreads(int numMapThreads) {
		this.numMapThreads = numMapThreads;
	}

	public void setNumReduceThreads(int numReduceThreads) {
		this.numReduceThreads = numReduceThreads;
	}
	
}