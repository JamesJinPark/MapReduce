package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.upenn.cis455.mapreduce.Context;

public class WorkerContext implements Context{
	
	boolean calledFromMap;
	boolean calledFromReduce;
	File fSpoolInDir;
	File fSpoolOutDir;
	File outputDir;
	String numWorkers;
	
	public WorkerContext(boolean calledFromMap, boolean calledFromReduce, File fSpoolInDir, File fSpoolOutDir, 
			String numWorkers){
		this.calledFromMap = calledFromMap;
		this.calledFromReduce = calledFromReduce;
		this.fSpoolInDir = fSpoolInDir;
		this.fSpoolOutDir = fSpoolOutDir;
		this.numWorkers = numWorkers;
		this.outputDir = null;
	}

	/**
	 * @throws NoSuchAlgorithmException
	 * Generates a SHA hash 
	 */
	public String generateSHA(String key) throws NoSuchAlgorithmException{
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		md.update(key.getBytes());
		byte[] output = md.digest();
		return bytesToString(output);
	}
	
	public String bytesToString(byte[] array){
		StringBuilder builder = new StringBuilder();
		for(byte i : array){
			builder.append(String.format("%02x", i));
		}
		return builder.toString();
		
		
	}
	
	public void setOutputDir(File outputDir){
		this.outputDir = outputDir;
	}

	@Override
	public void write(String key, String value) {
		File file = null;
		if(calledFromMap){
			int workerNum = 0;
			try {
				String hashString = generateSHA(key);
				BigInteger hash = new BigInteger(hashString , 16);
				BigInteger numDivisions = new BigInteger(numWorkers);
				BigInteger maxHexRange = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
				BigInteger bucketSize = maxHexRange.divide(numDivisions);
				BigInteger workerBigNum = hash.divide(bucketSize);
				workerNum = workerBigNum.intValue() + 1;
			} catch (NoSuchAlgorithmException e1) {
				e1.printStackTrace();
			}

			file = new File(fSpoolOutDir + "/worker" + workerNum);
		}
		if(calledFromReduce){
			if(!outputDir.exists()){
				outputDir.mkdir();
			}
			file = new File(outputDir + "/output.txt");
		}
		try {
			synchronized(file){
				PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
				out.println(key + "\t" + value);
				out.close();
			}
		} catch (IOException e) {
		}

	}

}
