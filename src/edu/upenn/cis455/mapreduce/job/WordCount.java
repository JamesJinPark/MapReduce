package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

/**
 * @author James Park
 * Implementation of the Job interface
 *
 */
public class WordCount implements Job {

  /* (non-Javadoc)
 * @see edu.upenn.cis455.mapreduce.Job#map(java.lang.String, java.lang.String, edu.upenn.cis455.mapreduce.Context)
 * value is a line of the word document which is split by words into a temp array.
 * for loop on the temp array to emit intermediate key value pair of the word with number 1
 */
public void map(String key, String value, Context context)
  {
	  String[] words = value.split(" ");
	  for(int i = 0; i < words.length; i++){
		  context.write(words[i].trim(), "1");
	  }
  }
  
  /* (non-Javadoc)
 * @see edu.upenn.cis455.mapreduce.Job#reduce(java.lang.String, java.lang.String[], edu.upenn.cis455.mapreduce.Context)
 * returns the length of the array of values that is a count of how many times the word appears 
 */
public void reduce(String key, String[] values, Context context)
  {
	  context.write(key, String.valueOf(values.length));
  }
  
}
