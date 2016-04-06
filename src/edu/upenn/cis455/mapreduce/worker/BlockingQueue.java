package edu.upenn.cis455.mapreduce.worker;

import java.util.LinkedList;

/**
 * @author James Park
 *
 * Blocking queue to hold files to be read by workers
 * 
 * @param <E>
 */
public class BlockingQueue<E> {
	
	private LinkedList<E> queue;
	private int maxLength;

	public BlockingQueue(int maxLength){
		this.queue = new LinkedList<E>();
		this.maxLength = maxLength;
	}
	
	/**
	 * @param object
	 * Adds object to the queue
	 */
	public synchronized void enqueue(E object){
		while(this.isFull()){
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		if(!this.isFull()){  
			notifyAll();
		}
		this.queue.add(object);
	}
	
	/**
	 * @return
	 * @throws InterruptedException
	 * Removes item from the queue
	 */
	public synchronized E dequeue() throws InterruptedException{		
		while(this.isEmpty()){
			wait();
		}
		if(!this.isEmpty()){ 
			notifyAll();
		}
		return this.queue.poll();
	}
	
	public boolean isEmpty(){
		return this.queue.size() == 0;
	}

	public boolean isFull(){
		return this.queue.size() == this.maxLength;
	}
}
