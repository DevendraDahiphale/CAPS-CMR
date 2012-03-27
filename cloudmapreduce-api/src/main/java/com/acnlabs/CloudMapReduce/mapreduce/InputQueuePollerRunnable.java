
/*
* @author Devendra Dahiphale (devendradahiphale@gmail.com)
*/ 
package com.acnlabs.CloudMapReduce.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.DbManager;
import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.QueueManager;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;
import com.acnlabs.CloudMapReduce.util.WorkerThreadQueue;
import com.amazonaws.queue.model.Message;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.S3Item;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.S3FileSystem;
public class InputQueuePollerRunnable implements Runnable{
	private SimpleQueue inputQueue;
	private Mapper map;
	private Reducer combiner;
	private PerformanceTracker perf;
	private WorkerThreadQueue mapWorkers;
	private MapReduce dummyMapReduceInstance;
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduceApp");
	public InputQueuePollerRunnable(SimpleQueue inputQueue,WorkerThreadQueue mapWorkers,Mapper map, Reducer combiner,PerformanceTracker perf)
	{
		this.inputQueue=inputQueue;
		this.mapWorkers=mapWorkers;
		this.combiner=combiner;
		this.map=map;
		this.perf=perf;
	}
	public void run()
	{
		while(true)
		{
			try {
				do {
					// while input SQS not empty, dequeue, then map
					for (Message msg : inputQueue) {
						String value = msg.getBody();
						// get the key for the key/value pair, in our case, key is the mapId
						int separator = value.indexOf(Global.separator);   // first separator
						int mapId = Integer.parseInt(value.substring(0, separator));
						value = value.substring(separator+Global.separator.length());
						logger.debug(mapId + ".");
				   		mapWorkers.push(dummyMapReduceInstance.new MapRunnable(map, combiner, Integer.toString(mapId), value, perf, mapId, msg.getReceiptHandle()));
				   		mapWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here
					}
					// SQS appears to be empty, but really should make sure we processed all in the queue
				} while (true);//! dbManager.isStageFinished(jobID, Global.STAGE.MAP) );  // if still has work left in theory, go back bang the queue again
			}
			catch (Exception e) {
				logger.info("Error in InputQueuePollerRunnable:" + e);
	        	logger.warn("Map Exception: " + e.getMessage());
			}	
		}
	}
}