/*
* Copyright 2009 Accenture. All rights reserved.
*
* Accenture licenses this file to you under the Apache License, 
* Version 2.0 (the "License"); you may not use this file except in 
* compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* @author Huan Liu (huanliu AT cs.stanford.edu)
*/ 
package com.acnlabs.CloudMapReduce.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
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

/**
 * The main function that implements MapReduce
 */
public class MapReduce {
	
	private DbManager dbManager;
	private QueueManager queueManager;
	private PerformanceTracker perf = new PerformanceTracker();
	private WorkerThreadQueue mapWorkers;
	private WorkerThreadQueue reduceWorkers;
	private WorkerThreadQueue queueWorkers;  // used for input/output/master reduce queues
	private String jobID;
	private String taskID;
	private SimpleQueue inputQueue;
	private SimpleQueue outputQueue;
	private SimpleQueue masterReduceQueue;
	private HashSet<String> committedMap;  // set of taskId,mapId pair that generated valid reduce messages
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduce");
	private int numReduceQs;
	private HashSet<String> committedMapForJobProgressTrace;  //Devendra: To keep track of job progress
	private HashSet<String> committedReducerForJobProgressTrace; //Devendra: To keep track of job progress
	private boolean endCurrentJob;						//Devendra: Local flag when set indicates all data to be processed is finished
	private ArrayList<String> reduceQsBoundWithReducers = new ArrayList<String>();  //Devendra: already considered redueceQs are stored in
	private String s3Path;
	private Snapshot snapshotObj;
	private ArrayList<String> mapperProgress=new ArrayList<String>();
	private ArrayList<String> reducerProgress=new ArrayList<String>();
	public class MapCollector implements OutputCollector {
		
		// each map collector has its own set of queue to facilitate tagging
		private HashMap<String, SimpleQueue> queues = new HashMap<String, SimpleQueue>();
		private int numReduceQs;
		private int mapId;  // one collector per map to facilitate tagging
		private WorkerThreadQueue workers;
		private int[] reduceQStat;  // one set of stats per mapId

		public MapCollector(int numReduceQs, int mapId) {
			this.numReduceQs = numReduceQs;
			this.mapId = mapId;
			reduceQStat = new int[numReduceQs];
	        workers = new WorkerThreadQueue(Global.numUploadWorkersPerMap, "map"+mapId);
		}
		
		public void collect(String key, String value) throws IOException {
				
				/*RBK which will be used for graph plotting purpose only*/
			Global.jobProgressTracker.incrementNumRecordsGeneratedByMappers();
			
		//	Global.numOfRecordsProcessedByMappers++;
			
			perf.incrementCounter("mapOutputRecords", 1);
			
			SimpleQueue reduceQueue;
			int hash = (key.hashCode() & Integer.MAX_VALUE) % numReduceQs;
			
			// keep statistics so we know how many we expect when we read
			reduceQStat[hash] ++ ;
			
			String queueName = getSubReduceQueueName(jobID, String.valueOf(hash));
		
    		if (queues.containsKey(queueName)) {
    			reduceQueue = queues.get(queueName);
    		}
    		else {
    			reduceQueue = queueManager.getQueue(queueName, true, 1, QueueManager.QueueType.REDUCE, null, taskID + "_map" + mapId, workers);
    			queues.put(queueName, reduceQueue);
    		}
    		
    		// SQS supports limited Unicode, see http://www.w3.org/TR/REC-xml/#charsets, has to encode both key and value to get around
    		// encode conservatively using UTF-8, someone more familiar with encoding need to revisit
    		// Encode separately so that the separator could take on characters not in UTF-8 but accepted by SQS
    		try {
    			reduceQueue.push(URLEncoder.encode(key, "UTF-8") + Global.separator + URLEncoder.encode(value, "UTF-8"));
    		}
    		catch (Exception ex) {
        		logger.error("Message encoding failed. " + ex.getMessage());
    		}

		}
		
		public void close() {
			// update the reduce queue processed count to SimpleDB 
			dbManager.updateReduceOutputPerMap(jobID, taskID, mapId, reduceQStat);
			try {
		    	for (SimpleQueue queue : queues.values()) {
		    		queue.flush();   // Efficient queues need to be cleared
		    	}
				this.workers.waitForFinish();
				this.workers.close();  // flush the queue and kill the threads
			} catch (Exception e) {
				logger.warn("Fail to close worker: " + e.getMessage());
			}
		}	
	}
	
	public class CombineCollector implements OutputCollector {
		
		private static final long maxCombineMemorySize = 64*1024*1024;
		private Reducer combiner;
		private MapCollector output;
		private HashMap<String, Object> combineStates = new HashMap<String, Object>();
		private long memorySize = 0;
		
		public CombineCollector(Reducer combiner, MapCollector output) {
			this.combiner = combiner;
			this.output = output;     // This is the normal Map collector, write to the reduce queue
		}
		
		public void collect(String key, String value) throws Exception {
			perf.incrementCounter("combineMapOutputRecords", 1);
			
			if (combiner == null) {  // if no combiner, just use Map collector as normal
				output.collect(key, value);
				return;
			}
			if (!combineStates.containsKey(key)) {
				Object state = combiner.start(key, output);
				combineStates.put(key, state);
				memorySize += combiner.getSize(state) + key.length();
			}
			Object state = combineStates.get(key);
			long stateSize = combiner.getSize(state);
			combiner.next(key, value, state, output, perf);
			memorySize += combiner.getSize(state) - stateSize;  // account for the memory increase due to State 
			if (memorySize >= maxCombineMemorySize) {
				flush();
			}
		}
		
		public void flush() {
			if (combiner == null) {
				return;
			}
			for (Entry<String, Object> entry : combineStates.entrySet()) {
				try {
					combiner.complete(entry.getKey(), entry.getValue(), output);
				}
				catch (Exception e) {
					logger.warn("Combiner exception: " + e.getMessage());
				}
			}
			combineStates.clear();
			memorySize = 0;
		}
	}

	public class ReduceCollector implements OutputCollector {

		private SimpleQueue outputQueue;
		
		public ReduceCollector(SimpleQueue outputQueue) {
			this.outputQueue = outputQueue;
		}
		
		synchronized public void collect(String key, String value) throws IOException {
			perf.incrementCounter("reduceOutputRecords", 1);
			
			// collect final key-value pair and put in output queue 
			// enqueue to outSQS
    		try {
    		    
    			outputQueue.push(URLEncoder.encode(key, "UTF-8") + Global.separator + URLEncoder.encode(value, "UTF-8"));
    		}
    		catch (Exception ex) {
        		logger.error("Message encoding failed. " + ex.getMessage());
    		}
		}
	}
	
	/**
	 * Instantiate MapReduce class by initializing amazon cloud authentication
	 * 
	 * @param dbManager SimpleDB interface to manage job progress
	 * @param queueManager SQS interface to manager I/O
	 */
	public MapReduce(String jobID, DbManager dbManager, QueueManager queueManager, SimpleQueue inputQueue, SimpleQueue outputQueue,String s3Path) {
		this.jobID = jobID;
		this.dbManager = dbManager;
		this.queueManager = queueManager;
		// initialize thread pool for map and reduce workers
		this.mapWorkers = new WorkerThreadQueue(Global.numLocalMapThreads, "mapWorkers");
		this.reduceWorkers = new WorkerThreadQueue(Global.numLocalReduceThreads, "reduceWorkers");
		this.queueWorkers = new WorkerThreadQueue(20, "queueWorkers");  // fix at a constant for now
		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;
		Global.endCurrentJob=false;
		this.endCurrentJob=false;
		Global.numOfInvokedMappers=0;
		Global.numOfInvokedReducers=0;
		this.s3Path=s3Path;
	}
	
    private class CreateQueueRunnable implements Runnable {
    	private String jobID;
    	private int reduceQId;
    	
    	public CreateQueueRunnable(String jobID, int reduceQId) {
    		this.jobID = jobID;
    		this.reduceQId = reduceQId;
		}
    	
    	public void run() {
			masterReduceQueue.push(String.valueOf(reduceQId));
			queueManager.getQueue(getSubReduceQueueName(jobID, (String.valueOf(reduceQId))), false, 1, QueueManager.QueueType.REDUCE, null, null, queueWorkers).create();
			logger.debug(reduceQId + ".");
    	}
    }

	// The setup nodes split the task of populating the master reduce queue and creating the reduce queues
	private void setup(int numReduceQs, int numSetupNodes) {
		masterReduceQueue = queueManager.getQueue(getReduceQueueName(jobID), false, 1, QueueManager.QueueType.MASTERREDUCE, null, null, queueWorkers);
		masterReduceQueue.create();
		
		// Setup phase
		long setupTime = perf.getStartTime();
		logger.info("start setup phase");
		
		queueManager.cacheExistingQueues(jobID);
		
		if (Global.clientID < numSetupNodes) {
			String taskID = "setup_" + Global.clientID;
			dbManager.startTask(jobID, taskID, "setup");
			int interval = numReduceQs / numSetupNodes;
			int start = Global.clientID*interval;
			int end = Global.clientID + 1 == numSetupNodes ? numReduceQs : (Global.clientID*interval)+interval;
			
			//Dev comment:For Performance	logger.info("Creating reduce queues: ");
			for (int f = start; f < end; f++) {
				queueWorkers.push(new CreateQueueRunnable(jobID, f));
			}
			queueWorkers.waitForFinish();
	
			dbManager.completeTask(jobID, taskID, "setup");
		}
		queueWorkers.waitForFinish();
		
		// This is a critical synchronization point, everyone has to wait for all setup nodes 
		// (including the first one, which sets up the input/output queues) finish before proceeding
		dbManager.waitForPhaseComplete(jobID, "setup", numSetupNodes);
		perf.stopTimer("setupTime", setupTime);
	}
	// Naming convention for the master reduce queue
	private String getReduceQueueName(String jobID) {
		return jobID + "_reduceQueue";
	}
	// Naming convention for the reduce queues
	private String getSubReduceQueueName(String jobID, String suffix) {
		return jobID + "_reduceQueue" + "_" + suffix;
	}

	// Synchronized access to get ReduceQ size to reduce getReduceQSize implementation complexity
	synchronized int getReduceQSize (String jobID, int bucket, HashSet<String> committedMap) {
		return dbManager.getReduceQSize(jobID, bucket, committedMap);
	}
	
	/**
	 * The MapReduce interface that supports combiner
	 * 
	 * @param map the Map class
	 * @param reduce the Reduce class
	 * @param inputQueue the queue storing inputs 
	 * @param outputQueue the output queue where results go
	 * @param jobID an unique string to identify this MapReduce job. It is used as a prefix to all intermediate SQS queues so that two MapReduce jobs will use different SQS queues for intermediate results.
	 * @param numReduceQs number of reduce queues (size of the master reduce queue)
	 * @param numSetupNodes setup nodes split the task of creating the reduce queues and populating the master reduce queue
	 * @param combiner the Combiner class, typically the same as the Reduce class
	 */
	
		//Devendra made these varibales final (map, reduce and combiner)
	public void mapreduce(final Mapper map, final Reducer reduce, 
			 int numReduceQs, int numSetupNodes, final Reducer combiner,String accessKeyId,String secretAccessKey) {
		
		this.numReduceQs = numReduceQs;  // TODO, move it into the constructor??
		
		SimpleQueue masterReduceQueue = queueManager.getQueue(getReduceQueueName(jobID), false, 1, QueueManager.QueueType.MASTERREDUCE, null, null, queueWorkers);
		
		ReduceCollector reduceCollector = new ReduceCollector(outputQueue);

		// Setup
		setup(numReduceQs, numSetupNodes);
		
		// assign task ID
		taskID = "task" + Global.clientID;

		// count overall MapReduce time
		long mapReduceTime = perf.getStartTime();
	
		// MAP phase
		long mapPhase = perf.getStartTime();
		//Dev comment:For Performance logger.info("start map phase heay:");
		//dbManager.startTask(jobID, taskID, "map");   // log as one map worker 
		/* 
		 * ********************************************************************************************************************************
		 * Devendra Dahiphale: 
		 * newly added code
		 * This block is made a thread for pipelining purpose
		 */
		new Thread(new Runnable(){
			public void run()
			{
			//Devendra: new 	while(true){
				try {
					 do{
							// while input SQS not empty, dequeue, then map
							for (Message msg : inputQueue) {
								String value = msg.getBody();
								// get the key for the key/value pair, in our case, key is the mapId
								int separator = value.indexOf(Global.separator);   // first separator
								int mapId = Integer.parseInt(value.substring(0, separator));
								value = value.substring(separator+Global.separator.length());
								logger.debug(mapId + ".");
							   	mapWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here
								logger.info("**Mapper no " + mapId + " is invoked");
						   		mapWorkers.push(new MapRunnable(map, combiner, Integer.toString(mapId), value, perf, mapId, msg.getReceiptHandle()));
						   		Global.numOfInvokedMappers++;  //Devendra: to keep local job progress status
							}
							Thread.sleep(1000);//Devendra: wait little bit for records to appear in input SQS
							// SQS appears to be empty, but really should make sure we processed all in the queue
						} while (!Global.endCurrentJob);//Devendra: ! dbManager.isStageFinished(jobID, Global.STAGE.MAP) );  // if still has work left in theory, go back bang the queue again
					}catch (Exception e) {
			        	logger.warn("Map Exception: " + e.getMessage());
					}
		//Devendra: new }		
			}
		}).start();
		
	   /*
	     *****************************************************************************************
		 * Devendra Dahiphale:
		 * newly added code to statically bound reducers to the reduce queue.
		 * Not exactly static binding it's virtual static binding
		 * i.e reducer will continuously update the visibility timeout of master reduceQ so they behave as statically bound
		 * will be useful in reducer failure handling  
		 */
		long reducePhase = perf.getStartTime();
	//	while(Global.numOfInvokedReducers < Global.numLocalReduceThreads)
		for (Message msg:masterReduceQueue){
				String bucket;
					//Devendra: Check if message returned by masterReduceQueue is not earlier due to eventual consistancy ( same message can be read more than once )
					//atleast local conflict can be handled
				if(!reduceQsBoundWithReducers.contains(bucket=msg.getBody()))
				{	
					reduceWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here ( changed location by Devendra ) was 
					reduceWorkers.push(new ReduceRunnable(jobID, bucket, reduce, reduceCollector, msg.getReceiptHandle()));
					Global.numOfInvokedReducers++;
					logger.info("**Reducer " + bucket + " is invoked and bound with " + bucket + " Reduce Queue");
					reduceQsBoundWithReducers.add(bucket);
					
				}
		}
		//********************************************************************************************
		// read from input SQS
		/* Devendra:
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
					
			   		mapWorkers.push(new MapRunnable(map, combiner, Integer.toString(mapId), value, perf, mapId, msg.getReceiptHandle()));
			   		mapWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here
				}
				// SQS appears to be empty, but really should make sure we processed all in the queue
			} while ( ! dbManager.isStageFinished(jobID, Global.STAGE.MAP) );  // if still has work left in theory, go back bang the queue again
		}
		catch (Exception e) {
        	logger.warn("Map Exception: " + e.getMessage());
		} 
		
		 // not needed anymore, we wait in getReduceQsize for all counts
		// sync all maps to make sure they finish updateCompletedReduce
		dbManager.completeTask(jobID, taskID, "map");    
		dbManager.waitForPhaseComplete(jobID, "map", 0);

		// Get list of valid reduce messages
 		committedMap = dbManager.getCommittedTask(jobID, Global.STAGE.MAP);
		
		// stop map phase
		queueManager.report(); */
		
		/*
		 * Devendra: Wait till user wants to terminate job. 
		 * calling snapshot module on user request is added in the main thread ( down here )
		 * also job progress track is kept here so that user will come to know when all data 
		 * to be processed is complete processed. for terminating job on user command user has to upload 
		 * an empty file in a folder "control" of the input path with name "stop"( without extension ).
		 * for now job is terminated after current data to be processed is finished
		 */
		
		while(!Global.endCurrentJob){
			try{
				  //Devendra: For a snapshot request new Snapshot(outputQueue,accessKeyId,secretAccessKey,s3Path)check wheather all the reducer has given their output in outputSQS
			//	reducerProgress.add((String.valueOf(System.currentTimeMillis()-Global.timeTOCopleteJob) + " " + String.valueOf(Global.numOfRecordsProcessedByReducers)));
			//	mapperProgress.add((String.valueOf(System.currentTimeMillis()-Global.timeTOCopleteJob) + " " + String.valueOf(Global.numOfRecordsProcessedByMappers)));
				if(Global.numOfInvokedReducers > 0 && Global.numberOfReducerGivenOutputForCurrentSnapshotRequest== Global.numOfInvokedReducers){
					
					logger.info("**A snapshot request serving is in process");
					outputQueue.flush();  // output queue is an efficient queue, need to flush to clear
					new Snapshot(outputQueue,accessKeyId,secretAccessKey,s3Path).ShowSnapshot(); 
					
					Global.numberOfReducerGivenOutputForCurrentSnapshotRequest=0; 
				}
			committedMapForJobProgressTrace = dbManager.getCommittedTask(jobID, Global.STAGE.MAP);
			if(committedMapForJobProgressTrace.size() == Global.numSplit){
				if( Global.numFinishedReducers == numReduceQs){
					
					logger.info("**Local reducers are done with their queues");
					committedReducerForJobProgressTrace = dbManager.getCommittedTask(jobID, Global.STAGE.REDUCE);
					logger.info("**completed reducers are : " + committedReducerForJobProgressTrace.size() );
					if(committedReducerForJobProgressTrace.size()==numReduceQs){
						logger.info("***Current input of the job is completely processed" ); // if you want to terminate the job upload 'stop' named file in control folder of input path");
						 //Devendra : make termination status true when all data is finished  for now, will be 
					// Global.endCurrentJob=true;
				   	 //	endCurrentJob=true;
						logger.info("***can stop now");
					}
				}
					logger.info("*in main thread polling ");
			}
				Thread.sleep(500);
			}catch(Exception e){
				logger.info("Main thread is interrupted" + e.getMessage());
			}
		}
		perf.stopTimer("mapPhase", mapPhase);
		 
		 //Devendra: Wait till all reducers have written their available output closed properly 
	/*	while(!endCurrentJob){
			try{
				logger.info("can stop now");
				committedMap = dbManager.getCommittedTask(jobID, Global.STAGE.MAP);
				if(committedMap.size()==)
				committedReducerForJobProgressTrace = dbManager.getCommittedTask(jobID, Global.STAGE.REDUCE);
				if(committedReducerForJobProgressTrace.size()==numReduceQs)
					endCurrentJob=true;
				Thread.sleep(250);
			}catch(Exception e){
				logger.info("Main thread is interrupted" + e.getMessage());
			}
		} */

		// REDUCE phase
	//	long reducePhase = perf.getStartTime();
	/*	logger.info("start reduce phase");
		try {			
			do {
				// dequeue one reduce key , generate a iterator of values
				for (Message msg : masterReduceQueue) {
					String bucket = msg.getBody();
			   		reduceWorkers.push(new ReduceRunnable(jobID, bucket, reduce, reduceCollector, msg.getReceiptHandle()));
			   		reduceWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here
				}
			} while ( !dbManager.isStageFinished(jobID, Global.STAGE.REDUCE));
			reduceWorkers.waitForFinish();
		}
		catch ( Exception ex ) {
			logger.warn("Reduce Exception: " + ex.getMessage());
		}
		// stop reduce phase
	   dbManager.completeTask(jobID, taskID, "reduce");
	   dbManager.waitForPhaseComplete(jobID, "reduce",0);
		
		outputQueue.flush(); */  // output queue is an efficient queue, need to flush to clear
	/*	reducerProgress.add((String.valueOf(System.currentTimeMillis()-Global.timeTOCopleteJob) + " " + String.valueOf(Global.numOfRecordsProcessedByReducers)));
		mapperProgress.add((String.valueOf(System.currentTimeMillis()-Global.timeTOCopleteJob) + " " + String.valueOf(Global.numOfRecordsProcessedByMappers)));
		snapshotObj.writeDataForGraph("mapperProgressLog" + System.currentTimeMillis() + ".txt", mapperProgress);
		snapshotObj.writeDataForGraph("reducerProgressLog" + System.currentTimeMillis() + ".txt",reducerProgress);*/
		
	/*	try{
			reduceWorkers.waitForFinish();
			reduceWorkers.close();
		}catch(Exception e){
			logger.error("Error while closing reducer threads" + e.getMessage());
		} */
		perf.stopTimer("reducePhase", reducePhase);	
		perf.stopTimer("mapReduce", mapReduceTime);

		queueManager.perf.report();
		perf.report();
		dbManager.perf.report();
		long temp = System.currentTimeMillis()- Global.timeTOCopleteJob;
		logger.info("*****Time Required to complete job is:" + temp);
	}
    private class MapRunnable implements Runnable {
		private Mapper map;
		private Reducer combiner;
		private String key;
    	private String value;
    	private PerformanceTracker perf;
    	private int mapId;
    	private String receiptHandle;
    	
    	public MapRunnable(Mapper map, Reducer combiner, String key, String value, PerformanceTracker perf, int mapId, String receiptHandle) {
    		this.map = map;
    		this.combiner = combiner;
    		this.key = key;
    		this.value = value;
    		this.perf = perf;
    		this.mapId = mapId;
    		this.receiptHandle = receiptHandle;
		}
    	
    	public void run() {	
    		try {
				long mapLocal = perf.getStartTime();
    			MapCollector mapCollector = new MapCollector(numReduceQs, mapId);
    			CombineCollector combineCollector = new CombineCollector(combiner, mapCollector);
				long mapFunction = perf.getStartTime();     // for profiling purpose
				map.map(key, value, combineCollector, perf);
				perf.stopTimer("mapFunction", mapFunction);
				// make sure all results are saved in SQS
				combineCollector.flush();
				// commit the change made by this map task
				dbManager.commitTask(jobID, taskID, mapId, Global.STAGE.MAP);
				mapCollector.close();   // close worker threads, update stats
				// Delete message from input queue, important to delete after commit in case failure
				inputQueue.deleteMessage(receiptHandle);
				perf.stopTimer("mapLocal", mapLocal);
    		}
			catch (Exception e) {
	        	logger.warn("MapRunnable Exception: " + e.getMessage());
			}
    	}
    }
    private class ReduceRunnable implements Runnable {
    	private String jobID;
    	private String bucket;  // the reduce queue number this runnable is responsible for
    	private Reducer reduce;
    	private OutputCollector collector;
    	private HashMap<String, Object> reduceStates;
    	private String receiptHandle;
    	private long snapshotRequestsServed;
    	
    	
    	public ReduceRunnable(String jobID, String bucket, Reducer reduce, OutputCollector collector, String receiptHandle) {
    		this.jobID = jobID;
    		this.bucket = bucket;
    		this.reduce = reduce;
    		this.collector = collector;
			this.reduceStates = new HashMap<String, Object>();
			this.receiptHandle = receiptHandle;   // receipt handle for the message in the master reduce queue
			this.snapshotRequestsServed=0;//Devendra: Initially zero requests are served
		}
    	
    	public void run() {
    		int total = 0;//getReduceQSize(jobID, Integer.parseInt(bucket), committedMap);
			int count = 0;
			int oldcount = 0;
			int committedMappers = 0;
			int tempVar=0;
			int numOfPasses=0;
			boolean iAmDone=false;
			long reduceLocal= perf.getStartTime();
				// allocate worker
			WorkerThreadQueue workers = new WorkerThreadQueue(Global.numDownloadWorkersPerReduce, "reduce" + bucket);
				// 	A reduce queue could contain multiple reduce keys
				// set numReduceQReadBuffer high to request a lot to parallelize
			SimpleQueue value = queueManager.getQueue(getSubReduceQueueName(jobID, bucket), true, Global.numReduceQReadBuffer, QueueManager.QueueType.REDUCE, committedMap, null, workers);
  
    		try {
   
    	  /*Devendra: changed location
    	    	committedMap = dbManager.getCommittedTask(jobID, Global.STAGE.MAP);
				// total and count are for tracking the number of entries in a reduce queue
			    total = getReduceQSize(jobID, Integer.parseInt(bucket), committedMap);

				// allocate worker
				WorkerThreadQueue workers = new WorkerThreadQueue(Global.numDownloadWorkersPerReduce, "reduce" + bucket);
				// A reduce queue could contain multiple reduce keys
				// set numReduceQReadBuffer high to request a lot to parallelize
				SimpleQueue value = queueManager.getQueue(getSubReduceQueueName(jobID, bucket), true, Global.numReduceQReadBuffer, QueueManager.QueueType.REDUCE, committedMap, null, workers);
		    	//	long reduceLocal = perf.getStartTime();
		   * */
    					do {
    						
    							//Devendra: this condition is for avoiding unwanted accesses to simpleDB, if all mappers are already committed then no need to get them again 
    						   if(committedMappers < Global.numSplit){
    							   
    							   committedMap = dbManager.getCommittedTask(jobID, Global.STAGE.MAP);
    							     //Devendra: sometimes we can get not true value so keep previous as it is 
    							   committedMappers =((tempVar=committedMap.size())>committedMappers?tempVar:committedMappers);
    							   	//total and count are for tracking the number of entries in a reduce queue		
    							   total = ((tempVar=getReduceQSize(jobID, Integer.parseInt(bucket), committedMap))>total?tempVar:total);
    						   
    						   }
    						      
    						    for (Message msg : value) {
    							// RBK: only for graph plotting, may be removed later 
    							Global.jobProgressTracker.incrementNumRecordsProcessedByReducers();
    						//    dbManager.incrementNUmberOfRecordsProcessedByReducers();
    							String keyVal = msg.getBody();
    							count ++ ;
    							int sep = keyVal.lastIndexOf(Global.separator);  // could choose indexOf too, should be unique in theory
    							String key = keyVal.substring(0, sep);
    							String val = keyVal.substring(sep + Global.separator.length());
    							
    							// 	decode message as it was encoded when pushed to SQS
    							try {
    								key = URLDecoder.decode(key, "UTF-8");
    								val = URLDecoder.decode(val, "UTF-8");
    							}
    							catch (Exception ex) {
    								logger.error("Message decoding failed. " + ex.getMessage());
    							}
    							if (!reduceStates.containsKey(key)) {
    								long reduceStart = perf.getStartTime();
    								Object state = reduce.start(key, collector);
    								perf.stopTimer("reduceStart", reduceStart);
    								reduceStates.put(key, state);
    							}
    							long reduceNext = perf.getStartTime();
    							reduce.next(key, val, reduceStates.get(key), collector, perf);
    							perf.stopTimer("reduceNext", reduceNext);
    						}
    						if ( oldcount != count ) {
    							logger.debug(bucket + ": Processed " + count + " When mappers are completed :" + committedMappers);
    							oldcount = count;
    							if(iAmDone){ 
    								dbManager.updateFinishedReducersCount("decrement");
    								iAmDone=false;
    							}
    						}
    					//	else
    					//		numOfPasses++;  //Devendra: TO ensure all records from reduceQ are processed
    						
    						if ( count < total ){
    							 	Thread.sleep(250);
    							if(committedMappers < Global.numSplit)//Devendra: if mappers are not finished sleep little bit more, so that mappers will get chance to produce some data
    								Thread.sleep(250);
    							
    						}
    						
						// sleep a little bit to wait
				/*	if ( emptypass > 5 )  { // should we start conflict resolution process?
						perf.incrementCounter("attemptConflictResolution", 1);   // count how often we do this
						logger.info("start conflict resolution");
						emptypass = 0;   // do not bother again for another round
						dbManager.claimReduce(jobID, taskID, bucket, receiptHandle);
						String winner = dbManager.resolveReduce(jobID, bucket);
						logger.info("resolution winner is " + winner);
						if ( winner != null && ! winner.startsWith(taskID) ) {  // if we are not the winner, clean and quit
							perf.incrementCounter("lostReduceConflictResolution", 1);
							// Do not need this with the current design, only one entry for each number in the master reduce queue
							/*
							String winningReceipt = winner.substring(winner.indexOf('_')+1);
							if ( winningReceipt != receiptHandle ) {
								perf.incrementCounter("lostReduceConflictResolutionBecauseOfDup", 1);
								logger.info("Remove duplicate master reduce message " + receiptHandle);
								// must have been duplicate message in the master reduce queue, remove the redundant one, but not the primary one in case of failure
								masterReduceQueue.deleteMessage(winningReceipt);
							}outputQueue
							return;
						}
					}*/
    						/*
    						 * Devendra: only true when a reuest for snapshot will made and to ensure a reducer will write it's output
    						 * only once for a request from user ( ensures no multiple copies from same resucers will be given to user
    						 */
    						if(Global.snapshotRequestNumber > snapshotRequestsServed){
    							for (Entry<String, Object> entry : reduceStates.entrySet()) {
    								long reduceComplete = perf.getStartTime();
    								reduce.complete(entry.getKey(), entry.getValue(), collector);
    								perf.stopTimer("reduceComplete", reduceComplete);
    							}
    							logger.info("snapshot : " + bucket + " reducer has commited with request number " + Global.snapshotRequestNumber + " local number " + snapshotRequestsServed);
    							snapshotRequestsServed=Global.snapshotRequestNumber; //Devendra: TO keep track of snapshot service number
    							dbManager.updateOutputCommittedReducerCount();
    						}
    						//Dev: check if this reducer has already committed or not (used for getting job progress status and job completion status)
        					if(!iAmDone && count >= total && committedMappers==Global.numSplit ){//&& numOfPasses > 1){
        						dbManager.updateFinishedReducersCount("increment");//Dev: if not then commit 
        						iAmDone=true;
        					} 
    						logger.info("***In Run of Reducer " + bucket  );
        					if(Global.endCurrentJob)//Devendra: special case, user request for terminating job is entertained with considering job status 
        						break;
    					} while ( !Global.endCurrentJob || count < total || committedMap.size()!=Global.numSplit);// || numOfPasses < 2);  // queue may not be empty because of eventual consistency
    		if ( count > total )
    				logger.warn("Reduce queue " + bucket + " processed more than available: " + count + " vs. " + total);
				
    		}catch (Exception e) {
    				logger.warn("ReduceRunnable Exception: " + e.getMessage());
    		}
    		try{
    			for (Entry<String, Object> entry : reduceStates.entrySet()) {
    				long reduceComplete = perf.getStartTime();
    				reduce.complete(entry.getKey(), entry.getValue(), collector);
    				perf.stopTimer("reduceComplete", reduceComplete);
    			}
    		}catch(Exception e){
    			logger.error("An error occured while writing output in output queue" + e.getMessage());
    		}
    		try{
    			    workers.close();  // No need to wait for finish because we only download. The fact that we have downloaded all data means the workers have finished
    		}catch(Exception e){
    			    logger.error("An exception is occured in closing downloader workers of " + bucket + "reducer" + e.getMessage());
    		}
    	
    		reduceStates.clear();
    		perf.stopTimer("reduceLocal", reduceLocal);
    		  //Devendra: still need to commit, used to track reducer progress when job will be running on multiple instances
    		dbManager.commitTask(jobID, taskID, Integer.parseInt(bucket), Global.STAGE.REDUCE);
    		masterReduceQueue.deleteMessage(receiptHandle);  // delete what we processed
    	}
    }

}			