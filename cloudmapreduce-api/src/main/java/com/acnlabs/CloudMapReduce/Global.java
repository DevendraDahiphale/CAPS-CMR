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
package com.acnlabs.CloudMapReduce;

import com.acnlabs.CloudMapReduce.performance.JobProgressTracker;

// Hosts global configuration variables. It is a lot easier to host them in a central place than passing them around

public class Global {

	// stage definition
	public enum STAGE {MAP, REDUCE};

	// Each node has a unique ID
	static public int clientID;
	// Specifies how many Map or Reduce threads to run respectively
	static public int numLocalMapThreads;
	static public int numLocalReduceThreads;
	// The local buffer size for reading from reduce queues, bigger == better hiding latency
	static public int numReduceQReadBuffer;
	static public int numUploadWorkersPerMap;
	static public int numDownloadWorkersPerReduce;
	static public boolean enableCombiner;
	static public int numSDBDomain;
	static public String outputQueueName; // used to identify which is the output queue that should not be deleted at the end
	
	// Visibility timeouts
	static public int mapQTimeout;  // timeout for pickup failed map tasks
	static public int reduceQTimeout;  // timeout to resume after conflict resolution
	static public int masterReduceQTimeout;  // timeout for pickup failed reduce tasks
	
	// Key-value pair separator, must be different from EfficientQueue's separator, and not substring of one another
	// This separator must not be a normal string that could be confused with a key or value
	static public final String separator = "!+!";
	
	   //Devendra: Added new variables
	static public int numSplit;  
	static public long numFinishedReducers;  //total number of finished local reduce workers
	static public long numFinishedMappers;   //total number of finished local map workers
	static public long snapshotRequestNumber; // For each request it will be incremented by one
	static public boolean endCurrentJob=false; //this is made true when user wants to teminate the job, notification track is kept in streamhandling module
	static public long numberOfReducerGivenOutputForCurrentSnapshotRequest; //for snapshot serving purpose
	static public long timeTOCopleteJob;   //keep total time required to complete the job
	static public long numOfInvokedMappers; //total number of invoked local mappers 
	static public long numOfInvokedReducers; //total number of invoked local reducers
	static public long numOfRecordsProcessedByMappers;
	static public long numOfRecordsProcessedByReducers;
	
	/*RBK: */
	static public JobProgressTracker jobProgressTracker;
}