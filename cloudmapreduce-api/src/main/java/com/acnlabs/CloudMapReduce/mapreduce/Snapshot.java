package com.acnlabs.CloudMapReduce.mapreduce;

import java.util.ArrayList;
import java.util.HashSet;

import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.QueueManager;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.S3FileSystem;
import com.acnlabs.CloudMapReduce.S3Item;
import com.amazon.s3.*;
//import com.amazon.services.s3;
import com.amazonaws.queue.model.Message;
import org.apache.log4j.Logger;

/*
 * Devendra: This module serves user request for available output of job at
 * any time in job span, called as snapshot of job. A copy of available output
 * is given to the user as many times as user requests 
 * output will be multiple files in a output directory ( given by the user )
 * and each file is associated with a node from cluster nodes on which job runs
*/

public class Snapshot{
	
			private SimpleQueue outputQueue;
			String accessKeyId;
			String secretAccessKey;
			private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduce");
			private S3FileSystem s3FileSystem;
			private String s3Path;
			AWSAuthConnection conn;
			private int numberOfPasses;
			private boolean localFlag;
	
			public Snapshot(SimpleQueue outputQueue,String accessKeyId, String secretAccessKey,String s3Path)
			{
				this.outputQueue = outputQueue;
				this.s3Path=s3Path;
				this.accessKeyId=accessKeyId;
				this.secretAccessKey=secretAccessKey;
				this.numberOfPasses=0;
				this.localFlag=false;
				s3FileSystem=new S3FileSystem(accessKeyId, secretAccessKey);
			}
			public void ShowSnapshot()
			{
				   
					//Devendra: If output is not available then a problem can occure while uploading zero byte file so made it nonempty
				String outputString="Available output : ";
				//reading from outputQueue passed as param
				logger.info("**In Snapshot serving module getting data");
				try{
				// do{
					   //Devendra: Reading data from output queue and store it into local string
					for (Message msg : outputQueue) { 
				//		localFlag=true;
						String keyValuePair = msg.getBody();
						outputString=outputString + "\n" + keyValuePair.substring(0, keyValuePair.indexOf('!')) + " " + keyValuePair.substring(keyValuePair.indexOf('!')+3);
					}
				//	if(localFlag==false)
				//	   numberOfPasses++;
				//	else
				//		break;
				// }while(numberOfPasses < 2);//Devendra: Check out output queue more than once to tackle eventual consistency problem of SQS
					
						// creating bucket and uploading file NOTE: its static right now, make it dynamic, TBD
					if(outputString.length() > 25){
						
						logger.info("**Snapshot_thread :" + s3FileSystem.getItem(s3Path.substring(0,s3Path.substring(1).indexOf('/') +2) + "output/") );
						S3Item s=s3FileSystem.getItem(s3Path.substring(0,s3Path.substring(1).indexOf('/') +2) + "output/");
						s.upload("snapshot" + System.currentTimeMillis() + ".txt", outputString.getBytes());
					}
					else
					{
						logger.info("**Snapshot_thread : No output available ");
					}
				}
				catch(Exception e)
				{
					logger.warn(e + "Error in snapshot module");
				}
				
			}
			public void writeDataForGraph(String fileName,ArrayList<String> graphData){
			
					S3Item s3Item=s3FileSystem.getItem(s3Path.substring(0,s3Path.substring(1).indexOf('/') +2) + "JobProgressTrackerData/");
					String fileData="\n";
					for(String s:graphData){
						fileData=fileData + "\n" + s;
					}
					s3Item.upload(fileName, fileData.getBytes()); 
					
		
			}
}
