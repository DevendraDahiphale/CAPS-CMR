
/*
 * Devendra Dahiphale:
 * This module is for handling streaming data, periodically polls s3 for
 * new data and if it finds any spits up into user specified parts,
 * also keep track of user notification such as snapshot request and job 
 * termination request ( but snapshot request now works only for single node job, will be made for multiple node job) 
 */
package com.acnlabs.CloudMapReduce.application;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.S3Item;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.S3FileSystem;
public class StreamHandler implements Runnable{
	private S3FileSystem s3FileSystem;
	private String s3Path;
	private long numSplit;
	private SimpleQueue inputQueue;
	private ArrayList<String> preProcessedFileList = new ArrayList<String>();
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduceApp");
	private  int mapNum;
	private long dynamicSleepTime;
	public StreamHandler(String s3Path,SimpleQueue inputQueue,long numSplit,S3FileSystem s3FileSystem)
	{
		this.s3Path=s3Path;
		this.inputQueue=inputQueue;
		this.numSplit=numSplit;
		this.s3FileSystem=s3FileSystem;
		dynamicSleepTime=500;
		Global.snapshotRequestNumber=0;
	}
	public void run()
	{
		while(!Global.endCurrentJob)
		{
			try
			{
				ArrayList<S3Item> fileListToBeProcessed = new ArrayList<S3Item>();
				//Devendra: check for the new file and add to the fileListToBeProcessed
				if(addDirToList(s3FileSystem.getItem(s3Path),fileListToBeProcessed)) 
				{
					 //Dev: just testing purpose, print all new files found in this poll
					for(S3Item child:fileListToBeProcessed)
						logger.info("**A new file is found: " + child.getPath() );
					Global.numSplit+=numSplit;	//dev: to keep track of total number of mappers
					addSplits(fileListToBeProcessed); //Dev add pointer to splits in input queue
					if(dynamicSleepTime>500)  //dev: time should not go less than 500
						   dynamicSleepTime-=250; //Dev: gradually decrease sleep time for this thread
				}
					//Dev: Maximum sleep time does not exceed 2 min
				if(dynamicSleepTime<1000)
					dynamicSleepTime+=250;  //Dev: Gradually increase the sleep time for this thread
				Thread.sleep(dynamicSleepTime);
			}catch(Exception e){
				logger.info("Error in StreamHangler:" + e);
			}
		}
	}
	private boolean addDirToList(S3Item item,ArrayList<S3Item> fileListToBeProcessed){
		boolean newItem=false;
		if(item == null)
			return newItem;
		if (item.isDir()) {

			Collection<S3Item> children = item.getChildren(true);
			
			if (children.size() < 750) {
				for (S3Item child : children) {		
					if (!child.isDir() && !preProcessedFileList.contains(child.getPath())){
						if(checkForInputFileType(child,fileListToBeProcessed))
							newItem=true;
					}	
				}
			}
			else {
				for (S3Item child : item.getChildren()) {
					if (child.isDir()) {
						addDirToList(child, fileListToBeProcessed);
					}
					else {
						//Devendra: if previously processed item then do not add
						if(!preProcessedFileList.contains(child.getPath()))
						{
							if(checkForInputFileType(child,fileListToBeProcessed))
									newItem=true;
						}
					}
				}
			}
		}
		else {
			//Devendra: if previously processed item then do not add
			if(!preProcessedFileList.contains(item.getPath()))
			{
				if(checkForInputFileType(item,fileListToBeProcessed))
						newItem=true;
			}
		}
		return newItem; //Devendra: shows new item is found for processing if newItem is true
	}
			//Devendra: check if new file is for notification or data to be processed 
	private boolean checkForInputFileType(S3Item child,ArrayList<S3Item> fileListToBeProcessed){
			
			if((s3Path + "control/stop").equals(child.getPath())){
				logger.info("**A request to terminate job is encountered");
				Global.endCurrentJob=true;
				//Devendra: no need to add to preProcessedFileList, any way job is going to terminate
				return false;
			}
			else if((s3Path + "control/").equals(child.getPath().substring(0,((child.getPath().lastIndexOf('/') + 1))))){
				logger.info("**A request for snapshot is detected");
				//Devendra: do not delete file when notification is detected because multiple node may not have got notification yet 
				Global.snapshotRequestNumber++;
				preProcessedFileList.add(child.getPath());
				return false;
			}
			else{
				fileListToBeProcessed.add(child);
				/*
				 *  Devendra: adding child only doesn't work because each request for s3item 
				 *  will give different handle(a number) so added path which will be unique for same item
				 */
				preProcessedFileList.add(child.getPath());
				return true;
			} 
	}
	
	private void addSplits(ArrayList<S3Item> fileListToBeProcessed) {
		StringBuilder sb = new StringBuilder();
		
		long totalSize = 0;
		long splitSize=0;
		for (S3Item item : fileListToBeProcessed) {
			totalSize += item.getSize();
		}
		splitSize=totalSize/numSplit+1;
		logger.info("Total input file size: " + totalSize + ". Each split size: " + splitSize);
		
		long currentSize = 0;
		for (S3Item item : fileListToBeProcessed) {
			long filePos = 0;
			while (filePos < item.getSize()) {
				long len = Math.min(item.getSize() - filePos, splitSize - currentSize);
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append(item.getPath());
				sb.append(",");
				sb.append(filePos);
				sb.append(",");
				sb.append(len);
				filePos += len;
				currentSize += len;
				if (currentSize == splitSize) {
					inputQueue.push(mapNum + Global.separator + sb.toString());
					
					mapNum ++ ;
					currentSize = 0;
					sb = new StringBuilder();
				}
			}
		}
		if (sb.length() > 0) {
			//Prepend mapNum to uniquely identify each message
			inputQueue.push(mapNum + Global.separator + sb.toString());
			mapNum ++ ;
		}
	}
}