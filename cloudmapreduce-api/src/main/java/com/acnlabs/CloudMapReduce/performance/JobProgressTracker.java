package com.acnlabs.CloudMapReduce.performance;
import java.util.*;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.*;

public class JobProgressTracker implements Runnable{
			
		private long numRecordsGeneratedByMappers=0;	//total number-cumulative of all mappers
		private long numRecordsProcessedByReducers=0;	//total number-cumulative of all reducers
		private long progressTrackingInterval=1000;
		private ArrayList<String> mapperPerfList=new ArrayList<String>();
		private ArrayList<String> reducerPerfList=new ArrayList<String>();
		
		//for S3:
		
		private static String s3Path;	//take a single s3 path-must be a folder. upload 2 files- mapperProgressLog and reducerProgressLog
		private static S3FileSystem s3FileSystem;
		private static S3Item s3Item;
		private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.JobProgressTracker");
		
		//s3path must be a filename
		public JobProgressTracker(long progressTrackingIntervalInMillis,String s3Path, String accessKeyId,String secretAccessKey){	//the interval in milliseconds after which the thread makes a new entry in the log
			
			this.progressTrackingInterval=progressTrackingIntervalInMillis;
			this.s3Path=s3Path;	//RBK TODO take from user as a command line argument. Must be a folder-ending in "/"
			s3FileSystem=new S3FileSystem(accessKeyId,secretAccessKey);
			s3Item=s3FileSystem.getItem(s3Path);
			s3Item.upload("mapperProgressLog.txt",(String.valueOf(System.currentTimeMillis()/1000) + " " + String.valueOf(0)).getBytes());
			s3Item.upload("reducerProgressLog.txt",(String.valueOf(System.currentTimeMillis()/1000) + " " + String.valueOf(0)).getBytes());
				
		}
		
		public void run(){
			while(true){
				logger.info("run madhe");
				try {
					Thread.sleep(progressTrackingInterval);
					
					mapperPerfList.add(String.valueOf(System.currentTimeMillis()/1000) + " " + String.valueOf(numRecordsGeneratedByMappers));
					reducerPerfList.add(String.valueOf(System.currentTimeMillis()/1000) + " " + String.valueOf(numRecordsProcessedByReducers));
					
					if(mapperPerfList.size()>=100){
						/* untested code: moved to updateFile()
						 * S3Item fileToWriteMapperProgressTo=s3FileSystem.getItem(s3Item.getPath() + "/mapperProgressLog.txt");
						String fileData=fileToWriteMapperProgressTo.getData();
						for(String s:mapperPerfList){
							fileData=fileData+"\n"+s;
						}
						fileToWriteMapperProgressTo.delete();
						s3Item.upload(s3Item.getPath()+"/mapperProgressLog.txt", fileData.getBytes());
						
						mapperPerfList.clear();	//IMP REM.
						*/
						updateFile("mapperProgressLog.txt",mapperPerfList); //dont give /mapperProgressLog as the / is included in the s3Itempath??
						mapperPerfList.clear();
					}
					if(reducerPerfList.size()>=100){
						/* untested code: moved to updateFile()
						S3Item fileToWriteReducerProgressTo=s3FileSystem.getItem(s3Item.getPath() + "/reducerProgressLog.txt");
						String fileData=fileToWriteReducerProgressTo.getData();
						for(String s:reducerPerfList){
							fileData=fileData+"\n"+s;
						}
						fileToWriteReducerProgressTo.delete();
						s3Item.upload(s3Item.getPath()+"/reducerProgressLog.txt", fileData.getBytes());
						
						reducerPerfList.clear();	//IMP REM.
						*/
						updateFile("reducerProgressLog.txt",reducerPerfList);
						reducerPerfList.clear();
						
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		public void incrementNumRecordsGeneratedByMappers(){
			numRecordsGeneratedByMappers++;
		}
		public void incrementNumRecordsProcessedByReducers(){
			numRecordsProcessedByReducers++;
		}
		private void updateFile(String filename, ArrayList<String> newDataList){	
			logger.info("\n\n\npath is: " +s3Item.getPath() + filename);
			
			S3Item fileToWriteProgressTo=s3FileSystem.getItem(s3Item.getPath() + filename);
			String fileData=fileToWriteProgressTo.getData();
			for(String s:newDataList){
				//logger.info("\n\ncurrent file data: " + fileData);
				fileData=fileData+"\n"+s;
			}
			fileToWriteProgressTo.delete();
			//s3Item.upload(s3Item.getPath()+filename, fileData.getBytes()); //NO just the filename. s3Item itself has the necessary hierarchy in it
			s3Item.upload(filename, fileData.getBytes()); 
		//	newDataList.clear();	//IMP REM.
			//this clear is called in the CALLING function, as i wasn't sure if the reference to the list is passed, or a copy
			
		}
}

