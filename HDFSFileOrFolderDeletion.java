//this code is an example on how to use hdfs java api

package com.arun.code;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.IOUtils;

public class HDFSFileOrFolderDeletion {

 private static final Logger logger = Logger.getLogger(hdfsFileDeletion.class);
   public static void main(String[] args) throws Exception {
	
		if (args.length < 3 ) {
			logger.error("Please pass 3 arguments, nbr of days, file path, FS Default name..");
			 throw new Exception ("Missing arguments.... Please check !!!!");
			 }
			 String retentionDays = args[0];
			 String dir = args[1];
			 String argStrFSDefaultName = args[2];
			 
			 try {
				cleanupDirs (retentionDays, dir, argStrFSDefaultName);
				} catch(Exception e) {
					logger.error("Error....:" + e.getMessage());
					e.printStackTrace();
					throw e;
				}
			}
			
			public static void cleanupDirs (String retDays, String inDir, String fsDefaultName) throws Exception {
			
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", fsDefaultName);
				FileSystem fsHDFS = FileSystem.get(conf);
				Path fsPath = new Path(inDir);
				if (fsHDFS.exists(fsPath))
				  {
					  FileStatus fsLst [] = fsHDFS.listStatus(fsPath);
					  for (FileStatus flStatus : fsLst) {
						Path tmpFile = flStatus.getPath();
						String fileNm = tmpFile.getName();
						long flSize = (fsHDFS.getContentSummary(tmpFile).getSpaceConsumed())/1024/1024;
						long flCnt = fsHDFS.getContentSummary(tmpFile).getFileCount();
						long modTim = flStatus.getModificationTime();
						long currTime = System.currentTimeMillis();
						long flAge = currTime - modTime;
						if (flAge > Long.parseLong(retDays)*24*3600*1000) 
						  {
						     logger.info(fileNm + " " + flSize + " " + flCnt);
							 if (fsHDFS.isDirectory(tmpFile)) {
								fsHDFS.delete(tmpFile,true);
							} else {
							         fsHDFS.delete(tmpFile);
							      }
							  }
						}
					}
				
				else
				 {
				    logger.error("Please check whether the input path is a valid one ");
					throw new Exception( "Input directory/file does not exist!");
				}
			}
         }
     
