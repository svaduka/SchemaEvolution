package com.vodafone.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

	
	public static boolean copyFromLocalToHDFS(String srcNameWithLoc, String destNameWithLoc, Configuration conf)
	    	throws IOException{
	    		
			conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");
			FileSystem hdfs = FileSystem.get(conf);
	    	hdfs.copyFromLocalFile(new Path(srcNameWithLoc), new Path(destNameWithLoc));
	    	
	    	return Boolean.TRUE;
	}
	
}
