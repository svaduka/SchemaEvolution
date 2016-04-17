package com.vodafone.scheduler;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.vodafone.constants.SEConstants;
import com.vodafone.util.FileUtil;
import com.vodafone.util.PropertyReader;

public class CronJob {

	/**
	 * @param args
	 */
	
	public static PropertyReader propReader = null;
	
	public static void main(String[] args) throws IOException {
		// Read Properties
		propReader = new PropertyReader("D:\\Naveen\\Workspace\\PRTS.properties");
		boolean propertiesLoaded = propReader.loadProperties();

		if(propertiesLoaded)
		{
			while (Boolean.TRUE) {
				boolean anyFilesProcessed=process(args);
				if(anyFilesProcessed){
					System.out.println("Some Files are processed please check in the archive dirctory with current timestamp");
				}
			}
		}
		
	}
	
	public static boolean process(final String[] args) throws IOException {
		
		boolean isAnyFilesProcessed=Boolean.FALSE;
		
		String BASE_LOC = propReader.getValue(SEConstants.BASE_LOC);
//		String ctlFile = null;
		List<File> files = FileUtil.getTriggerFiles(BASE_LOC);
		for (File file : files) {
			System.out.println(file.getAbsolutePath());
		}
		
		return isAnyFilesProcessed;
	}

}
