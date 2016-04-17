package com.vodafone.scheduler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vodafone.constants.SEConstants;
import com.vodafone.exceptions.SERuntimeException;
import com.vodafone.util.FileUtil;
import com.vodafone.util.PropertyReader;

public class CronJob extends Configured implements Tool{

	/**
	 * @param args
	 */
	
	public static PropertyReader propReader = null;
	
	public static void main(String[] args) throws IOException {

		// Read Properties
		propReader = new PropertyReader("PRTS.properties");
		boolean propertiesLoaded = propReader.loadProperties();

		if (propertiesLoaded) {

			try {
				Configuration conf = new Configuration(Boolean.TRUE);
				int i = ToolRunner.run(conf, new CronJob(), args);
				if (i == 0) {
					System.out.println("Success");
				} else {
					System.out.println("Failed");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	
	@Override
	public int run(String[] args) throws Exception {

			while (Boolean.TRUE) {
				boolean anyFilesProcessed=process(args);
				if(anyFilesProcessed){
					System.out.println("Some Files are processed please check in the archive dirctory with current timestamp");
				}else{
					System.out.println("Waiting....");
				}
			}
		
	
		return 0;
	}
	
	
	public static boolean process(final String[] args) throws IOException {
		
		boolean isAnyFilesProcessed=Boolean.FALSE;
		
		final String BASE_LOC = propReader.getValue(SEConstants.BASE_LOC);
		final String INBOX_LOC=BASE_LOC+System.getProperty("file.separator")+propReader.getValue(SEConstants.INBOX);
		final String triggerExt=propReader.getValue(SEConstants.TRIGGER_FILE_NAME_EXT);

		Map<String, List<String>> groupedFiles = FileUtil.groupSimilarFiles(INBOX_LOC,triggerExt);
		
//		for (File file : files) {
//			if(StringUtils.equals(FilenameUtils.getExtension(file.getName()), SEConstants.EXTENSION_CTL)) {
//				String fileName = StringUtils.substringBefore(file.getName(), SEConstants.SEPERATOR_DOT);
//				//System.out.println(fileName);
//			}
//		}
		
		
		if(groupedFiles!=null && groupedFiles.isEmpty()){
			
			for (Map.Entry<String, List<String>> groupFile : groupedFiles.entrySet()) {
				final String lookupFileName=groupFile.getKey(); 
				final List<String> processFiles=groupFile.getValue();
				boolean filesProcessed=processIndividualGroupFile(lookupFileName, processFiles);
				
				if(filesProcessed){
					System.out.println("Files processed for :"+lookupFileName);
				}else{
					throw new SERuntimeException("Unable to process for file with lookup Name:"+lookupFileName);
				}
			}
			
			isAnyFilesProcessed=Boolean.TRUE;
		}
		return isAnyFilesProcessed;
	}

	public static boolean processIndividualGroupFile(final String lookupFile, final List<String> processFiles)
	{
		return Boolean.FALSE;
	}
}
