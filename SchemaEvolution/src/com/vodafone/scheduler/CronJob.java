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
import com.vodafone.pojo.CtlInfo;
import com.vodafone.util.FileUtil;
import com.vodafone.util.HDFSUtil;
import com.vodafone.util.PropertyReader;

public class CronJob extends Configured implements Tool{
	
	private long processStartTime=-1l;

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
				
				processStartTime=System.currentTimeMillis();  // This is the time through out the current process
				
				
				boolean anyFilesProcessed=process(args);
				if(anyFilesProcessed){
					System.out.println("Some Files are processed please check in the archive dirctory with current timestamp");
				}else{
					System.out.println("Waiting....");
				}
			}
		
	
		return 0;
	}
	
	
	public boolean process(final String[] args) throws IOException {
		
		boolean isAnyFilesProcessed=Boolean.FALSE;
		
		final String BASE_LOC = propReader.getValue(SEConstants.BASE_LOC);
		final String INBOX_LOC=BASE_LOC+System.getProperty("file.separator")+propReader.getValue(SEConstants.INBOX);
		final String triggerExt=propReader.getValue(SEConstants.TRIGGER_FILE_NAME_EXT);

		Map<String, List<String>> groupedFiles = FileUtil.groupSimilarFiles(INBOX_LOC,triggerExt);
		
		
		if(groupedFiles!=null && groupedFiles.isEmpty()){
			
			for (Map.Entry<String, List<String>> groupFile : groupedFiles.entrySet()) 
			{
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

	public boolean processIndividualGroupFile(final String lookupFile, final List<String> processFiles) throws IOException
	{
		boolean isFilesMovedToHDFS=processMoveFilesToHDFS(lookupFile, processFiles);
		
		if(isFilesMovedToHDFS)
		{
			boolean isFileMovedToArchive=processMoveFilesToArchive(lookupFile, processFiles);
			if(!isFileMovedToArchive)
			{
				throw new SERuntimeException("Process failed to move to archive look up file name:"+lookupFile);
			}
			
		}else{
			
			boolean isFilesMovedToFailed=processMoveFilesToFailed(lookupFile, processFiles);
			if(!isFilesMovedToFailed)
			{
				throw new SERuntimeException("Process failed to move to failed look up file name:"+lookupFile);
			}
			
		}
		
		return Boolean.FALSE;
	}
	
	public boolean processMoveFilesToHDFS(final String lookupFile, final List<String> processFiles) throws IOException{
		
		boolean isFilesMovedTOHDFS=Boolean.TRUE;
		
		final Configuration conf=super.getConf();
		
		final String hdfsbaseLoc=propReader.getValue(SEConstants.HDFS_BASE_LOC);
		
		final String ctlFileWithLoc=FileUtil.getExtFile(processFiles, propReader.getValue(SEConstants.CTL_FILE_EXT));
		CtlInfo ctlInfo=FileUtil.parseCtlFile(ctlFileWithLoc);
		
		final String hdfsCTLFileLoc=HDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo, hdfsbaseLoc, 
				propReader.getValue(SEConstants.HDFS_CONTROL_FOLDER_NAME), 
				propReader.getValue(SEConstants.HDFS_INBOX_LOC), 
				processStartTime);
		
		System.out.println("CTL FILE LOC:"+hdfsCTLFileLoc);
		
		boolean isFileMoved=HDFSUtil.copyFromLocalToHDFS(ctlFileWithLoc, hdfsCTLFileLoc, conf);
		
		if(!isFileMoved){
//			throw new SERuntimeException("File :"+ctlFileWithLoc+"Unable to move to hdfs location:"+hdfsCTLFileLoc);
			isFilesMovedTOHDFS=Boolean.FALSE;
		}

		final String metaFileLoc=FileUtil.getExtFile(processFiles, propReader.getValue(SEConstants.META_FILE_EXT));
		final String hdfsMETAFileLoc=HDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo, hdfsbaseLoc, 
				propReader.getValue(SEConstants.HDFS_META_FOLDER_NAME), 
				propReader.getValue(SEConstants.HDFS_INBOX_LOC), 
				processStartTime);
		
		System.out.println("META FILE LOC:"+hdfsMETAFileLoc);
		
		isFileMoved=HDFSUtil.copyFromLocalToHDFS(metaFileLoc, hdfsMETAFileLoc, conf);
		
		if(!isFileMoved){
//			throw new SERuntimeException("File :"+metaFileLoc+"Unable to move to hdfs location:"+hdfsMETAFileLoc);
			isFilesMovedTOHDFS=Boolean.FALSE;
		}
		
		final String datFileLoc=FileUtil.getExtFile(processFiles, propReader.getValue(SEConstants.DAT_FILE_EXT));
		final String hdfsDATFileLoc=HDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo, hdfsbaseLoc, 
				propReader.getValue(SEConstants.HDFS_DATA_FOLDER_NAME), 
				propReader.getValue(SEConstants.HDFS_INBOX_LOC), 
				processStartTime);
		
		System.out.println("DAT FILE LOC:"+hdfsDATFileLoc);
		
		isFileMoved=HDFSUtil.copyFromLocalToHDFS(datFileLoc, hdfsDATFileLoc, conf);
		
		if(!isFileMoved){
//			throw new SERuntimeException("File :"+datFileLoc+"Unable to move to hdfs location:"+hdfsDATFileLoc);
			isFilesMovedTOHDFS=Boolean.FALSE;
		}
		
		return isFilesMovedTOHDFS;
	}
	
	public  boolean processMoveFilesToArchive(final String lookupFile, final List<String> processFiles) throws IOException{
		final String archiveLoc=propReader.getValue(SEConstants.ARCHIVE_LOC);
		return Boolean.FALSE;
	}
	
	public  boolean processMoveFilesToFailed(final String lookupFile, final List<String> processFiles) throws IOException{
		final String failedLoc=propReader.getValue(SEConstants.FAILED_LOC);
		return Boolean.FALSE;
	}
}
