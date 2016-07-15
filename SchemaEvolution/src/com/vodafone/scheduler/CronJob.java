package com.vodafone.scheduler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hdfsservice.util.HDFSUtil;

import com.commonservice.FileUtil;
import com.vodafone.constants.SEConstants;
import com.vodafone.exceptions.SERuntimeException;
import com.vodafone.pojo.CtlInfo;
import com.vodafone.util.SEFileUtil;
import com.vodafone.util.SEHDFSUtil;
import com.vodafone.util.PropertyReader;

public class CronJob extends Configured implements Tool{
	
	
	//The process time is constant for all processed files
	private long processStartTime=-1l;

	//Class used to read the property files
	public static PropertyReader propReader = null; 
	
	public static void main(String[] args) throws IOException {

		// Read Properties
		propReader = new PropertyReader(SEConstants.PROPERTY_FILE_NAME);
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
				e.printStackTrace();
			}
		}

	}
	
	public int run(String[] args) throws Exception {

			while (Boolean.TRUE) {
				
				processStartTime=System.currentTimeMillis();  // This is the time through out the current process
				
				boolean anyFilesProcessed=process(args);
				if(anyFilesProcessed){
					System.out.println("Some Files are processed please check in the archive dirctory with current timestamp");
				}
//				else{
//					System.out.println("Waiting....");
//				}
			}
		
	
		return 0;
	}
	
	/**
	 * 
	 * @param args
	 * @return TRUE/FALSE indicates the files inside the inbox location is processed or not
	 * @throws IOException
	 */
	public boolean process(final String[] args) throws IOException {
		
		boolean isAnyFilesProcessed=Boolean.FALSE;
		
		final String BASE_LOC = propReader.getValue(SEConstants.BASE_LOC);
		final String INBOX_LOC=BASE_LOC+System.getProperty("file.separator")+propReader.getValue(SEConstants.INBOX);
		final String triggerExt=propReader.getValue(SEConstants.TRIGGER_FILE_NAME_EXT);

		Map<String, List<String>> groupedFiles = FileUtil.groupSimilarFiles(INBOX_LOC,triggerExt);
		
		
		if(groupedFiles!=null && !groupedFiles.isEmpty()){
			
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
	
	/**
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the individual file group processed or not
	 * @throws IOException
	 */

	public boolean processIndividualGroupFile(final String lookupFile, final List<String> processFiles) throws IOException
	{
		boolean isFilesMovedToHDFS=processMoveFilesToHDFS(lookupFile, processFiles);
		
		if(isFilesMovedToHDFS)
		{
			boolean isFileMovedToArchive=processMoveFilesToArchive(lookupFile, processFiles);
			if(!isFileMovedToArchive)
			{
				throw new SERuntimeException("Process failed: move file to archive location, look up file name:"+lookupFile);
			}
			
		}else{
			boolean isFilesMovedToFailed=processMoveFilesToFailed(lookupFile, processFiles);
			if(!isFilesMovedToFailed)
			{
				throw new SERuntimeException("Process failed: move file to failed location, look up file name:"+lookupFile);
			}
			
		}
		
		return Boolean.TRUE;
	}
	
	/**
	 * This method will move the corresponding lookupfile entries from inbox location to respective HDFS location.
	 * If moving HDFS files fail, then the inbox files will move to failed directory.
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the list of files moved to hdfs or not
	 * @throws IOException
	 */
	public boolean processMoveFilesToHDFS(final String lookupFile, final List<String> processFiles) {
		
		boolean isFilesMovedTOHDFS=Boolean.TRUE;
		
		boolean isHDFSEnabled=Boolean.parseBoolean(propReader.getValue(SEConstants.ENABLE_HDFS)); //environment property to enable hdfs during project discussion
		
		if(isHDFSEnabled)
		{
		
		final Configuration conf=super.getConf();
		
		//HDFS base location from property file
		final String hdfsbaseLoc=propReader.getValue(SEConstants.HDFS_BASE_LOC);
		
		final String ctlFileWithLoc=FileUtil.getExtFile(processFiles, propReader.getValue(SEConstants.CTL_FILE_EXT));
		try
		{
			CtlInfo ctlInfo = SEFileUtil.parseCtlFile(ctlFileWithLoc);

			final String hdfsCTLFileLoc = SEHDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo,hdfsbaseLoc,
							propReader
									.getValue(SEConstants.HDFS_CONTROL_FOLDER_NAME),
							propReader.getValue(SEConstants.HDFS_INBOX_LOC),
							processStartTime);

			System.out.println("CTL FILE LOC:" + hdfsCTLFileLoc);

			isFilesMovedTOHDFS = HDFSUtil.writeLocalFileOnHDFS(ctlFileWithLoc,hdfsCTLFileLoc, conf);

			final String metaFileLoc = FileUtil.getExtFile(processFiles,propReader.getValue(SEConstants.META_FILE_EXT));
			
			final String hdfsMETAFileLoc = SEHDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo,hdfsbaseLoc,
							propReader.getValue(SEConstants.HDFS_META_FOLDER_NAME),
							propReader.getValue(SEConstants.HDFS_INBOX_LOC),
							processStartTime);

			System.out.println("META FILE LOC:" + hdfsMETAFileLoc);

			isFilesMovedTOHDFS =  HDFSUtil.writeLocalFileOnHDFS(metaFileLoc,hdfsMETAFileLoc, conf);

			final String datFileLoc = FileUtil.getExtFile(processFiles,propReader.getValue(SEConstants.DAT_FILE_EXT));
			
			final String hdfsDATFileLoc = SEHDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo,hdfsbaseLoc,
							propReader.getValue(SEConstants.HDFS_DATA_FOLDER_NAME),
							propReader.getValue(SEConstants.HDFS_INBOX_LOC),
							processStartTime);

			System.out.println("DAT FILE LOC:" + hdfsDATFileLoc);

			isFilesMovedTOHDFS =  HDFSUtil.writeLocalFileOnHDFS(datFileLoc,hdfsDATFileLoc, conf);
		}
		
		catch (IOException e) {
			isFilesMovedTOHDFS=Boolean.FALSE;
		}
		}
		return isFilesMovedTOHDFS;
	}
	
	/**
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the files are moved successfully
	 * This method will move the files in list to failed location
	 */
	
	public  boolean processMoveFilesToArchive(final String lookupFile, final List<String> processFiles) {
		
		boolean procesedFiles=Boolean.TRUE;
		
		final String BASE_LOCATION=propReader.getValue(SEConstants.BASE_LOC);
		final String archiveLoc=propReader.getValue(SEConstants.ARCHIVE_LOC);
		final String destinationLoc=BASE_LOCATION+SEConstants.FILE_SEPARATOR+archiveLoc;

		procesedFiles=processMoveFilesToDestination(destinationLoc, processFiles);
		return procesedFiles;
		
	}

	/**
	 * 
	 * @param lookupFile
	 * @param processFiles
	 * @return TRUE/FALSE indicates the files are moved successfully
	 * This method will move the files in list to failed location
	 */
	public  boolean processMoveFilesToFailed(final String lookupFile, final List<String> processFiles) {
		boolean procesedFiles=Boolean.TRUE;
		
		final String BASE_LOCATION=propReader.getValue(SEConstants.BASE_LOC);
		final String failedLoc=propReader.getValue(SEConstants.FAILED_LOC);
		final String destinationLoc=BASE_LOCATION+SEConstants.FILE_SEPARATOR+failedLoc;

		procesedFiles=processMoveFilesToDestination(destinationLoc, processFiles);
		
		return procesedFiles;
	}
	
	/**
	 * 
	 * @param destinationLoc
	 * @param processFiles
	 * @return TRUE/FALSE indicates the files are moved successfully
	 * 
	 * This method will move the files in list to destination location
	 */
	
	public  boolean processMoveFilesToDestination(final String destinationLoc,final List<String> processFiles) {
		
		boolean procesedFiles=Boolean.TRUE;
		
		try
		{
			for (String fileNameWithLoc : processFiles) 
			{
				FileUtil.moveFileToLoc(fileNameWithLoc,destinationLoc);
			}
		}catch (IOException e) {
			procesedFiles=Boolean.FALSE;
		}
		return procesedFiles;
		
	}
}
