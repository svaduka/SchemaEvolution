package com.schemaevolution.scheduler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hdfsservice.util.HDFSUtil;

import com.commonservice.FileUtil;
import com.commonservice.util.DateUtil;
import com.commonservice.util.LoggerUtil;
import com.controlprocess.constants.ProcessType;
import com.controlprocess.constants.Status;
import com.controlprocess.pojo.ControlProcess;
import com.controlprocess.pojo.ControlProcessDetail;
import com.controlprocess.util.ControlProcessUtil;
import com.schemaevolution.constants.SEConstants;
import com.schemaevolution.exceptions.SERuntimeException;
import com.schemaevolution.pojo.CtlInfo;
import com.schemaevolution.util.PropertyReader;
import com.schemaevolution.util.SEFileUtil;
import com.schemaevolution.util.SEHDFSUtil;
import com.vodafone.rdbms.dao.HibernateDAO;

public class CronJob extends Configured implements Tool{
	
	
	//The process time is constant for all processed files
	private long cronStartTime=-1l;
	private long processStartTime=-1l;

	private long cronEndTime=-1l;
	private long processEndTime=-1l;

	
	//Class used to read the property files
	public static PropertyReader propReader = null; 
	
	// Control Process audit details
	
	private ControlProcess controlProcess=null;
	private ControlProcessDetail controlProcessDetail=null;
	
	LoggerUtil logUtil=new LoggerUtil(this.getClass());
	
	public static void main(String[] args) throws IOException {

		// Read Properties
		propReader = new PropertyReader(SEConstants.PROPERTY_FILE_NAME);
		boolean propertiesLoaded = propReader.loadProperties();

		if (propertiesLoaded) {

			try {
				Configuration conf = new Configuration(Boolean.TRUE);
				conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
				int i = ToolRunner.run(conf, new CronJob(), args);
				if (i == 0) {
					System.out.println("Success");
				} else {
					System.out.println("Failed");
				}
			} catch (Exception e) 
			{
				e.printStackTrace();
			}
		}

	}
	
	public int run(String[] args) {

			while (Boolean.TRUE) {
				
				cronStartTime=System.currentTimeMillis();  // This is the time through out the current process
				
				logUtil.debug("CRON JOB Started at:"+cronStartTime);
				
				boolean anyFilesProcessed;
				try {
					anyFilesProcessed = process(args);
					if(anyFilesProcessed){
						if(controlProcess!=null)
						{
						controlProcess.setStatus(Status.MOVED.getStringValue());
						}
						logUtil.debug("Some Files are processed please check in the archive dirctory with current timestamp");
					}
					else{
						if(controlProcess!=null)
						{
						controlProcess.setStatus(Status.FAILED.getStringValue());
						}
						System.out.println("Waiting....");
					}
				} catch (IOException e) 
				{
					cronEndTime=System.currentTimeMillis();
					
					logUtil.error("CRON JOB ends at time :"+cronEndTime);
					processEndTime=System.currentTimeMillis();
					if(controlProcess!=null)
					{
						controlProcess.setStatus(Status.FAILED.getStringValue());
						controlProcess.setEndTime(DateUtil.convertTimeMillisIntoSqlTimeStamp(processEndTime));
					}
					e.printStackTrace();
				}finally{
					if(controlProcess!=null)
					{
						boolean isControlProcessEnabled=Boolean.parseBoolean(propReader.getValue(SEConstants.ENABLE_CONTROL_PROCESS));
						if(isControlProcessEnabled && controlProcess!=null && controlProcessDetail!=null)
						{ 
//						Logger.getRootLogger().getAppender("")
						controlProcessDetail.setDetailLogFileLoc("SchemaEvolution.log");	
						HibernateDAO.save(controlProcess);
//						HibernateDAO.save(controlProcessDetail);
						}
						controlProcess=null;
						controlProcessDetail=null;
					}
				}
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
		
		
		if(groupedFiles!=null && !groupedFiles.isEmpty())
		{

			processStartTime=System.currentTimeMillis();//process started now
			
			controlProcess=new ControlProcess();
			
			controlProcess.setControlProcessId(ControlProcessUtil.genarateControlProcessId());
			controlProcess.setProcessType(ProcessType.CRON.getStringValue());
			
			controlProcess.setStartTime(DateUtil.convertTimeMillisIntoSqlTimeStamp(processStartTime));
			
			for (Map.Entry<String, List<String>> groupFile : groupedFiles.entrySet()) 
			{
				controlProcessDetail=new ControlProcessDetail();
				
				controlProcessDetail.setControlProcess(controlProcess);
				final String lookupFileName=groupFile.getKey(); 
				
				final List<String> processFiles=groupFile.getValue();
				
				boolean filesProcessed=processIndividualGroupFile(lookupFileName, processFiles);
				
				if(filesProcessed){
					controlProcess.getDetail().add(controlProcessDetail);
					System.out.println("Files processed for :"+lookupFileName);
				}else{
					throw new SERuntimeException("Unable to process for file with lookup Name:"+lookupFileName);
				}
			}
			processEndTime=System.currentTimeMillis();
			controlProcess.setEndTime(DateUtil.convertTimeMillisIntoSqlTimeStamp(processEndTime));
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
		boolean isEnableFilesMove=Boolean.parseBoolean(propReader.getValue(SEConstants.ENABLE_MOVE_FILES)); //environment property to enable moving files during project discussion
		
		if(isEnableFilesMove)
		{
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
		
		boolean isFilesMovedTOHDFS=Boolean.TRUE; //Some Times application disable the moving HDFS for testing
		
		boolean isHDFSEnabled=Boolean.parseBoolean(propReader.getValue(SEConstants.ENABLE_HDFS)); //environment property to enable hdfs during project discussion

		final String ctlFileWithLoc=FileUtil.getExtFile(processFiles, propReader.getValue(SEConstants.CTL_FILE_EXT));
		controlProcessDetail.setBaseControlFileNameWithLoc(ctlFileWithLoc);

		final String metaFileLoc = FileUtil.getExtFile(processFiles,propReader.getValue(SEConstants.META_FILE_EXT));
		controlProcessDetail.setBaseMetaFileNameWithLoc(metaFileLoc);

		final String datFileLoc = FileUtil.getExtFile(processFiles,propReader.getValue(SEConstants.DAT_FILE_EXT));
		controlProcessDetail.setBaseDatFileNameWithLoc(datFileLoc);

		
		if(isHDFSEnabled)
		{
		
		final Configuration conf=super.getConf();
		
		//HDFS base location from property file
		final String hdfsbaseLoc=propReader.getValue(SEConstants.HDFS_BASE_LOC);
		
		try
		{
			CtlInfo ctlInfo = SEFileUtil.parseCtlFile(ctlFileWithLoc);

			final String hdfsCTLFileLoc = SEHDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo,hdfsbaseLoc,
							propReader
									.getValue(SEConstants.HDFS_CONTROL_FOLDER_NAME),
							propReader.getValue(SEConstants.HDFS_INBOX_LOC),
							processStartTime);


			logUtil.debug("CTL FILE LOC:" + hdfsCTLFileLoc);

			isFilesMovedTOHDFS = HDFSUtil.writeLocalFileOnHDFS(ctlFileWithLoc,hdfsCTLFileLoc, conf);
			
			if(isFilesMovedTOHDFS)
			{
				controlProcessDetail.setHdfsCtlFileLoc(hdfsCTLFileLoc+SEConstants.FILE_SEPARATOR+FileUtil.getOnlyFileName(ctlFileWithLoc));
			}

			final String hdfsMETAFileLoc = SEHDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo,hdfsbaseLoc,
							propReader.getValue(SEConstants.HDFS_META_FOLDER_NAME),
							propReader.getValue(SEConstants.HDFS_INBOX_LOC),
							processStartTime);

			logUtil.debug("META FILE LOC:" + hdfsMETAFileLoc);

			isFilesMovedTOHDFS =  HDFSUtil.writeLocalFileOnHDFS(metaFileLoc,hdfsMETAFileLoc, conf);

			if(isFilesMovedTOHDFS)
			{
				controlProcessDetail.setHdfsMetaFileLoc(hdfsMETAFileLoc+SEConstants.FILE_SEPARATOR+FileUtil.getOnlyFileName(metaFileLoc));
			}
			final String hdfsDATFileLoc = SEHDFSUtil.createHDFSDestinationLocationForExtensionAndFolder(ctlInfo,hdfsbaseLoc,
							propReader.getValue(SEConstants.HDFS_DATA_FOLDER_NAME),
							propReader.getValue(SEConstants.HDFS_INBOX_LOC),
							processStartTime);

			logUtil.debug("DAT FILE LOC:" + hdfsDATFileLoc);

			isFilesMovedTOHDFS =  HDFSUtil.writeLocalFileOnHDFS(datFileLoc,hdfsDATFileLoc, conf);
			if(isFilesMovedTOHDFS)
			{
				controlProcessDetail.setHdfsDatFileLoc(hdfsDATFileLoc+SEConstants.FILE_SEPARATOR+FileUtil.getOnlyFileName(datFileLoc));
			}
		}
		
		catch (IOException e) 
		{
			isFilesMovedTOHDFS=Boolean.FALSE;
		}
		if(isFilesMovedTOHDFS)
		{
			controlProcessDetail.setStatus(Status.MOVED.getStringValue());
			
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

		//TODO Properly update the information
		controlProcessDetail.setDestControlFileNameWithLoc(destinationLoc);
		controlProcessDetail.setDestDatFileNameWithLoc(destinationLoc);
		controlProcessDetail.setDestMetaFileNameWithLoc(destinationLoc);
		
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

		//TODO Properly update the information
		controlProcessDetail.setDestControlFileNameWithLoc(destinationLoc);
		controlProcessDetail.setDestDatFileNameWithLoc(destinationLoc);
		controlProcessDetail.setDestMetaFileNameWithLoc(destinationLoc);
				
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
