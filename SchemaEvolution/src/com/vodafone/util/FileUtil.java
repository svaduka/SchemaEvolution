package com.vodafone.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.vodafone.constants.SEConstants;
import com.vodafone.pojo.ColumnInfo;
import com.vodafone.pojo.CtlInfo;
import com.vodafone.pojo.TableMetaData;

public class FileUtil {

	public static void main(String[] args) {
		
		

	}
	
	public static List<String> readLines(String fileNameWithLoc) throws IOException{
		
		List<String> lines = FileUtils.readLines(new File(fileNameWithLoc));
		return lines;
		
	}

	public static TableMetaData parseMetaFile(String metaFileNameWithLoc) throws FileNotFoundException, IOException {

		List<String> lines=readLines(metaFileNameWithLoc);
		TableMetaData tblMD = null;
		if(lines!=null && !lines.isEmpty())
		{
				
			final String line=lines.get(SEConstants.ZERO);
			String[] tokens = StringUtils.splitPreserveAllTokens(line, SEConstants.SEPERATOR_PIPE);
			
			tblMD = new TableMetaData();
			
			tblMD.setSOURCE_NAME(!StringUtils.isEmpty(tokens[0])?tokens[0]:SEConstants.EMPTY);
			tblMD.setSCHEMA_NAME(!StringUtils.isEmpty(tokens[1])?tokens[1]:SEConstants.EMPTY);
			tblMD.setTABLE_NAME(!StringUtils.isEmpty(tokens[2])?tokens[2]:SEConstants.EMPTY);
			Set<ColumnInfo> hashSet = new HashSet<ColumnInfo>();
			String strLine=null;
			for (int i=1;i<lines.size();i++) 
			{
				strLine=lines.get(i);
			ColumnInfo columnInfo = FileUtil.createColumnInfo(strLine);
			hashSet.add(columnInfo);
			}
			tblMD.setColumns(hashSet);
			
		}
		return tblMD;
		
	}
	
	public static ColumnInfo createColumnInfo(String line) {
		
		String[] columns = StringUtils.splitPreserveAllTokens(line, "|");
		ColumnInfo columnInfo = null;
		if(!StringUtils.isEmpty(line))
		{
			columnInfo=new ColumnInfo();
				
			columnInfo.setSourceName(!StringUtils.isEmpty(columns[0])?columns[0]:SEConstants.EMPTY);
			columnInfo.setSchemaName(!StringUtils.isEmpty(columns[1])?columns[1]:SEConstants.EMPTY);
			columnInfo.setTableName(!StringUtils.isEmpty(columns[2])?columns[2]:SEConstants.EMPTY);
			columnInfo.setColumnName(!StringUtils.isEmpty(columns[3])?columns[3]:SEConstants.EMPTY);
			columnInfo.setDataType(!StringUtils.isEmpty(columns[4])?columns[4]:SEConstants.EMPTY);
			columnInfo.setDataLength(!StringUtils.isEmpty(columns[5])?Integer.parseInt(columns[5]):SEConstants.ZERO);
			columnInfo.setDataScale(!StringUtils.isEmpty(columns[6])?columns[6]:SEConstants.EMPTY);
			columnInfo.setFormat(!StringUtils.isEmpty(columns[7])?columns[7]:SEConstants.EMPTY);
			columnInfo.setPrimaryKey(StringUtils.isEmpty(columns[8]) 
									|| StringUtils.equalsIgnoreCase(columns[8], SEConstants.FALSE) ? Boolean.FALSE:Boolean.TRUE);
			columnInfo.setColumnId(!StringUtils.isEmpty(columns[9])?Integer.parseInt(columns[9]):SEConstants.ZERO);
									
		}	
		return columnInfo;
	}
	
	public static CtlInfo parseCtlFile(String ctlFileNameWithLoc) throws IOException {
		
		CtlInfo cntrlFile = null;
		
		List<String> lines=readLines(ctlFileNameWithLoc);
		if(lines!=null && !lines.isEmpty())
		{
			
			cntrlFile = new CtlInfo();
			
				String[] values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_SOURCENAME_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setSourceName(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_SOURCESCHEMANAME_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setSourceSchemaName(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_ENTITYNAME_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setEntityName(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_RECORDCOUNT_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setRecordCount(Integer.parseInt(values[SEConstants.ONE]));

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_EXTRACTDATE_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setExtractDate(values[SEConstants.ONE]);
				
				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_EXTRACTTIME_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setExtractTime(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_FILETIMESTAMP_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setFileTimeStamp(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_EXTRACTDATETIME_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setExtractDateTime(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_ETLLANDINGDIRECTORY_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.seteTLLandingDirectory(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_FILELOADTYPE_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setFileLoadType(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_ETLAPPUSERID_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.seteTLAppUserId(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_EXTRACTSTATUS_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setExtractStatus(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_SOURCETIMEZONE_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setSourceTimeZone(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_EFFECTIVEDATE_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setEffectiveDate(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_DATAFILEEXTENSION_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setDataFileExtension(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_ADDITIONALFILESEXTENSIONS_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setAdditionalFilesExtensions(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_FILEFORMATTYPE_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setFileFormatType(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_FIELDDELIMITER_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setFieldDelimiter(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_SOURCEWATERMARK_1_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setSourceWatermark_1(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_SOURCEWATERMARK_2_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setSourceWatermark_2(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_ETLPROCESS_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.seteTLProcess(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_FREQUENCY_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setFrequency(values[SEConstants.ONE]);

				values = StringUtils.splitByWholeSeparatorPreserveAllTokens(lines.get(SEConstants.CTL_RECORDDELIMITER_IDX), SEConstants.SEPERATOR_EQUAL);
				cntrlFile.setRecordDelimiter(values[SEConstants.ONE]);

		}
		
	return cntrlFile;
		
	}

	public static List<File> getTriggerFiles(final String inboxLoc, final String triggerExt)
	{
		String[] extensions = new String[] {triggerExt};
		List<File> files = (List<File>)FileUtils.listFiles(new File(inboxLoc), extensions, true);
		return files; 
	}
	
	/**
	 * 
	 * @param triggerFileNameWithLocAndExt  D:\Naveen\Datasets\SchemaEvolution\20150317T044228_20156423_ORS_ORSEXTR_ISSUE_FULL.ctl
	 * @param triggerExt ctl 
	 * lookupName = 
	 * @return
	 */
	
	public static List<String> getSimilarFiles(final String triggerFileNameWithLocAndExt, final String triggerExt)
	{
		List<String> similarFiles=new ArrayList<String>();
		
		final String lookUpName=getFileName(triggerFileNameWithLocAndExt);
		
		System.out.println("LookUpName:"+lookUpName);
		
		File f=new File(triggerFileNameWithLocAndExt);
		
		File parentFile=f.getParentFile();
		
		File[] files=parentFile.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				return StringUtils.indexOf(name, lookUpName)!=-1;
			}
		});
		
		for (File file : files) {
			similarFiles.add(file.getAbsolutePath());
		}
		
		return similarFiles;
		
	}
	
	public static Map<String, List<String>> groupSimilarFiles(final String inbox_loc,final String triggerFileExt){
		
		List<File> triggeFiles=getTriggerFiles(inbox_loc, triggerFileExt);
		
		Map<String, List<String>> groupFiles=null;
		
		if(triggeFiles!=null && !triggeFiles.isEmpty())
		{
			groupFiles=new HashMap<String, List<String>>();
			
			for (File file : triggeFiles) {
				final String key=getFileName(file.getAbsolutePath());
				List<String> similarFiles=getSimilarFiles(file.getAbsolutePath(), triggerFileExt);
				groupFiles.put(key, similarFiles);
			}
		}
		return groupFiles;
	}
	
	public static String getFileName(final String fileNameWithExt)
	{
		String onlyFileName=fileNameWithExt.substring(fileNameWithExt.lastIndexOf(SEConstants.FILE_SEPARATOR)+1);
		if(onlyFileName==null){
			onlyFileName=fileNameWithExt;
		}
		onlyFileName=onlyFileName.substring(0,onlyFileName.lastIndexOf("."));
		
		return onlyFileName;
	}
	
	
	public boolean moveToArchiveFile(final String inbox_loc,final String archive_loc, final String moveFileLookupName)
	{
		return Boolean.FALSE;
	}
	
	public static String getExtFile(final List<String> groupFiles, final String extFile)
	{
		String ctlFile=null;
		for (String file : groupFiles) {
			if(file.indexOf(("."+extFile))!=-1){
				ctlFile=file;
				break;
			}
		}
		return ctlFile;
	}
	
	public static String getExtFile(final String anyFileNameWithExt, final String extFile)
	{
		final String onlyFileName=getFileName(anyFileNameWithExt);
	
		return onlyFileName.concat(extFile);
	}

	public static void moveFileToLoc(String fileNameWithLoc,String destinationLoc) throws IOException {
		FileUtils.moveFileToDirectory(new File(fileNameWithLoc), new File(destinationLoc), Boolean.TRUE);
		
	}
	
}
