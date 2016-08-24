package com.schemaevolution.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.commonservice.FileUtil;
import com.schemaevolution.constants.SEConstants;
import com.schemaevolution.pojo.CtlInfo;

public class SEFileUtil {
	
//	public static TableMetaData parseMetaFile(String metaFileNameWithLoc) throws FileNotFoundException, IOException {
//
//		List<String> lines=readLines(metaFileNameWithLoc);
//		TableMetaData tblMD = null;
//		if(lines!=null && !lines.isEmpty())
//		{
//				
//			final String line=lines.get(SEConstants.ZERO);
//			String[] tokens = StringUtils.splitPreserveAllTokens(line, SEConstants.SEPERATOR_PIPE);
//			
//			tblMD = new TableMetaData();
//			
//			tblMD.setSOURCE_NAME(!StringUtils.isEmpty(tokens[0])?tokens[0]:SEConstants.EMPTY);
//			tblMD.setSCHEMA_NAME(!StringUtils.isEmpty(tokens[1])?tokens[1]:SEConstants.EMPTY);
//			tblMD.setTABLE_NAME(!StringUtils.isEmpty(tokens[2])?tokens[2]:SEConstants.EMPTY);
//			Set<ColumnInfo> hashSet = new HashSet<ColumnInfo>();
//			String strLine=null;
//			for (int i=1;i<lines.size();i++) 
//			{
//				strLine=lines.get(i);
//			ColumnInfo columnInfo = FileUtil.createColumnInfo(strLine);
//			hashSet.add(columnInfo);
//			}
//			tblMD.setColumns(hashSet);
//			
//		}
//		return tblMD;
//		
//	}
//	
//	public static ColumnInfo createColumnInfo(String line) {
//		
//		String[] columns = StringUtils.splitPreserveAllTokens(line, "|");
//		ColumnInfo columnInfo = null;
//		if(!StringUtils.isEmpty(line))
//		{
//			columnInfo=new ColumnInfo();
//				
//			columnInfo.setSourceName(!StringUtils.isEmpty(columns[0])?columns[0]:SEConstants.EMPTY);
//			columnInfo.setSchemaName(!StringUtils.isEmpty(columns[1])?columns[1]:SEConstants.EMPTY);
//			columnInfo.setTableName(!StringUtils.isEmpty(columns[2])?columns[2]:SEConstants.EMPTY);
//			columnInfo.setColumnName(!StringUtils.isEmpty(columns[3])?columns[3]:SEConstants.EMPTY);
//			columnInfo.setDataType(!StringUtils.isEmpty(columns[4])?columns[4]:SEConstants.EMPTY);
//			columnInfo.setDataLength(!StringUtils.isEmpty(columns[5])?Integer.parseInt(columns[5]):SEConstants.ZERO);
//			columnInfo.setDataScale(!StringUtils.isEmpty(columns[6])?columns[6]:SEConstants.EMPTY);
//			columnInfo.setFormat(!StringUtils.isEmpty(columns[7])?columns[7]:SEConstants.EMPTY);
//			columnInfo.setPrimaryKey(StringUtils.isEmpty(columns[8]) 
//									|| StringUtils.equalsIgnoreCase(columns[8], SEConstants.FALSE) ? Boolean.FALSE:Boolean.TRUE);
//			columnInfo.setColumnId(!StringUtils.isEmpty(columns[9])?Integer.parseInt(columns[9]):SEConstants.ZERO);
//									
//		}	
//		return columnInfo;
//	}
	
	public static CtlInfo parseCtlFile(String ctlFileNameWithLoc) throws IOException {
		
		CtlInfo cntrlFile = null;
		
		List<String> lines=FileUtil.readLines(ctlFileNameWithLoc);
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
}
