package com.vodafone.util;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.vodafone.constants.SEConstants;
import com.vodafone.pojo.CtlInfo;

public class SEHDFSUtil {

	/**
	 * 
	 * @param ctlInfo
	 * @param hdfsBaseLoc is null the method will use default properties PRTS.properties
	 * @param extension
	 * @return
	 * @throws IOException
	 */
	
	public static String createHDFSDestinationLocationForExtensionAndFolder(final CtlInfo ctlInfo, 
			String hdfsBaseLoc, final String extension, 
			final String folder, final long intakeTime) throws IOException 
	{
		PropertyReader reader = new PropertyReader();// Loading SchemaEvolution PRTS.properties
		StringBuffer hdfsDestinationLoc=null;
		if(StringUtils.isEmpty(hdfsBaseLoc))
		{
			hdfsBaseLoc=reader.getValue(SEConstants.HDFS_BASE_LOC);
		}
		if(!StringUtils.isEmpty(hdfsBaseLoc))
		{
			hdfsDestinationLoc=new StringBuffer(hdfsBaseLoc);//PROJECT_NAME/MODULE_NAME/ENV(d_dev/t_dev/ p_dev)/RAW/
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			hdfsDestinationLoc.append(ctlInfo.getSourceName()); // SOURCE_NAME
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			hdfsDestinationLoc.append(ctlInfo.getSourceSchemaName()); // SOURCE_NAME
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			hdfsDestinationLoc.append(ctlInfo.getEntityName()); // ENTITY_NAME
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			hdfsDestinationLoc.append(extension); // CTL /META / DAT
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			if(StringUtils.indexOf(ctlInfo.getDataFileExtension(), extension)!=-1)
			{
			hdfsDestinationLoc.append(folder); //INBOX / FAILED / READY / PROCESSED
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			}
			final String timeFormat=DateUtil.convertTimeIntoFormat(intakeTime, SEConstants.PROJECT_TIME_FORMAT);
			hdfsDestinationLoc.append(timeFormat);// /20161704
			hdfsDestinationLoc.append(SEConstants.FILE_SEPARATOR);// /
			
		}
		return hdfsDestinationLoc.toString().trim();
	}
	
}
