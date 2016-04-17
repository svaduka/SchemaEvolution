package com.vodafone.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoggerUtil {
	
	private Log logger=null;
	
	@SuppressWarnings("rawtypes")
	public LoggerUtil(final Class oClass)
	{
		logger=LogFactory.getLog(oClass);
	}
	
	public void info(final String infoMsg)
	{
		logger.info(infoMsg);
	}
	
	public void debug(final String infoMsg)
	{
		logger.debug(infoMsg);
	}
	
	public void warn(final String infoMsg)
	{
		logger.warn(infoMsg);
	}
	
	public void error(final String infoMsg)
	{
		logger.error(infoMsg);
	}
	
	public void fatal(final String infoMsg)
	{
		logger.fatal(infoMsg);
	}
	
	
	
}
