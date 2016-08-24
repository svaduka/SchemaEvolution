package com.schemaevolution.exceptions;

import com.commonservice.util.LoggerUtil;

public class SERuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private final LoggerUtil util=new LoggerUtil(SERuntimeException.class);
	
	public SERuntimeException(final String msg){
		logMessage(msg);
		
	}
	
	public void logMessage(final String msg)
	{
		util.error(msg);
	}

}
