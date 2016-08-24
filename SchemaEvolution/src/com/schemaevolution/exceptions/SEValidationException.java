package com.schemaevolution.exceptions;

import com.commonservice.util.LoggerUtil;

public class SEValidationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	LoggerUtil logger=new LoggerUtil(SEValidationException.class);
	private Exception e=null;
	
	public SEValidationException(final String msg){
		e=new Exception(msg);
		logger.fatal(e.getMessage());
		
	}

}
