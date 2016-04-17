package com.vodafone.exceptions;

public class SEValidationException extends Exception {

	/**
	 *: 
	 */
	private static final long serialVersionUID = 1L;
	
	private Exception e=null;
	
	public SEValidationException(final String msg){
		e=new Exception(msg);
	}

}
