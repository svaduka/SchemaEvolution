package com.vodafone.exceptions;

public class SERuntimeException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private RuntimeException re=null;
	
	public SERuntimeException(final String msg){
		re=new RuntimeException(msg);
	}

}
