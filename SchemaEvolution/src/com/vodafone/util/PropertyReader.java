package com.vodafone.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyReader {
	
	Properties properties=null;
	
	String fileNameWithLoc = null;

	public PropertyReader(String fileNameWithLoc) {
		this.fileNameWithLoc=fileNameWithLoc;
	}
	
	public boolean loadProperties() throws IOException {

	InputStream inStream = new FileInputStream(new File(fileNameWithLoc));
	properties = new Properties();
	properties.load(inStream);
	
	return Boolean.TRUE;
		
	}
	
	public String getValue(final String key)
	{
		if(properties==null){
			throw new RuntimeException("Please load the properties");
		}
		return properties.getProperty(key);
	}
	
}
