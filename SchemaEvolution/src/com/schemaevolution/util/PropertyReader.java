package com.schemaevolution.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.schemaevolution.constants.SEConstants;

public class PropertyReader {
	
	Properties properties=null;
	
	public static String fileNameWithLoc = null;
	
	public PropertyReader() throws IOException{
		loadProperties();
	}
 
	public PropertyReader(String fileNameWithLoc) throws IOException {
		PropertyReader.fileNameWithLoc=fileNameWithLoc;
		loadProperties();
	}
	
	public boolean loadProperties() throws IOException {

		if (fileNameWithLoc == null) {
			fileNameWithLoc = SEConstants.PROPERTY_FILE_NAME;
			System.out.println(" ALERT!!! ALERT!!! ALERT!!! "
							+"Property File Name is null using default property filename:"
							+ fileNameWithLoc);
		}

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
