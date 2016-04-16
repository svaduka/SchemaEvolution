package com.vodafone.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;
import org.json.JSONException;

import com.vodafone.util.AvroUtil;

public class TestAvsc {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, JSONException {

		final String metaFileLocationWithName="D://Naveen//NaveenWS//20150317T044228_20156423_ORS_ORSEXTR_ISSUE_FULL.meta";
		
		Schema schema=AvroUtil.convertMetaFileToAVSC(metaFileLocationWithName);
		
		System.out.println(schema.toString());
	}

}
