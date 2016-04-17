package com.vodafone.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.json.JSONException;

import com.vodafone.util.FileUtil;

public class TestAvsc {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, JSONException {

		final String inboxLocation="D:\\Naveen\\Datasets\\SchemaEvolution\\inbox";
		Map<String, List<String>> groupedFiles=FileUtil.groupSimilarFiles(inboxLocation, "ctl");
		
		System.out.println(groupedFiles);
//		
//		Schema schemaAvroUtil.convertMetaFileToAVSC(metaFileLocationWithName);
//		
//		System.out.println(schema.toString());
//		
//		File f=new File("D:\\Naveen\\Datasets\\SchemaEvolution\\20150317T044228_20156423_ORS_ORSEXTR_ISSUE_FULL.ctl");
//		
//		final String lookUpName="20150317T044228_20156423_ORS_ORSEXTR_ISSUE_FULL";
//		File[] files=f.getParentFile().listFiles(new FilenameFilter() {
//			
//			@Override
//			public boolean accept(File dir, String name) {
//				// TODO Auto-generated method stub
//				return StringUtils.indexOf(name, lookUpName)!=-1;
//			}
//		});	
//		
//		for (File file : files) {
//			System.out.println(file.getAbsolutePath());
//		}
//		
//		
		
		
		
//		final String fileName="20150317T044228_20156423_ORS_ORSEXTR_ISSUE_FULL.ctl";
		
//		System.out.println(fileName.substring(0, fileName.lastIndexOf(".")));
	}

}
