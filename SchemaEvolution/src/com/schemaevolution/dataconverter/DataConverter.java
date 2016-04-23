package com.schemaevolution.dataconverter;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.avroservice.job.AvroDataConverterJob;

import com.vodafone.constants.SEConstants;
import com.vodafone.util.PropertyReader;

public class DataConverter {
	

	public static void main(String[] args) throws IOException {

		PropertyReader propertyReader=new PropertyReader(SEConstants.PROPERTY_FILE_NAME);
		final String destinationFormats=propertyReader.getValue(SEConstants.DESTINATION_FORMAT);
		
		try{
			int i = -1;
			String tokens[] =StringUtils.splitPreserveAllTokens(destinationFormats,SEConstants.SEPERATOR_COMMA);
			
			for (String dataFormat : tokens) 
			{
			if(StringUtils.equalsIgnoreCase(dataFormat, SEConstants.DESTINATION_FORMAT_AVRO))
			{
				if(args.length<4)
				{
					System.out.println("Java Usage: AvroDataConverterJob /hdfs/path/to/avsc /hdfs/path/to/data/ datadelimiter /hdfs/path/to/output");
					return;
				}
				i=ToolRunner.run(new AvroDataConverterJob(), args);
			}
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILED");
			}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
				
	}

}
