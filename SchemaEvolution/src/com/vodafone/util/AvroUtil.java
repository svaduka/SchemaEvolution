package com.vodafone.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;

import com.vodafone.pojo.TableMetaData;

public class AvroUtil {

	public Schema convertMetaFileToAVSC(final String metaFileLocationWithName) throws FileNotFoundException, IOException {
		
		TableMetaData tableMetaData=FileUtil.readMetaFile(metaFileLocationWithName);
		
		Schema tableSchema=convertTableMetaDataToAVSC(tableMetaData);
		
		return tableSchema;
	}
	
	public Schema convertTableMetaDataToAVSC(final TableMetaData tableMetaData) {
		
		final String avscSchema=null;
		Schema avroSchema=new Schema.Parser().parse(avscSchema);
		
		return avroSchema;
		
	}
	
//	public Json
	
}
