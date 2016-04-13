package com.vodafone.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.vodafone.constants.SEConstants;
import com.vodafone.pojo.ColumnInfo;
import com.vodafone.pojo.TableMetaData;

public class AvroUtil {

	public static Schema convertMetaFileToAVSC(final String metaFileLocationWithName) throws FileNotFoundException, IOException {
		
		TableMetaData tableMetaData=FileUtil.readMetaFile(metaFileLocationWithName);
		
		Schema tableSchema=convertTableMetaDataToAVSC(tableMetaData);
		
		return tableSchema;
	}
	
	public static Schema convertTableMetaDataToAVSC(final TableMetaData tableMetaData) {
		
		final String avscSchema=null;
		Schema avroSchema=new Schema.Parser().parse(avscSchema);
		
		return avroSchema;
		
	}
	
	public static JSONObject createJsonSchemaFromTableMetaData(final TableMetaData tableMetaData) throws JSONException{
		
		JSONObject avscSchema=new JSONObject();
		
		avscSchema.put(SEConstants.AVROConstants.TYPE, SEConstants.AVROConstants.TYPE_RECORD);
		avscSchema.put(SEConstants.AVROConstants.SOURCE_NAME, tableMetaData.getSOURCE_NAME());
		avscSchema.put(SEConstants.AVROConstants.NAMESPACE, tableMetaData.getSCHEMA_NAME());
		avscSchema.put(SEConstants.AVROConstants.TABLE_NAME, tableMetaData.getTABLE_NAME());
		
		
		
		avscSchema.put(SEConstants.AVROConstants.FIELDS, tableMetaData.getColumns());
		
		return avscSchema;
	}
	
	public static JSONArray createFieldsFromTableMetaDataColumns(final TableMetaData tableMetaData)
	{
		return null;
	}
	
	public static JSONObject createFieldFromTableMetaDataColumn(final ColumnInfo column)
	{
		return null;
	}
	
}
