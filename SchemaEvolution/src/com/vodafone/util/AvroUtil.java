package com.vodafone.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.vodafone.constants.SEConstants;
import com.vodafone.pojo.ColumnInfo;
import com.vodafone.pojo.TableMetaData;

public class AvroUtil {
	
	public static GenericRecord convertAVSCToAvro(TableMetaData tableMetaData) throws JSONException {
		
		Schema schema = convertTableMetaDataToAVSC(tableMetaData);
		GenericRecord datum = new GenericData.Record(schema);
		return datum;
		
	}

	public static Schema convertMetaFileToAVSC(final String metaFileLocationWithName) throws FileNotFoundException, IOException, JSONException {
		
		TableMetaData tableMetaData=FileUtil.readMetaFile(metaFileLocationWithName);
		
		Schema tableSchema=convertTableMetaDataToAVSC(tableMetaData);
		
		return tableSchema;
	}
	
	public static Schema convertTableMetaDataToAVSC(final TableMetaData tableMetaData) throws JSONException {
		
		final JSONObject avscSchema=createJsonSchemaFromTableMetaData(tableMetaData);
		Schema avroSchema=new Schema.Parser().parse(avscSchema.toString());
		
		return avroSchema;
		
	}
	
	public static JSONObject createJsonSchemaFromTableMetaData(final TableMetaData tableMetaData) throws JSONException{
		
		JSONObject avscSchema=new JSONObject();
		
		avscSchema.put(SEConstants.AVROConstants.TYPE, SEConstants.AVROConstants.TYPE_RECORD);
		avscSchema.put(SEConstants.AVROConstants.SOURCE_NAME, tableMetaData.getSOURCE_NAME());
		avscSchema.put(SEConstants.AVROConstants.NAMESPACE, tableMetaData.getSCHEMA_NAME());
		avscSchema.put(SEConstants.AVROConstants.TABLE_NAME, tableMetaData.getTABLE_NAME());
		JSONArray fields=createFieldsFromTableMetaDataColumns(tableMetaData);
		avscSchema.put(SEConstants.AVROConstants.FIELDS, fields);
		
		return avscSchema;
	}
	
	public static JSONArray createFieldsFromTableMetaDataColumns(final TableMetaData tableMetaData) throws JSONException
	{
		final Set<ColumnInfo> columns=tableMetaData.getColumns();
		JSONArray fields=new JSONArray();
		JSONObject field=null;
		for (ColumnInfo column : columns) {
			field=createFieldFromTableMetaDataColumn(column);
			fields.put(field);
		}
		return fields;
	}
	
	public static JSONObject createFieldFromTableMetaDataColumn(final ColumnInfo column) throws JSONException
	
	{
		JSONObject field =new JSONObject();
		field.put(SEConstants.AVROConstants.FIELD_NAME, column.getColumnName());
		field.put(SEConstants.AVROConstants.FIELD_COLUMNNAME, column.getColumnName());
		field.put(SEConstants.AVROConstants.FIELD_COLUMNID, column.getColumnId());
		field.put(SEConstants.AVROConstants.FIELD_DATALENGTH, column.getDataLength());
		field.put(SEConstants.AVROConstants.FIELD_COLUMNNAME, column.getColumnName());
		field.put(SEConstants.AVROConstants.FIELD_DATASCALE, column.getDataScale());
		field.put(SEConstants.AVROConstants.FIELD_TYPE, getType(column.getDataType()));
		field.put(SEConstants.AVROConstants.FIELD_DATATYPE, column.getDataType());
		field.put(SEConstants.AVROConstants.FIELD_FORMAT, column.getFormat());
		return field;
	}
	
	public static String getType(final String type)
	{
		String returnType=null;
		
		if(type.equalsIgnoreCase("VARCHAR")){
			returnType="string";
		}else if(type.equalsIgnoreCase("DATE")){
			returnType="string";
		}else if(type.equalsIgnoreCase("TIMESTAMP")){
			returnType="string";
		}
		return returnType;
	}
	
}
