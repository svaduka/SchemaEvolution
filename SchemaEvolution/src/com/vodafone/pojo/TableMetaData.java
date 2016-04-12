package com.vodafone.pojo;

import java.util.Set;

public class TableMetaData {
	
	private String SOURCE_NAME;
	private String SCHEMA_NAME;
	private String TABLE_NAME;

	private Set<ColumnInfo> columns;

	public String getSOURCE_NAME() {
		return SOURCE_NAME;
	}

	public void setSOURCE_NAME(String sOURCE_NAME) {
		SOURCE_NAME = sOURCE_NAME;
	}

	public String getSCHEMA_NAME() {
		return SCHEMA_NAME;
	}

	public void setSCHEMA_NAME(String sCHEMA_NAME) {
		SCHEMA_NAME = sCHEMA_NAME;
	}

	public String getTABLE_NAME() {
		return TABLE_NAME;
	}

	public void setTABLE_NAME(String tABLE_NAME) {
		TABLE_NAME = tABLE_NAME;
	}

	public Set<ColumnInfo> getColumns() {
		return columns;
	}

	public void setColumns(Set<ColumnInfo> columns) {
		this.columns = columns;
	}
	
	
	
}
