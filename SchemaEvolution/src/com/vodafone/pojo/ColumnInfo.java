package com.vodafone.pojo;

public class ColumnInfo {
	
	private String sourceName;
	private String schemaName;
	private String tableName;
	private String columnName;
	private String dataType;
	private int dataLength;
	private String dataScale;
	private String format;
	private boolean primaryKey;
	private int columnId;
	public String getSourceName() {
		return sourceName;
	}
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public int getDataLength() {
		return dataLength;
	}
	public void setDataLength(int dataLength) {
		this.dataLength = dataLength;
	}
	public String getDataScale() {
		return dataScale;
	}
	public void setDataScale(String dataScale) {
		this.dataScale = dataScale;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public boolean isPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}
	public int getColumnId() {
		return columnId;
	}
	public void setColumnId(int columnId) {
		this.columnId = columnId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + columnId;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnInfo other = (ColumnInfo) obj;
		if (columnId != other.columnId)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "ColumnInfo [sourceName=" + sourceName + ", schemaName="
				+ schemaName + ", tableName=" + tableName + ", columnName="
				+ columnName + ", dataType=" + dataType + ", dataLength="
				+ dataLength + ", dataScale=" + dataScale + ", format="
				+ format + ", primaryKey=" + primaryKey + ", columnId="
				+ columnId + "]";
	}
	
	}
