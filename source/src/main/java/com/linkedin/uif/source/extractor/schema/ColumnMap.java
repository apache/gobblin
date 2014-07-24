package com.linkedin.uif.source.extractor.schema;

/**
 * Column and alias mapping object
 * 
 * @author nveeramr
 */
public class ColumnMap {
	String columnName;
	String AliasName;
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getAliasName() {
		return AliasName;
	}
	public void setAliasName(String aliasName) {
		AliasName = aliasName;
	}
}
