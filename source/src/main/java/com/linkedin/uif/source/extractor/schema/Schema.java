package com.linkedin.uif.source.extractor.schema;

import com.google.gson.JsonObject;

/**
 * Schema from extractor
 */
public class Schema {
	private String columnName;
	private JsonObject dataType;
	private boolean waterMark;
	private int primaryKey;
	private long length;
	private int precision;
	private int scale;
	private boolean nullable;
	private String format;
	private String comment;
	private String defaultValue;
	private boolean unique;
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public JsonObject getDataType() {
		return dataType;
	}

	public void setDataType(JsonObject dataType) {
		this.dataType = dataType;
	}

	public boolean getWaterMark() {
		return waterMark;
	}

	public void setWaterMark(boolean waterMark) {
		this.waterMark = waterMark;
	}

	public int getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(int primaryKey) {
		this.primaryKey = primaryKey;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public boolean getNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public boolean getUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}
}
