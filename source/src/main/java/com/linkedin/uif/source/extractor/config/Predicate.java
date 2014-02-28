package com.linkedin.uif.source.extractor.config;

/**
 * An implementation for predicate conditions
 * columnName : name of the column
 * value: value
 * condition: predicate condition using column and value
 * format: column format
 */
public class Predicate {
	public String columnName;
	public long value;
	public String condition;
	public String format;

	public Predicate(String columnName, long value, String condition, String format) {
		this.columnName = columnName;
		this.value = value;
		this.condition = condition;
		this.format = format;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}
}