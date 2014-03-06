package com.linkedin.uif.source.extractor.schema;

public class MapDataType extends DataType {
	String values;
	
	public MapDataType(String type, String values) {
		super(type);
		this.values = values;
	}
	
	public String getValues() {
		return values;
	}
	public void setValues(String values) {
		this.values = values;
	}
}
