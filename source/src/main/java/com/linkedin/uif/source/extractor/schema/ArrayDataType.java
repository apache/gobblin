package com.linkedin.uif.source.extractor.schema;

public class ArrayDataType extends DataType{
	String items;
	
	public ArrayDataType(String type, String items) {
		super(type);
		this.items = items;
	}
	
	public String getItems() {
		return items;
	}
	public void setItems(String items) {
		this.items = items;
	}
}
