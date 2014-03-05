package com.linkedin.uif.source.extractor.schema;

import java.util.List;

public class EnumDataType extends DataType {
	String name;
	List<String> symbols;
	
	public EnumDataType(String type, String name, List<String> symbols) {
		super(type);
		this.name = name;
		this.symbols = symbols;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getSymbols() {
		return symbols;
	}
	public void setSymbols(List<String> symbols) {
		this.symbols = symbols;
	}
}
