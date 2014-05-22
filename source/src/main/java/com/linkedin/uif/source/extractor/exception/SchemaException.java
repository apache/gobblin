package com.linkedin.uif.source.extractor.exception;

public class SchemaException extends Exception {
    
	private static final long serialVersionUID = 1L;

	public SchemaException(String message) {
		super(message);
	}
	
    public SchemaException(String message, Exception e) {
        super(message, e);
    }
}
