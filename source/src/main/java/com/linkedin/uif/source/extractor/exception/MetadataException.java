package com.linkedin.uif.source.extractor.exception;

public class MetadataException extends Exception {
    
    private static final long serialVersionUID = 1L;

    public MetadataException(String message) {
		super(message);
	}
    
    public MetadataException(String message, Exception e) {
        super(message, e);
    }
}
