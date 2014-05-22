package com.linkedin.uif.source.extractor.exception;

public class RestApiProcessingException extends Exception {
	
    private static final long serialVersionUID = 1L;

	public RestApiProcessingException(String message) {
		super(message);
	}
	
    public RestApiProcessingException(String message, Exception e) {
        super(message, e);
    }
}
