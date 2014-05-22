package com.linkedin.uif.source.extractor.exception;

public class RestApiClientException extends Exception {
	
    private static final long serialVersionUID = 1L;

    public RestApiClientException(String message) {
		super(message);
	}
    
    public RestApiClientException(String message, Exception e) {
        super(message, e);
    }
}
