package com.linkedin.uif.source.extractor.exception;

public class HighWatermarkException extends Exception {
    
	private static final long serialVersionUID = 1L;

	public HighWatermarkException(String message) {
		super(message);
	}
	
	public HighWatermarkException(String message, Exception e) {
	    super(message, e);
	}
}
