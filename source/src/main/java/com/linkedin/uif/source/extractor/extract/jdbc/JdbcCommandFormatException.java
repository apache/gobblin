package com.linkedin.uif.source.extractor.extract.jdbc;

/**
 * Exception if jdbc command is failed to execute
 * 
 * @author nveeramr
 */

public class JdbcCommandFormatException extends Exception {
	private static final long serialVersionUID = 1L;

	public JdbcCommandFormatException(String message) {
		super(message);
	}

	public JdbcCommandFormatException(String message, Exception e) {
		super(message, e);
	}
}
