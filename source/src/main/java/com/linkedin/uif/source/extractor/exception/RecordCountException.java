package com.linkedin.uif.source.extractor.exception;

public class RecordCountException extends Exception {

    private static final long serialVersionUID = 1L;

    public RecordCountException(String message) {
        super(message);
    }

    public RecordCountException(String message, Exception e) {
        super(message, e);
    }
}
