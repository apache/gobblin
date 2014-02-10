package com.linkedin.uif.converter;

/**
 * A type of {@link Exception} thrown when there's anything wrong
 * with data conversion.
 */
public class DataConversionException extends Exception {

    public DataConversionException(Throwable cause) {
        super(cause);
    }

    public DataConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
