package com.linkedin.uif.converter;

/**
 * A type of {@link Exception} thrown when there's anything wrong
 * with schema conversion.
 */
public class SchemaConversionException extends Exception {

    public SchemaConversionException(Throwable cause) {
        super(cause);
    }

    public SchemaConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
