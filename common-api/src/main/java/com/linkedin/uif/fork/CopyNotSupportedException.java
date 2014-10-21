package com.linkedin.uif.fork;

/**
 * A type of {@link java.lang.Exception}s thrown when copying is not supported.
 *
 * @author ynli
 */
public class CopyNotSupportedException extends Exception {

    public CopyNotSupportedException(String message) {
        super(message);
    }
}
