package com.linkedin.uif.runtime;

/**
 * A type of {@link java.lang.Exception}s thrown when the number of schemas or
 * data records returned by a {@link com.linkedin.uif.converter.ForkOperator}
 * is not equal to the number of declared branches.
 *
 * @author ynli
 */
public class ForkBranchMismatchException extends Exception {

    public ForkBranchMismatchException(String message) {
        super(message);
    }
}
