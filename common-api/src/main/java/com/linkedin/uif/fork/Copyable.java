package com.linkedin.uif.fork;

/**
 * An interface for classes that supports making copies of their instances.
 *
 * @author ynli
 */
public interface Copyable<T> {

    /**
     * Make a new copy of this instance.
     *
     * @return new copy of this instance
     */
    public T copy() throws CopyNotSupportedException;
}
