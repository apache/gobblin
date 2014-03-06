package com.linkedin.uif.source.extractor.resultset;

/**
 * An interface to extract data records using an iterator
 *
 * @param <D> type of data record
 */
public interface RecordSet<D> extends Iterable<D> {

    /**
     * add record to the list
     */
	public void add(D record);
	
    /**
     * check is there are any records
     */
	public boolean isEmpty();
}
