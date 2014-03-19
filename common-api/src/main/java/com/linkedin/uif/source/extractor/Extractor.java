package com.linkedin.uif.source.extractor;

import java.io.IOException;

import com.linkedin.uif.source.extractor.DataRecordException;

/**
 * <p>Responsible for pulling data from a data source.  All source specific logic for
 * a data source should be encapsulated in an implementation of this class and {@link Source}
 * </p>
 * @author kgoodhop
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
public interface Extractor<S, D> {
	/**
	 * get schema(Metadata) corresponding to the data records
	 * @return schema
	 */
	public S getSchema();

	/**
	 * Read a data record from source
	 * 
	 * @throws DataRecordException,IOException if it can't read data record
	 * @return record of type D
	 */
	public D readRecord() throws DataRecordException, IOException;

	/**
	 * close extractor read stream
	 * update high watermark
	 */
	public void close();

	/**
	 * get source record count from source
	 * @return record count
	 */
	public long getExpectedRecordCount();

	/**
	 * get calculated high watermark of the current pull
	 * @return high watermark
	 */
	public long getHighWatermark();
}
