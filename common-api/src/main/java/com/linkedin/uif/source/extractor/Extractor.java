package com.linkedin.uif.source.extractor;

import java.io.IOException;


public interface Extractor<D, S> {
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
