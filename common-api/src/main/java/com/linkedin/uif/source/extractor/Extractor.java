/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.source.extractor;

import java.io.Closeable;
import java.io.IOException;

/**
 * <p>
 *     Responsible for pulling data from a data source. All source specific logic for
 *     a data source should be encapsulated in an implementation of this class and
 *     {@link com.linkedin.uif.source.Source}.
 * </p>
 *
 * @author kgoodhop
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
public interface Extractor<S, D> extends Closeable {

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
	public D readRecord(D reuse) throws DataRecordException, IOException;

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
