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

package gobblin.source.extractor;

import java.io.Closeable;
import java.io.IOException;


/**
 * An interface for classes that are responsible for extracting data from a data source.
 *
 * <p>
 *     All source specific logic for a data source should be encapsulated in an
 *     implementation of this interface and {@link gobblin.source.Source}.
 * </p>
 *
 * @author kgoodhop
 *
 * @param <S> output schema type
 * @param <D> output record type
 */
public interface Extractor<S, D> extends Closeable {

  /**
   * Get the schema (Metadata) of the extracted data records.
   *
   * @return schema of the extracted data records
   */
  public S getSchema();

  /**
   * Read a data record from the data source.
   *
   * <p>
   *   This method allows data record object reuse through the one passed in if the
   *   implementation class decides to do so.
   * </p>
   *
   * @param reuse the data record object to be used
   * @return a data record
   * @throws DataRecordException if there is problem with the extracted data record
   * @throws java.io.IOException if there is problem extract a data record from the source
   */
  public D readRecord(D reuse)
      throws DataRecordException, IOException;

  /**
   * Get the expected source record count.
   *
   * @return expected source record count
   */
  public long getExpectedRecordCount();

  /**
   * Get the calculated high watermark up to which data records are to be extracted.
   * @return high watermark
   */
  public long getHighWatermark();
}
