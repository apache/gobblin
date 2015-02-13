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

package gobblin.source.extractor.extract;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.watermark.WatermarkType;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.workunit.WorkUnit;


/**
 * An interface for protocol extractors
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public interface ProtocolSpecificLayer<S, D> {
  /**
   * Extract metadata(schema) from the source
   *
   * @param source schema name
   * @param source entity name
   * @param work unit
   * @throws SchemaException if there is anything wrong in extracting metadata
   */
  public void extractMetadata(String schema, String entity, WorkUnit workUnit)
      throws SchemaException, IOException;

  /**
   * High water mark for the snapshot pull
   * @param watermarkSourceFormat
   *
   * @param source schema name
   * @param source entity name
   * @param watermark column
   * @param watermark column format
   * @param list of all predicates that needs to be applied
   * @return high water mark
   * @throws SchemaException if there is anything wrong in getting high water mark
   */
  public long getMaxWatermark(String schema, String entity, String watermarkColumn,
      List<Predicate> snapshotPredicateList, String watermarkSourceFormat)
      throws HighWatermarkException;

  /**
   * Source record count
   *
   * @param source schema name
   * @param source entity name
   * @param work unit: properties
   * @param list of all predicates that needs to be applied
   * @return record count
   * @throws RecordCountException if there is anything wrong in getting record count
   */
  public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException;

  /**
   * record set: data records with an iterator
   *
   * @param source schema name
   * @param source entity name
   * @param work unit: properties
   * @param list of all predicates that needs to be applied
   * @return iterator with set of records
   * @throws SchemaException if there is anything wrong in getting data records
   */
  public Iterator<D> getRecordSet(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException, IOException;

  /**
   * water mark source format of water mark type
   * @return water mark source format(yyyyMMddHHmmss, yyyyMMdd etc.)
   */
  public String getWatermarkSourceFormat(WatermarkType watermarkType);

  /**
   * date predicate condition for types like timestamp and date
   * @return predicate condition (LastModifiedHour >= 10 and LastModifiedHour <= 20)
   */
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator);

  /**
   * date predicate condition for types like timestamp and date
   * @return predicate condition (LastModifiedDate >= 2014-01-01 and LastModifiedDate <= 2014-01-01)
   */
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator);

  /**
   * timestamp predicate condition for types like timestamp
   * @return predicate condition (LastModifiedTimestamp >= 2014-01-01T00:00:00.000Z and LastModifiedTimestamp <= 2014-01-10T15:05:00.000Z)
   */
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator);

  /**
   * set timeout for the source connection
   */
  public void setTimeOut(int timeOut);

  /**
   * Data type of source
   *
   * @return Map of source and target data types
   */
  public Map<String, String> getDataTypeMap();

  /**
   * Close connection after the completion of extract whether its success or failure
   * @throws Exception
   */
  public void closeConnection()
      throws Exception;

  /**
   * Get records using source specific api (Example: bulk api in salesforce source)
   * record set: data records with an iterator
   *
   * @param source schema name
   * @param source entity name
   * @param work unit: properties
   * @param list of all predicates that needs to be applied
   * @return iterator with set of records
   * @throws SchemaException if there is anything wrong in getting data records
   */
  public Iterator<D> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws IOException;
}
