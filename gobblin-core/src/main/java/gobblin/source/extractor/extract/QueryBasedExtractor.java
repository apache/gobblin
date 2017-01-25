/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.source.extractor.extract;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.exception.ExtractPrepareException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.schema.ArrayDataType;
import gobblin.source.extractor.schema.DataType;
import gobblin.source.extractor.schema.EnumDataType;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.watermark.WatermarkPredicate;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.schema.MapDataType;
import gobblin.source.workunit.WorkUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of common extractor for query based sources.
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
@Slf4j
public abstract class QueryBasedExtractor<S, D> implements Extractor<S, D>, ProtocolSpecificLayer<S, D> {
  private static final Gson GSON = new Gson();
  protected final WorkUnitState workUnitState;
  protected final WorkUnit workUnit;
  private final String entity;
  private final String schema;

  private boolean fetchStatus = true;
  private S outputSchema;
  private long sourceRecordCount = 0;
  private long highWatermark;

  private Iterator<D> iterator;
  protected final List<String> columnList = new ArrayList<>();
  private final List<Predicate> predicateList = new ArrayList<>();

  private S getOutputSchema() {
    return this.outputSchema;
  }

  protected void setOutputSchema(S outputSchema) {
    this.outputSchema = outputSchema;
  }

  private long getSourceRecordCount() {
    return this.sourceRecordCount;
  }

  public boolean getFetchStatus() {
    return this.fetchStatus;
  }

  public void setFetchStatus(boolean fetchStatus) {
    this.fetchStatus = fetchStatus;
  }

  public void setHighWatermark(long highWatermark) {
    this.highWatermark = highWatermark;
  }

  private boolean isPullRequired() {
    return getFetchStatus();
  }

  private boolean isInitialPull() {
    return this.iterator == null;
  }

  public QueryBasedExtractor(WorkUnitState workUnitState) {
    this.workUnitState = workUnitState;
    this.workUnit = this.workUnitState.getWorkunit();
    this.schema = this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA);
    this.entity = this.workUnitState.getProp(ConfigurationKeys.SOURCE_ENTITY);
    MDC.put("tableName", getWorkUnitName());
  }

  private String getWorkUnitName() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(StringUtils.stripToEmpty(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA)));
    sb.append("_");
    sb.append(StringUtils.stripToEmpty(this.workUnitState.getProp(ConfigurationKeys.SOURCE_ENTITY)));
    sb.append("_");
    String id = this.workUnitState.getId();
    int seqIndex = id.lastIndexOf("_", id.length());
    if (seqIndex > 0) {
      String timeSeqStr = id.substring(0, seqIndex);
      int timeIndex = timeSeqStr.lastIndexOf("_", timeSeqStr.length());
      if (timeIndex > 0) {
        sb.append(id.substring(timeIndex + 1));
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    if (!this.isPullRequired()) {
      log.info("No more records to read");
      return null;
    }

    D nextElement = null;

    try {
      if (isInitialPull()) {
        log.info("Initial pull");
        this.iterator = this.getIterator();
      }

      if (this.iterator.hasNext()) {
        nextElement = this.iterator.next();

        if (!this.iterator.hasNext()) {
          log.debug("Getting next pull");
          this.iterator = this.getIterator();
          if (this.iterator == null) {
            this.setFetchStatus(false);
          }
        }
      }
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records using rest api; error - " + e.getMessage(), e);
    }
    return nextElement;
  }

  /**
   * Get iterator from protocol specific api if is.specific.api.active is false
   * Get iterator from source specific api if is.specific.api.active is true
   * @return iterator
   */
  private Iterator<D> getIterator() throws DataRecordException, IOException {
    if (Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_SPECIFIC_API_ACTIVE))) {
      return this.getRecordSetFromSourceApi(this.schema, this.entity, this.workUnit, this.predicateList);
    }
    return this.getRecordSet(this.schema, this.entity, this.workUnit, this.predicateList);
  }

  /**
   * get source record count from source
   * @return record count
   */
  @Override
  public long getExpectedRecordCount() {
    return this.getSourceRecordCount();
  }

  /**
   * get schema(Metadata) corresponding to the data records
   * @return schema
   */
  @Override
  public S getSchema() {
    return this.getOutputSchema();
  }

  /**
   * get high watermark of the current pull
   * @return high watermark
   */
  @Override
  public long getHighWatermark() {
    return this.highWatermark;
  }

  /**
   * close extractor read stream
   * update high watermark
   */
  @Override
  public void close() {
    log.info("Updating the current state high water mark with " + this.highWatermark);
    this.workUnitState.setActualHighWatermark(new LongWatermark(this.highWatermark));
    try {
      this.closeConnection();
    } catch (Exception e) {
      log.error("Failed to close the extractor", e);
    }
  }

  /**
   * @return full dump or not
   */
  public boolean isFullDump() {
    return Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY));
  }

  /**
   * build schema, record count and high water mark
   */
  public Extractor<S, D> build() throws ExtractPrepareException {
    String watermarkColumn = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
    long lwm = this.workUnit.getLowWatermark(LongWatermark.class).getValue();
    long hwm = this.workUnit.getExpectedHighWatermark(LongWatermark.class).getValue();
    log.info("Low water mark: " + lwm + "; and High water mark: " + hwm);

    WatermarkType watermarkType;
    if (StringUtils.isBlank(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE))) {
      watermarkType = null;
    } else {
      watermarkType = WatermarkType
          .valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE).toUpperCase());
    }

    log.info("Source Entity is " + this.entity);
    try {
      this.setTimeOut(
          this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_TIMEOUT, ConfigurationKeys.DEFAULT_CONN_TIMEOUT));
      this.extractMetadata(this.schema, this.entity, this.workUnit);

      if (StringUtils.isNotBlank(watermarkColumn)) {
        this.highWatermark = this.getLatestWatermark(watermarkColumn, watermarkType, lwm, hwm);
        log.info("High water mark from source: " + this.highWatermark);
        // If high water mark is found, then consider the same as runtime high water mark.
        // Else, consider the low water mark as high water mark(with no delta).i.e, don't move the pointer
        long currentRunHighWatermark = (this.highWatermark != ConfigurationKeys.DEFAULT_WATERMARK_VALUE
            ? this.highWatermark : this.getLowWatermarkWithNoDelta(lwm));

        log.info("High water mark for the current run: " + currentRunHighWatermark);
        this.setRangePredicates(watermarkColumn, watermarkType, lwm, currentRunHighWatermark);
        this.highWatermark = currentRunHighWatermark;
      }

      // if it is set to true, skip count calculation and set source count to -1
      if (!Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SKIP_COUNT_CALC))) {
        this.sourceRecordCount = this.getSourceCount(this.schema, this.entity, this.workUnit, this.predicateList);
      } else {
        log.info("Skip count calculation");
        this.sourceRecordCount = -1;
      }

      if (this.sourceRecordCount == 0) {
        log.info("Record count is 0; Setting fetch status to false to skip readRecord()");
        this.setFetchStatus(false);
      }
    } catch (SchemaException e) {
      throw new ExtractPrepareException("Failed to get schema for this object; error - " + e.getMessage(), e);
    } catch (HighWatermarkException e) {
      throw new ExtractPrepareException("Failed to get high watermark; error - " + e.getMessage(), e);
    } catch (RecordCountException e) {
      throw new ExtractPrepareException("Failed to get record count; error - " + e.getMessage(), e);
    } catch (Exception e) {
      throw new ExtractPrepareException("Failed to prepare the extract build; error - " + e.getMessage(), e);
    }
    return this;
  }

  private long getLowWatermarkWithNoDelta(long lwm) {
    if (lwm == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
      return ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
    }

    String watermarkType = this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "TIMESTAMP");
    WatermarkType wmType = WatermarkType.valueOf(watermarkType.toUpperCase());
    int deltaNum = new WatermarkPredicate(wmType).getDeltaNumForNextWatermark();

    switch (wmType) {
      case SIMPLE:
        return lwm - deltaNum;
      default:
        Date lowWaterMarkDate = Utils.toDate(lwm, "yyyyMMddHHmmss");
        return Long
            .parseLong(Utils.dateToString(Utils.addSecondsToDate(lowWaterMarkDate, deltaNum * -1), "yyyyMMddHHmmss"));
    }
  }

  /**
   * if snapshot extract, get latest watermark else return work unit high watermark
   *
   * @param watermark column
   * @param low watermark value
   * @param high watermark value
   * @param column format
   * @return letst watermark
   * @throws IOException
   */
  private long getLatestWatermark(String watermarkColumn, WatermarkType watermarkType, long lwmValue, long hwmValue)
      throws HighWatermarkException, IOException {

    if (!Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SKIP_HIGH_WATERMARK_CALC))) {
      log.info("Getting high watermark");
      List<Predicate> list = new ArrayList<>();
      WatermarkPredicate watermark = new WatermarkPredicate(watermarkColumn, watermarkType);
      Predicate lwmPredicate = watermark.getPredicate(this, lwmValue, ">=", Predicate.PredicateType.LWM);
      Predicate hwmPredicate = watermark.getPredicate(this, hwmValue, "<=", Predicate.PredicateType.HWM);
      if (lwmPredicate != null) {
        list.add(lwmPredicate);
      }
      if (hwmPredicate != null) {
        list.add(hwmPredicate);
      }

      return this.getMaxWatermark(this.schema, this.entity, watermarkColumn, list,
          watermark.getWatermarkSourceFormat(this));
    }

    return hwmValue;
  }

  /**
   * range predicates for watermark column and transaction columns.
   * @param string
   * @param watermarkType
   * @param watermark column
   * @param date column(for appends)
   * @param hour column(for appends)
   * @param batch column(for appends)
   * @param low watermark value
   * @param high watermark value
   */
  private void setRangePredicates(String watermarkColumn, WatermarkType watermarkType, long lwmValue, long hwmValue) {
    log.debug("Getting range predicates");
    WatermarkPredicate watermark = new WatermarkPredicate(watermarkColumn, watermarkType);
    this.addPredicates(watermark.getPredicate(this, lwmValue, ">=", Predicate.PredicateType.LWM));
    this.addPredicates(watermark.getPredicate(this, hwmValue, "<=", Predicate.PredicateType.HWM));

    if (Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_HOURLY_EXTRACT))) {
      String hourColumn = this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_HOUR_COLUMN);
      if (StringUtils.isNotBlank(hourColumn)) {
        WatermarkPredicate hourlyWatermark = new WatermarkPredicate(hourColumn, WatermarkType.HOUR);
        this.addPredicates(hourlyWatermark.getPredicate(this, lwmValue, ">=", Predicate.PredicateType.LWM));
        this.addPredicates(hourlyWatermark.getPredicate(this, hwmValue, "<=", Predicate.PredicateType.HWM));
      }
    }
  }

  /**
   * add predicate to the predicate list
   * @param Predicate(watermark column,type,format and condition)
   * @return watermark list
   */
  private void addPredicates(Predicate predicate) {
    if (predicate != null) {
      this.predicateList.add(predicate);
    }
  }

  /**
   * @param given list of watermark columns
   * @param column name to search for
   * @return true, if column name is part of water mark columns. otherwise, return false
   */
  protected boolean isWatermarkColumn(String watermarkColumn, String columnName) {
    if (columnName != null) {
      columnName = columnName.toLowerCase();
    }

    if (StringUtils.isNotBlank(watermarkColumn)) {
      List<String> waterMarkColumnList = Arrays.asList(watermarkColumn.toLowerCase().split(","));
      if (waterMarkColumnList.contains(columnName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param given list of watermark columns
   * @return true, if there are multiple water mark columns. otherwise, return false
   */
  protected boolean hasMultipleWatermarkColumns(String watermarkColumn) {
    if (StringUtils.isBlank(watermarkColumn)) {
      return false;
    }

    return Arrays.asList(watermarkColumn.toLowerCase().split(",")).size() > 1;
  }

  /**
   * @param given list of primary key columns
   * @param column name to search for
   * @return index of the column if it exist in given list of primary key columns. otherwise, return 0
   */
  protected int getPrimarykeyIndex(String primarykeyColumn, String columnName) {
    if (columnName != null) {
      columnName = columnName.toLowerCase();
    }

    if (StringUtils.isNotBlank(primarykeyColumn)) {
      List<String> primarykeyColumnList = Arrays.asList(primarykeyColumn.toLowerCase().split(","));
      return primarykeyColumnList.indexOf(columnName) + 1;
    }
    return 0;
  }

  /**
   * @param column name to search for
   * @param list of metadata columns
   * @return true if column is part of metadata columns. otherwise, return false.
   */
  protected boolean isMetadataColumn(String columnName, List<String> columnList) {
    boolean isColumnCheckEnabled =
        Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_METADATA_COLUMN_CHECK_ENABLED,
            ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_IS_METADATA_COLUMN_CHECK_ENABLED));

    if (!isColumnCheckEnabled) {
      return true;
    }

    columnName = columnName.trim().toLowerCase();
    if (columnList.contains(columnName)) {
      return true;
    }
    return false;
  }

  /**
   * @param column name
   * @param data type
   * @param data type of elements
   * @param elements
   * @return converted data type
   */
  protected JsonObject convertDataType(String columnName, String type, String elementType, List<String> enumSymbols) {
    String dataType = this.getDataTypeMap().get(type);
    if (dataType == null) {
      dataType = "string";
    }
    DataType convertedDataType;
    if (dataType.equals("map")) {
      convertedDataType = new MapDataType(dataType, elementType);
    } else if (dataType.equals("array")) {
      convertedDataType = new ArrayDataType(dataType, elementType);
    } else if (dataType.equals("enum")) {
      convertedDataType = new EnumDataType(dataType, columnName, enumSymbols);
    } else {
      convertedDataType = new DataType(dataType);
    }

    return GSON.fromJson(GSON.toJson(convertedDataType), JsonObject.class).getAsJsonObject();
  }

  /**
   * @param predicate list
   * @return true, if there are any predicates. otherwise, return false.
   */
  protected boolean isPredicateExists(List<Predicate> predicateList) {
    if (predicateList == null || predicateList.isEmpty()) {
      return false;
    }
    return true;
  }
}
