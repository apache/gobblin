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

package org.apache.gobblin.ingestion.google;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.writer.partitioner.WriterPartitioner;


/**
 * This day partitioner is responsible to for partition the output into the layout as
 * - timestamp_append
 *  - yyyy
 *    - MM
 *      - dd
 *        - part.your.data.output.avro
 *
 * In order to get the date column for partitioning, you must provide GoggleIngestionConfigurationKeys.KEY_DATE_COLUMN_NAME,
 * otherwise, the column will be default to "Date". And the date format is default to "yyyy-MM-dd",
 * you can change it by configuring the key GoggleIngestionConfigurationKeys.KEY_DATE_FORMAT.
 *
 * You can futher enable adding column names to the output paths.
 * If you turn on column names option (configured by GoggleIngestionConfigurationKeys.KEY_INCLUDE_COLUMN_NAMES), the layout would become
 * - timestamp_append
 *  - year=yyyy
 *    - month=MM
 *      - day=dd
 *        - part.your.data.output.avro
 */
public class DayPartitioner implements WriterPartitioner<GenericRecord> {
  private static final String PARTITION_COLUMN_PREFIX = "type";
  private static final String PARTITION_COLUMN_YEAR = "year";
  private static final String PARTITION_COLUMN_MONTH = "month";
  private static final String PARTITION_COLUMN_DAY = "day";

  private static final String DEFAULT_DATE_COLUMN = "Date";
  private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
  private static final String NAME = "YearMonthDayPartitioner";
  private static final String NAME_SPACE = "gobblin.ingestion.google";

  private final boolean _withColumnNames;
  private final String _prefix;
  private final boolean _withPrefix;
  private final String _dateColumn;
  private final DateTimeFormatter _dateFormatter;
  private final Schema _partitionSchema;

  public DayPartitioner(State state, int numBranches, int branchId) {
    _withColumnNames = state.getPropAsBoolean(GoggleIngestionConfigurationKeys.KEY_INCLUDE_COLUMN_NAMES, false);
    _prefix = state.getProp(GoggleIngestionConfigurationKeys.KEY_PARTITIONER_PREFIX);
    _withPrefix = StringUtils.isNotBlank(_prefix);

    _dateColumn = state.getProp(GoggleIngestionConfigurationKeys.KEY_DATE_COLUMN_NAME, DEFAULT_DATE_COLUMN);
    _dateFormatter =
        DateTimeFormat.forPattern(state.getProp(GoggleIngestionConfigurationKeys.KEY_DATE_FORMAT, DEFAULT_DATE_FORMAT));

    SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record(NAME).namespace(NAME_SPACE).fields();
    Schema stringType = Schema.create(Schema.Type.STRING);

    if (_withPrefix) {
      assembler = assembler.name(PARTITION_COLUMN_PREFIX).type(stringType).noDefault();
    }
    _partitionSchema =
        assembler.name(PARTITION_COLUMN_YEAR).type(stringType).noDefault().name(PARTITION_COLUMN_MONTH).type(stringType)
            .noDefault().name(PARTITION_COLUMN_DAY).type(stringType).noDefault().endRecord();
  }

  @Override
  public Schema partitionSchema() {
    return _partitionSchema;
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = new GenericData.Record(_partitionSchema);
    String dateString = record.get(_dateColumn).toString();
    DateTime date = _dateFormatter.parseDateTime(dateString);

    if (_withPrefix) {
      if (_withColumnNames) {
        partition.put(PARTITION_COLUMN_PREFIX, PARTITION_COLUMN_PREFIX + "=" + _prefix);
      } else {
        partition.put(PARTITION_COLUMN_PREFIX, _prefix);
      }
    }

    if (_withColumnNames) {
      partition.put(PARTITION_COLUMN_YEAR, PARTITION_COLUMN_YEAR + "=" + date.getYear());
      partition.put(PARTITION_COLUMN_MONTH, PARTITION_COLUMN_MONTH + "=" + date.getMonthOfYear());
      partition.put(PARTITION_COLUMN_DAY, PARTITION_COLUMN_DAY + "=" + date.getDayOfMonth());
    } else {
      partition.put(PARTITION_COLUMN_YEAR, date.getYear());
      partition.put(PARTITION_COLUMN_MONTH, date.getMonthOfYear());
      partition.put(PARTITION_COLUMN_DAY, date.getDayOfMonth());
    }

    return partition;
  }
}
