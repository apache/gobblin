/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.compliance;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;


/**
 * This extractor doesn't extract anything, but is used to pass {@link HivePurgerPartitionRecord} to the converter.
 *
 * @author adsharma
 */
public class HivePurgerExtractor implements Extractor<HivePurgerPartitionRecordSchema, HivePurgerPartitionRecord> {
  private boolean read = false;
  private WorkUnitState state;
  private HivePurgerPartitionRecord record = null;

  public HivePurgerExtractor(WorkUnitState state) {
    this.state = state;
  }

  @Override
  public HivePurgerPartitionRecordSchema getSchema() {
    return new HivePurgerPartitionRecordSchema();
  }

  /**
   * There is only one record ({@link HivePurgerPartitionRecord}) to be read.
   * @param record
   * @return {@link HivePurgerPartitionRecord}
   */
  @Override
  public HivePurgerPartitionRecord readRecord(HivePurgerPartitionRecord record)
      throws IOException {
    if (read) {
      return null;
    }
    read = true;
    if (this.record != null) {
      return this.record;
    }
    // There is only one record to be read per partition, and must return null after that to indicate end of reading.
    String partitionName = HivePurgerConfigurationKeys.PARTITION_NAME;
    return new HivePurgerPartitionRecord(state.getWorkunit().getProp(partitionName), state);
  }

  @Override
  public long getExpectedRecordCount() {
    return 1;
  }

  /**
   * Watermark is not managed by this extractor.
   */
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close()
      throws IOException {
  }

  @VisibleForTesting
  public void setRecord(HivePurgerPartitionRecord record) {
    this.record = record;
  }
}
