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

package org.apache.gobblin.policies.count;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;


public class RowCountRangePolicy extends TaskLevelPolicy {
  private final long rowsRead;
  private final long rowsWritten;
  private final double range;

  private static final Logger LOG = LoggerFactory.getLogger(RowCountRangePolicy.class);

  public RowCountRangePolicy(State state, Type type) {
    super(state, type);
    this.rowsRead = state.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED);
    this.rowsWritten = state.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
    this.range = state.getPropAsDouble(ConfigurationKeys.ROW_COUNT_RANGE);
  }

  @Override
  public Result executePolicy() {
    double computedRange = Math.abs((this.rowsWritten - this.rowsRead) / (double) this.rowsRead);
    if (computedRange <= this.range) {
      return Result.PASSED;
    }
    LOG.error(String.format(
        "RowCountRangePolicy check failed. Rows read %s, Rows written %s, computed range %s, expected range %s ",
        this.rowsRead, this.rowsWritten, computedRange, this.range));

    return Result.FAILED;
  }
}
