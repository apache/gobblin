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

package gobblin.policies.count;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.qualitychecker.task.TaskLevelPolicy;


public class RowCountRangePolicy extends TaskLevelPolicy {
  private final long rowsRead;
  private final long rowsWritten;
  private final double range;

  public RowCountRangePolicy(State state, Type type) {
    super(state, type);
    this.rowsRead = state.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED);
    this.rowsWritten = state.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
    this.range = state.getPropAsDouble(ConfigurationKeys.ROW_COUNT_RANGE);
  }

  @Override
  public Result executePolicy() {
    if (Math.abs((this.rowsWritten - this.rowsRead) / this.rowsRead) <= this.range) {
      return Result.PASSED;
    } else {
      return Result.FAILED;
    }
  }
}
