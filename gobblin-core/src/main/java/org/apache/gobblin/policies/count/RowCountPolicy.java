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

package gobblin.policies.count;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.qualitychecker.task.TaskLevelPolicy;


public class RowCountPolicy extends TaskLevelPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RowCountPolicy.class);
  private final long rowsRead;
  private final long rowsWritten;

  public RowCountPolicy(State state, TaskLevelPolicy.Type type) {
    super(state, type);
    this.rowsRead = state.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED);
    this.rowsWritten = state.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN);
  }

  @Override
  public Result executePolicy() {
    if (this.rowsRead == this.rowsWritten) {
      return Result.PASSED;
    }
    LOG.warn(this.getClass().getSimpleName() + " fails as read count and write count mismatch: " + this);
    return Result.FAILED;
  }

  @Override
  public String toString() {
    return String.format("RowCountPolicy [rowsRead=%s, rowsWritten=%s]", this.rowsRead, this.rowsWritten);
  }
}
