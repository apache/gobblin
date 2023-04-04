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

package org.apache.gobblin.runtime;

/**
 * A class returned by {@link org.apache.gobblin.runtime.SafeDatasetCommit} to provide metrics for the dataset
 * that can be reported as a single event in the commit phase.
 */
public class DatasetTaskSummary {
  private final String datasetUrn;
  private final long recordsWritten;
  private final long bytesWritten;

  public DatasetTaskSummary(String datasetUrn, long recordsWritten, long bytesWritten) {
    this.datasetUrn = datasetUrn;
    this.recordsWritten = recordsWritten;
    this.bytesWritten = bytesWritten;
  }

  public String getDatasetUrn() {
    return datasetUrn;
  }

  public long getRecordsWritten() {
    return recordsWritten;
  }

  public long getBytesWritten() {
    return bytesWritten;
  }
}
