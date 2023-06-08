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

package org.apache.gobblin.iceberg.writer;

import java.io.IOException;
import java.util.Map;
import org.apache.gobblin.completeness.verifier.KafkaAuditCountVerifier;
import org.apache.gobblin.configuration.State;

import static org.apache.gobblin.iceberg.writer.IcebergMetadataWriterConfigKeys.*;

/**
 * A class to check and update completion watermark based on srcCount/sum_of(refCount).
 * See {@link KafkaAuditCountVerifier#isTotalCountComplete(String, long, long)}
 */
public class TotalCountCompletenessWatermarkUpdater extends AbstractCompletenessWatermarkUpdater{
  public TotalCountCompletenessWatermarkUpdater(String topic, String auditCheckGranularity, String timeZone,
      IcebergMetadataWriter.TableMetadata tableMetadata, Map<String, String> propsToUpdate, State stateToUpdate,
      KafkaAuditCountVerifier auditCountVerifier) {
    super(topic, auditCheckGranularity, timeZone, tableMetadata, propsToUpdate, stateToUpdate, auditCountVerifier);
  }

  @Override
  boolean checkCompleteness(String datasetName, long beginInMillis, long endInMillis) throws IOException {
    return this.auditCountVerifier.isTotalCountComplete(datasetName, beginInMillis, endInMillis);
  }

  @Override
  void updateProps(long newCompletenessWatermark) {
    this.propsToUpdate.put(TOTAL_COUNT_COMPLETION_WATERMARK_KEY, String.valueOf(newCompletenessWatermark));
  }

  @Override
  void updateState(String catalogDbTableNameLowerCased, long newCompletenessWatermark) {
    this.stateToUpdate.setProp(
        String.format(STATE_TOTAL_COUNT_COMPLETION_WATERMARK_KEY_OF_TABLE, catalogDbTableNameLowerCased),
        newCompletenessWatermark);
  }

  @Override
  void updateCompletionWatermarkInTableMetadata(long newCompletenessWatermark) {
    this.tableMetadata.totalCountCompletionWatermark = newCompletenessWatermark;
  }

  @Override
  long getCompletionWatermarkFromTableMetadata() {
    return this.tableMetadata.totalCountCompletionWatermark;
  }
}
