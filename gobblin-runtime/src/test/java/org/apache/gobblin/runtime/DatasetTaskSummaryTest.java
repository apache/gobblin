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

import java.lang.reflect.Type;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.reflect.TypeToken;

import org.apache.gobblin.metrics.DatasetMetric;
import org.apache.gobblin.runtime.util.GsonUtils;


public class DatasetTaskSummaryTest {

  private static final Type SUMMARY_LIST_TYPE = new TypeToken<List<DatasetTaskSummary>>() { }.getType();

  @Test
  public void testToDatasetMetricMapsRetentionCountsFromJson() {
    // The shape a DDM retention run emits: nothing written, deletion counts set.
    String json = "[{\"datasetUrn\":\"hdfs://nn/data/foo\",\"recordsWritten\":0,\"bytesWritten\":0,"
        + "\"successfullyCommitted\":true,\"versionsTotal\":10,\"versionsDeleted\":4,\"directoriesPruned\":1}]";
    List<DatasetTaskSummary> summaries = GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(json, SUMMARY_LIST_TYPE);

    DatasetMetric metric = DatasetTaskSummary.toDatasetMetric(summaries.get(0));

    Assert.assertEquals(metric.getDatasetUrn(), "hdfs://nn/data/foo");
    Assert.assertEquals(metric.getEntitiesWritten(), Long.valueOf(0L));
    Assert.assertEquals(metric.getBytesWritten(), Long.valueOf(0L));
    Assert.assertTrue(metric.getSuccessfullyCommitted());
    Assert.assertEquals(metric.getVersionsTotal(), Long.valueOf(10L));
    Assert.assertEquals(metric.getVersionsDeleted(), Long.valueOf(4L));
    Assert.assertEquals(metric.getDirectoriesPruned(), Long.valueOf(1L));
  }
}
