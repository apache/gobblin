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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;


public class FsWriterMetricsTest {
  @Test
  public void testSerialization() throws IOException {
    final String WRITER_ID = "foobar123";
    final PartitionIdentifier PARTITION_KEY = new PartitionIdentifier("_partitionInfo", 3);
    final Set<FsWriterMetrics.FileInfo> FILE_INFOS = ImmutableSet.of(
        new FsWriterMetrics.FileInfo("file1", 1234),
        new FsWriterMetrics.FileInfo("file2", 4321)
    );

    String metricsJson = new FsWriterMetrics(WRITER_ID, PARTITION_KEY, FILE_INFOS).toJson();
    FsWriterMetrics parsedMetrics = FsWriterMetrics.fromJson(metricsJson);

    Assert.assertEquals(parsedMetrics.writerId, WRITER_ID);
    Assert.assertEquals(parsedMetrics.partitionInfo, PARTITION_KEY);
    Assert.assertEquals(parsedMetrics.fileInfos, FILE_INFOS);
  }
}
