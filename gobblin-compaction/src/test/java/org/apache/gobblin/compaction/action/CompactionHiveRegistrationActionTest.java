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

package org.apache.gobblin.compaction.action;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.compaction.mapreduce.MRCompactionTask;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.dataset.SimpleFileSystemDataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.context.NameConflictException;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.EventSubmitter;


public class CompactionHiveRegistrationActionTest {

  @Test
  public void testEvents()
      throws Exception {

    WorkUnitState state = new WorkUnitState();
    String inputDir = "/data/tracking";
    String inputSubDir = "hourly";
    String destSubDir = "daily";
    String pathPattern = "%s/myTopic/%s/2019/12/20";

    String datasetPath = String.format(pathPattern, inputDir, inputSubDir);
    state.setProp(MRCompactor.COMPACTION_INPUT_DIR, inputDir);
    state.setProp(MRCompactor.COMPACTION_DEST_DIR, inputDir);
    state.setProp(MRCompactor.COMPACTION_INPUT_SUBDIR, inputSubDir);
    state.setProp(MRCompactor.COMPACTION_DEST_SUBDIR, destSubDir);

    state.setProp(MRCompactionTask.FILE_COUNT, "10");
    state.setProp(MRCompactionTask.RECORD_COUNT, "100");

    CompactionHiveRegistrationAction action = new CompactionHiveRegistrationAction(state);
    MockMetricContext mockMetricContext = new MockMetricContext(getClass().getName());
    String namespace = "compaction.tracking.events";
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(mockMetricContext, namespace).build();
    action.addEventSubmitter(eventSubmitter);

    FileSystemDataset dataset = new SimpleFileSystemDataset(new Path(datasetPath));
    action.onCompactionJobComplete(dataset);

    String destinationPath = String.format(pathPattern, inputDir, destSubDir);

    Assert.assertEquals(mockMetricContext.events.size(), 1);
    CountEventBuilder fileCountEvent = CountEventBuilder.fromEvent(mockMetricContext.events.get(0));
    Assert.assertEquals(fileCountEvent.getNamespace(), namespace);
    Assert.assertEquals(fileCountEvent.getName(), CompactionHiveRegistrationAction.NUM_OUTPUT_FILES);
    Assert.assertEquals(fileCountEvent.getCount(), 10);
    Map<String, String> metadata = fileCountEvent.getMetadata();
    Assert.assertEquals(metadata.get(CompactionHiveRegistrationAction.DATASET_URN), destinationPath);
    Assert.assertEquals(metadata.get(CompactionHiveRegistrationAction.RECORD_COUNT), "100");
    Assert.assertEquals(metadata.get(CompactionHiveRegistrationAction.BYTE_COUNT), "-1");
  }

  private class MockMetricContext extends MetricContext {

    List<GobblinTrackingEvent> events;

    MockMetricContext(String name)
        throws NameConflictException {
      super(name,null, Lists.newArrayList(), false);
      events = Lists.newArrayList();
    }

    @Override
    public void submitEvent(GobblinTrackingEvent nonReusableEvent) {
      events.add(nonReusableEvent);
    }
  }
}
