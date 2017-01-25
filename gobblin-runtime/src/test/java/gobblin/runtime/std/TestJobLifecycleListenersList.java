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
package gobblin.runtime.std;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import gobblin.runtime.api.JobCatalogListenersContainer;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobSpecSchedulerListenersContainer;

/**
 * Unit tests for {@link JobLifecycleListenersList}
 */
public class TestJobLifecycleListenersList {

  @Test public void testHappyPath() {
    Logger log = LoggerFactory.getLogger("testHappyPath");
    JobCatalogListenersContainer jobCatalog = mock(JobCatalogListenersContainer.class);
    JobSpecSchedulerListenersContainer jobScheduler = mock(JobSpecSchedulerListenersContainer.class);
    JobExecutionDriver mockDriver = mock(JobExecutionDriver.class);
    JobExecutionState mockState = mock(JobExecutionState.class);

    JobLifecycleListener listener1 = mock(JobLifecycleListener.class);
    JobLifecycleListener listener2 = mock(JobLifecycleListener.class);

    JobLifecycleListenersList disp = new JobLifecycleListenersList(jobCatalog, jobScheduler, log);

    disp.registerJobLifecycleListener(listener1);
    disp.onJobLaunch(mockDriver);

    disp.registerWeakJobLifecycleListener(listener2);
    disp.onMetadataChange(mockState, "key", "oldValue", "newValue");

    verify(jobCatalog).addListener(eq(listener1));
    verify(jobScheduler).registerJobSpecSchedulerListener(eq(listener1));
    verify(listener1).onJobLaunch(eq(mockDriver));
    verify(listener2, never()).onJobLaunch(eq(mockDriver));

    verify(jobCatalog).registerWeakJobCatalogListener(eq(listener2));
    verify(jobScheduler).registerWeakJobSpecSchedulerListener(eq(listener2));
    verify(listener1).onMetadataChange(eq(mockState), eq("key"), eq("oldValue"), eq("newValue"));
    verify(listener2).onMetadataChange(eq(mockState), eq("key"), eq("oldValue"), eq("newValue"));
  }

}
