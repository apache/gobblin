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

package org.apache.gobblin.runtime.job_catalog;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.api.JobCatalogListener;
import org.apache.gobblin.runtime.api.JobSpec;

/** Unit tests for {@link JobCatalogListenersList} */
public class TestJobCatalogListenersList {

  @Test
  public void testCalls() {
    JobCatalogListenersList ll = new JobCatalogListenersList();

    JobSpec js1_1 = JobSpec.builder("test:job1").build();
    JobSpec js1_2 = JobSpec.builder("test:job1").withVersion("2").build();
    JobSpec js2 = JobSpec.builder("test:job2").build();

    JobCatalogListener l1 = Mockito.mock(JobCatalogListener.class);
    Mockito.doThrow(new RuntimeException("injected l1 failure")).when(l1)
        .onDeleteJob(Mockito.eq(js2.getUri()), Mockito.eq(js2.getVersion()));

    JobCatalogListener l2 = Mockito.mock(JobCatalogListener.class);
    Mockito.doThrow(new RuntimeException("injected l2 failure")).when(l2).onUpdateJob(Mockito.eq(js1_2));

    JobCatalogListener l3 = Mockito.mock(JobCatalogListener.class);
    Mockito.doThrow(new RuntimeException("injected l3 failure")).when(l3).onAddJob(Mockito.eq(js2));

    ll.addListener(l1);
    ll.addListener(l2);
    ll.addListener(l3);

    ll.onAddJob(js1_1);
    ll.onAddJob(js2);
    ll.onUpdateJob(js1_2);
    ll.onDeleteJob(js2.getUri(), js2.getVersion());

    Mockito.verify(l1).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l1).onAddJob(Mockito.eq(js2));
    Mockito.verify(l1).onUpdateJob(Mockito.eq(js1_2));
    Mockito.verify(l1).onDeleteJob(Mockito.eq(js2.getUri()), Mockito.eq(js2.getVersion()));

    Mockito.verify(l2).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l2).onAddJob(Mockito.eq(js2));
    Mockito.verify(l2).onUpdateJob(Mockito.eq(js1_2));
    Mockito.verify(l2).onDeleteJob(Mockito.eq(js2.getUri()), Mockito.eq(js2.getVersion()));

    Mockito.verify(l3).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l3).onAddJob(Mockito.eq(js2));
    Mockito.verify(l3).onUpdateJob(Mockito.eq(js1_2));
    Mockito.verify(l3).onDeleteJob(Mockito.eq(js2.getUri()), Mockito.eq(js2.getVersion()));

    Mockito.verifyNoMoreInteractions(l1, l2, l3);

  }

}
