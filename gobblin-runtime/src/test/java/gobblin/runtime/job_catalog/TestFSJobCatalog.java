/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.Properties;
import org.mockito.Mockito;

import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;
import org.testng.annotations.Test;


/**
 * Test interaction between (Mutable)FsJobCatalog and its listeners.
 * Inherit the testing routine for InMemoryJobCatalog.
 */
public class TestFSJobCatalog {
  @Test
  public void testCallbacks()
      throws Exception {
    FSJobCatalog cat = new FSJobCatalog(new Properties());

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);

    JobSpec js1_1 = JobSpec.builder("test:job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test:job1").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builder("test:job1").withVersion("3").build();
    JobSpec js2 = JobSpec.builder("test:job2").withVersion("1").build();

    cat.put(js1_1);
    cat.addListener(l);
    cat.put(js1_2);
    cat.put(js2);
    cat.put(js1_3);
    cat.remove(js2.getUri());
    cat.remove(new URI("test:dummy_job"));
    cat.removeListener(l);
    cat.remove(js1_3.getUri());

    // Sleep long enough for the internal detector to react.
    Thread.sleep(2000);

    Mockito.verify(l).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));
    Mockito.verify(l).onAddJob(Mockito.eq(js2));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_2), Mockito.eq(js1_3));
    Mockito.verify(l).onDeleteJob(Mockito.eq(js2));

    Mockito.verifyNoMoreInteractions(l);
  }
}
