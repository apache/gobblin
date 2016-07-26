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
package gobblin.runtime.std;

import java.net.URI;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.job_catalog.CachingJobCatalog;
import gobblin.runtime.job_catalog.InMemoryJobCatalog;
import gobblin.runtime.job_catalog.MutableCachingJobCatalog;

/** Unit tests for {@link CachingJobCatalog} and {@link MutableCachingJobCatalog} */
public class TestMutableCachingJobCatalog {

  @Test
  public void test() throws Exception {
    InMemoryJobCatalog baseCat =
        new InMemoryJobCatalog(Optional.<Logger>of(LoggerFactory.getLogger("baseCat")));
    MutableCachingJobCatalog cachedCat =
        new MutableCachingJobCatalog(baseCat, Optional.<Logger>of(LoggerFactory.getLogger("cachedCat")));

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);
    cachedCat.addListener(l);

    JobSpec js1_1 = JobSpec.builder("test:job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test:job1").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builder("test:job1").withVersion("3").build();
    URI jsURI = new URI("test:job1");

    baseCat.put(js1_1);
    JobSpec res = cachedCat.getJobSpec(new URI("test:job1"));
    Assert.assertEquals(res, js1_1);

    baseCat.put(js1_2);
    res = cachedCat.getJobSpec(jsURI);
    Assert.assertEquals(res, js1_2);

    baseCat.remove(jsURI);
    try {
      cachedCat.getJobSpec(jsURI);
      Assert.fail("Expected JobSpecNotFoundException");
    } catch (JobSpecNotFoundException e) {
      Assert.assertEquals(e.getMissingJobSpecURI(), jsURI);
    }

    cachedCat.removeListener(l);
    cachedCat.put(js1_3);
    res = cachedCat.getJobSpec(jsURI);
    Assert.assertEquals(res, js1_3);
    res = baseCat.getJobSpec(jsURI);
    Assert.assertEquals(res, js1_3);

    cachedCat.remove(jsURI);
    try {
      cachedCat.getJobSpec(jsURI);
      Assert.fail("Expected JobSpecNotFoundException");
    } catch (JobSpecNotFoundException e) {
      Assert.assertEquals(e.getMissingJobSpecURI(), jsURI);
    }
    try {
      baseCat.getJobSpec(jsURI);
      Assert.fail("Expected JobSpecNotFoundException");
    } catch (JobSpecNotFoundException e) {
      Assert.assertEquals(e.getMissingJobSpecURI(), jsURI);
    }

    Mockito.verify(l).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));
    Mockito.verify(l).onDeleteJob(Mockito.eq(js1_2));

    Mockito.verifyNoMoreInteractions(l);
  }

}
