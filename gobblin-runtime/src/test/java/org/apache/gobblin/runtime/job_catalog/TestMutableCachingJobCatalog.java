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

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import org.apache.gobblin.runtime.api.JobCatalogListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;

/** Unit tests for {@link CachingJobCatalog} and {@link MutableCachingJobCatalog} */
public class TestMutableCachingJobCatalog {

  @Test
  public void test() throws Exception {
    InMemoryJobCatalog baseCat =
        new InMemoryJobCatalog(Optional.<Logger>of(LoggerFactory.getLogger("baseCat")));
    baseCat.startAsync();
    baseCat.awaitRunning(2, TimeUnit.SECONDS);
    MutableCachingJobCatalog cachedCat =
        new MutableCachingJobCatalog(baseCat, Optional.<Logger>of(LoggerFactory.getLogger("cachedCat")));

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);
    cachedCat.addListener(l);

    cachedCat.startAsync();
    cachedCat.awaitRunning(10, TimeUnit.SECONDS);

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
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_2));
    Mockito.verify(l).onDeleteJob(Mockito.eq(js1_2.getUri()), Mockito.eq(js1_2.getVersion()));

    Mockito.verifyNoMoreInteractions(l);

    cachedCat.stopAsync();
    cachedCat.awaitTerminated(10, TimeUnit.SECONDS);
    baseCat.stopAsync();
    baseCat.awaitTerminated(2, TimeUnit.SECONDS);
  }

}
