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

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.runtime.job_spec.ResolvedJobSpec;
import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobCatalogListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;

import org.testng.Assert;


/**
 * Test interaction between (Mutable)FsJobCatalog and its listeners.
 * Inherit the testing routine for InMemoryJobCatalog.
 */
public class TestFSJobCatalog {

  private File jobConfigDir;
  private Path jobConfigDirPath;

  @Test
  public void testCallbacks()
      throws Exception {
    this.jobConfigDir = java.nio.file.Files.createTempDirectory(
        String.format("gobblin-test_%s_job-conf", this.getClass().getSimpleName())).toFile();
    this.jobConfigDirPath = new Path(this.jobConfigDir.getPath());

    try (PrintWriter printWriter = new PrintWriter(new Path(jobConfigDirPath, "job3.template").toString(), "UTF-8")) {
      printWriter.println("param1 = value1");
      printWriter.println("param2 = value2");
    }

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, this.jobConfigDir.getPath());
    PathAlterationObserver observer = new PathAlterationObserver(this.jobConfigDirPath);

    /* Exposed the observer so that checkAndNotify can be manually invoked. */
    FSJobCatalog cat = new FSJobCatalog(ConfigUtils.propertiesToConfig(properties), observer);
    cat.startAsync();
    cat.awaitRunning(10, TimeUnit.SECONDS);

    final Map<URI, JobSpec> specs = new Hashtable<>();

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation)
          throws Throwable {
        JobSpec spec = (JobSpec) invocation.getArguments()[0];
        specs.put(spec.getUri(), spec);
        return null;
      }
    }).when(l).onAddJob(Mockito.any(JobSpec.class));
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation)
          throws Throwable {
        JobSpec spec = (JobSpec) invocation.getArguments()[0];
        specs.put(spec.getUri(), spec);
        return null;
      }
    }).when(l).onUpdateJob(Mockito.any(JobSpec.class));

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation)
          throws Throwable {
        URI uri = (URI) invocation.getArguments()[0];
        specs.remove(uri);
        return null;
      }
    }).when(l).onDeleteJob(Mockito.any(URI.class), Mockito.anyString());

    JobSpec js1_1 = JobSpec.builder("test_job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test_job1").withVersion("2").build();
    JobSpec js2 = JobSpec.builder("test_job2").withVersion("1").build();
    JobSpec js3 = JobSpec.builder("test_job3").withVersion("1").withTemplate(new URI("FS:///job3.template"))
    .withConfig(ConfigBuilder.create().addPrimitive("job.template", "FS:///job3.template").build()).build();

    cat.addListener(l);
    observer.initialize();

    cat.put(js1_1);
    // enough time for file creation.
    observer.checkAndNotify();
    Assert.assertTrue(specs.containsKey(js1_1.getUri()));
    JobSpec js1_1_notified = specs.get(js1_1.getUri());
    Assert.assertTrue(ConfigUtils.verifySubset(js1_1_notified.getConfig(), js1_1.getConfig()));
    Assert.assertEquals(js1_1.getVersion(), js1_1_notified.getVersion());

    // Linux system has too large granularity for the modification time.
    Thread.sleep(1000);

    cat.put(js1_2);
    // enough time for file replacement.
    observer.checkAndNotify();
    Assert.assertTrue(specs.containsKey(js1_2.getUri()));
    JobSpec js1_2_notified = specs.get(js1_2.getUri());
    Assert.assertTrue(ConfigUtils.verifySubset(js1_2_notified.getConfig(), js1_2.getConfig()));
    Assert.assertEquals(js1_2.getVersion(), js1_2_notified.getVersion());

    Thread.sleep(1000);
    cat.put(js2);
    observer.checkAndNotify();
    Assert.assertTrue(specs.containsKey(js2.getUri()));
    JobSpec js2_notified = specs.get(js2.getUri());
    Assert.assertTrue(ConfigUtils.verifySubset(js2_notified.getConfig(), js2.getConfig()));
    Assert.assertEquals(js2.getVersion(), js2_notified.getVersion());

    Thread.sleep(1000);
    cat.remove(js2.getUri());

    // enough time for file deletion.
    observer.checkAndNotify();
    Assert.assertFalse(specs.containsKey(js2.getUri()));

    Thread.sleep(1000);
    cat.put(js3);
    observer.checkAndNotify();
    Assert.assertTrue(specs.containsKey(js3.getUri()));
    JobSpec js3_notified = specs.get(js3.getUri());
    Assert.assertTrue(ConfigUtils.verifySubset(js3_notified.getConfig(), js3.getConfig()));
    Assert.assertEquals(js3.getVersion(), js3_notified.getVersion());
    ResolvedJobSpec js3_resolved = new ResolvedJobSpec(js3_notified, cat);
    Assert.assertEquals(js3_resolved.getConfig().getString("param1"), "value1");
    Assert.assertEquals(js3_resolved.getConfig().getString("param2"), "value2");

    cat.stopAsync();
    cat.awaitTerminated(10, TimeUnit.SECONDS);
  }
}
