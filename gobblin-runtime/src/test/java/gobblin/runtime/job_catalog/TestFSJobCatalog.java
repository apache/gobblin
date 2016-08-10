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

import gobblin.util.filesystem.PathAlterationObserver;
import java.io.File;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;

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

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, this.jobConfigDir.getPath());
    PathAlterationObserver observer = new PathAlterationObserver(this.jobConfigDirPath);

    /* Exposed the observer so that checkAndNotify can be manually invoked. */
    FSJobCatalog cat = new FSJobCatalog(properties, observer);

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);


    JobSpec js1_1 = JobSpec.builder("test_job1.pull").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test_job1.pull").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builder("test_job1.pull").withVersion("3").build();
    JobSpec js2 = JobSpec.builder("test_job2.pull").withVersion("1").build();

    cat.addListener(l);
    observer.initialize();

    cat.put(js1_1);
    // enough time for file creation.
    Thread.sleep(1000);
    observer.checkAndNotify("1");
    Mockito.verify(l).onAddJob(Mockito.eq(js1_1));


    cat.put(js1_2);
    // enough time for file replacement.
    Thread.sleep(1000);
    observer.checkAndNotify("2");
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));

    cat.put(js2);
    // enough time for file creation.
    Thread.sleep(1000);
    observer.checkAndNotify("3");
    Mockito.verify(l).onAddJob(Mockito.eq(js2));

    cat.put(js1_3);
    // enough time for file replacement.
    Thread.sleep(1000);
    observer.checkAndNotify("4");
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_2), Mockito.eq(js1_3));

    cat.remove(js2.getUri());
    // enough time for file deletion.
    Thread.sleep(1000);
    observer.checkAndNotify("5");
    Mockito.verify(l).onDeleteJob(Mockito.eq(js2));

    cat.removeListener(l);
    cat.remove(js1_3.getUri());
    observer.checkAndNotify("6");
    Mockito.verifyNoMoreInteractions(l);
  }

}
