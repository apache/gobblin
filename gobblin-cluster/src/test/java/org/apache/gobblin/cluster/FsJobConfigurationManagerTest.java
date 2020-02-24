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
package org.apache.gobblin.cluster;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.event.DeleteJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.NewJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.UpdateJobConfigArrivalEvent;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.FsSpecProducer;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.job_catalog.NonObservingFSJobCatalog;
import org.apache.gobblin.util.filters.HiddenFilter;


@Slf4j
public class FsJobConfigurationManagerTest {
  private MutableJobCatalog _jobCatalog;
  private FsJobConfigurationManager jobConfigurationManager;

  private String jobConfDir = "/tmp/" + this.getClass().getSimpleName() + "/jobCatalog";
  private String fsSpecConsumerPathString = "/tmp/fsJobConfigManagerTest";
  private String jobSpecUriString = "testJobSpec";

  private FileSystem fs;
  private SpecProducer _specProducer;

  private int newJobConfigArrivalEventCount = 0;
  private int updateJobConfigArrivalEventCount = 0;
  private int deleteJobConfigArrivalEventCount = 0;

  // An EventBus used for communications between services running in the ApplicationMaster
  private EventBus eventBus;

  @BeforeClass
  public void setUp() throws IOException {
    this.eventBus = Mockito.mock(EventBus.class);
    Mockito.doAnswer(invocationOnMock -> {
      Object argument = invocationOnMock.getArguments()[0];

      if (argument instanceof NewJobConfigArrivalEvent) {
        newJobConfigArrivalEventCount++;
      } else if (argument instanceof DeleteJobConfigArrivalEvent) {
        deleteJobConfigArrivalEventCount++;
      } else if (argument instanceof UpdateJobConfigArrivalEvent) {
        updateJobConfigArrivalEventCount++;
      } else {
        throw new IOException("Unexpected event type");
      }
      return null;
    }).when(this.eventBus).post(Mockito.anyObject());

    this.fs = FileSystem.getLocal(new Configuration(false));
    Path jobConfDirPath = new Path(jobConfDir);
    if (!this.fs.exists(jobConfDirPath)) {
      this.fs.mkdirs(jobConfDirPath);
    }

    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, ConfigValueFactory.fromAnyRef(jobConfDir))
        .withValue(FsSpecConsumer.SPEC_PATH_KEY, ConfigValueFactory.fromAnyRef(fsSpecConsumerPathString))
        .withValue(GobblinClusterConfigurationKeys.JOB_SPEC_REFRESH_INTERVAL, ConfigValueFactory.fromAnyRef(1));

    this._jobCatalog = new NonObservingFSJobCatalog(config);
    ((NonObservingFSJobCatalog) this._jobCatalog).startAsync().awaitRunning();

    jobConfigurationManager = new FsJobConfigurationManager(eventBus, config, this._jobCatalog, this.fs);

    _specProducer = new FsSpecProducer(this.fs, config);
  }

  private void addJobSpec(String jobSpecName, String version, String verb)
      throws URISyntaxException, IOException {
    JobSpec jobSpec =
        JobSpec.builder(new URI(Files.getNameWithoutExtension(jobSpecName)))
            .withConfig(ConfigFactory.empty())
            .withTemplate(new URI("FS:///"))
            .withVersion(version)
            .withDescription("test")
            .build();

    SpecExecutor.Verb enumVerb = SpecExecutor.Verb.valueOf(verb);

    switch (enumVerb) {
      case ADD:
        _specProducer.addSpec(jobSpec);
        break;
      case DELETE:
        _specProducer.deleteSpec(jobSpec.getUri());
        break;
      case UPDATE:
        _specProducer.updateSpec(jobSpec);
        break;
      default:
        throw new IOException("Unknown Spec Verb: " + verb);
    }
  }

  @Test (expectedExceptions = {JobSpecNotFoundException.class})
  public void testFetchJobSpecs() throws ExecutionException, InterruptedException, URISyntaxException, JobSpecNotFoundException, IOException {
    //Ensure JobSpec is added to JobCatalog
    String verb1 = SpecExecutor.Verb.ADD.name();
    String version1 = "1";
    addJobSpec(jobSpecUriString, version1, verb1);
    this.jobConfigurationManager.fetchJobSpecs();
    JobSpec jobSpec = this._jobCatalog.getJobSpec(new URI(jobSpecUriString));
    Assert.assertTrue(jobSpec != null);
    Assert.assertTrue(jobSpec.getVersion().equals(version1));
    Assert.assertTrue(jobSpec.getUri().getPath().equals(jobSpecUriString));

    //Ensure the JobSpec is deleted from the FsSpecConsumer path.
    Path fsSpecConsumerPath = new Path(fsSpecConsumerPathString);
    Assert.assertEquals(this.fs.listStatus(fsSpecConsumerPath, new HiddenFilter()).length, 0);

    //Ensure NewJobConfigArrivalEvent is posted to EventBus
    Assert.assertEquals(newJobConfigArrivalEventCount, 1);
    Assert.assertEquals(updateJobConfigArrivalEventCount, 0);
    Assert.assertEquals(deleteJobConfigArrivalEventCount, 0);

    //Test that the updated JobSpec has been added to the JobCatalog.
    String verb2 = SpecExecutor.Verb.UPDATE.name();
    String version2 = "2";
    addJobSpec(jobSpecUriString, version2, verb2);
    this.jobConfigurationManager.fetchJobSpecs();
    jobSpec = this._jobCatalog.getJobSpec(new URI(jobSpecUriString));
    Assert.assertTrue(jobSpec != null);
    Assert.assertTrue(jobSpec.getVersion().equals(version2));

    //Ensure the updated JobSpec is deleted from the FsSpecConsumer path.
    Assert.assertEquals(this.fs.listStatus(fsSpecConsumerPath, new HiddenFilter()).length, 0);

    //Ensure UpdateJobConfigArrivalEvent is posted to EventBus
    Assert.assertEquals(newJobConfigArrivalEventCount, 1);
    Assert.assertEquals(updateJobConfigArrivalEventCount, 1);
    Assert.assertEquals(deleteJobConfigArrivalEventCount, 0);

    //Test that the JobSpec has been deleted from the JobCatalog.
    String verb3 = SpecExecutor.Verb.DELETE.name();
    addJobSpec(jobSpecUriString, version2, verb3);
    this.jobConfigurationManager.fetchJobSpecs();

    //Ensure the JobSpec is deleted from the FsSpecConsumer path.
    Assert.assertEquals(this.fs.listStatus(fsSpecConsumerPath, new HiddenFilter()).length, 0);
    this._jobCatalog.getJobSpec(new URI(jobSpecUriString));

    //Ensure DeleteJobConfigArrivalEvent is posted to EventBus
    Assert.assertEquals(newJobConfigArrivalEventCount, 1);
    Assert.assertEquals(updateJobConfigArrivalEventCount, 1);
    Assert.assertEquals(deleteJobConfigArrivalEventCount, 1);
  }

  @Test
  public void testException()
      throws Exception {
    FsJobConfigurationManager jobConfigurationManager = Mockito.spy(this.jobConfigurationManager);
    Mockito.doThrow(new ExecutionException(new IOException("Test exception"))).when(jobConfigurationManager).fetchJobSpecs();

    jobConfigurationManager.startUp();

    //Add wait to ensure that fetchJobSpecExecutor thread is scheduled at least once.
    Thread.sleep(2000);
    int numInvocations = Mockito.mockingDetails(jobConfigurationManager).getInvocations().size();
    Mockito.verify(jobConfigurationManager, Mockito.atLeast(1)).fetchJobSpecs();

    Thread.sleep(2000);
    //Verify that there new invocations of fetchJobSpecs()
    Mockito.verify(jobConfigurationManager, Mockito.atLeast(numInvocations + 1)).fetchJobSpecs();
    //Ensure that the JobConfigurationManager Service is running.
    Assert.assertTrue(!jobConfigurationManager.state().equals(Service.State.FAILED) && !jobConfigurationManager.state().equals(Service.State.TERMINATED));
  }

  @AfterClass
  public void tearDown() throws IOException {
    Path fsSpecConsumerPath = new Path(fsSpecConsumerPathString);
    if (fs.exists(fsSpecConsumerPath)) {
      fs.delete(fsSpecConsumerPath, true);
    }
    Path jobCatalogPath = new Path(jobConfDir);
    if (fs.exists(jobCatalogPath)) {
      fs.delete(jobCatalogPath, true);
    }
  }
}