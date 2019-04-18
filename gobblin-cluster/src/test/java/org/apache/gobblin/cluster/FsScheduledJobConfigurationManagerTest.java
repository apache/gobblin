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
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.job_catalog.NonObservingFSJobCatalog;
import org.apache.gobblin.runtime.job_spec.AvroJobSpec;

@Slf4j
public class FsScheduledJobConfigurationManagerTest {
  private MutableJobCatalog _jobCatalog;
  private FsScheduledJobConfigurationManager jobConfigurationManager;

  private String jobConfDir = "/tmp/" + this.getClass().getSimpleName() + "/jobCatalog";
  private String fsSpecConsumerPathString = "/tmp/fsJobConfigManagerTest";
  private String jobSpecUriString = "testJobSpec";

  private FileSystem fs;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration(false));
    Path jobConfDirPath = new Path(jobConfDir);
    if (!this.fs.exists(jobConfDirPath)) {
      this.fs.mkdirs(jobConfDirPath);
    }

    EventBus eventBus = new EventBus(FsScheduledJobConfigurationManagerTest.class.getSimpleName());
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, ConfigValueFactory.fromAnyRef(jobConfDir));
    this._jobCatalog = new NonObservingFSJobCatalog(config);
    ((NonObservingFSJobCatalog) this._jobCatalog).startAsync().awaitRunning();

    Config jobConfigurationManagerConfig = ConfigFactory.empty()
        .withValue(GobblinClusterConfigurationKeys.SPEC_CONSUMER_CLASS_KEY, ConfigValueFactory.fromAnyRef(FsSpecConsumer.class.getName()))
        .withValue(FsSpecConsumer.SPEC_PATH_KEY, ConfigValueFactory.fromAnyRef(fsSpecConsumerPathString));
    jobConfigurationManager = new FsScheduledJobConfigurationManager(eventBus, jobConfigurationManagerConfig, this._jobCatalog);
  }

  private void addJobSpec(String jobSpecName, String version, String verb) throws IOException {
    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put(FsSpecConsumer.VERB_KEY, verb);

    AvroJobSpec jobSpec = AvroJobSpec.newBuilder().
        setUri(Files.getNameWithoutExtension(jobSpecName)).
        setProperties(new HashMap<>()).
        setTemplateUri("FS:///").
        setDescription("test").
        setVersion(version).
        setMetadata(metadataMap).build();

    DatumWriter<AvroJobSpec> datumWriter = new SpecificDatumWriter<>(AvroJobSpec.SCHEMA$);
    DataFileWriter<AvroJobSpec> dataFileWriter = new DataFileWriter<>(datumWriter);

    Path fsSpecConsumerPath = new Path(fsSpecConsumerPathString, jobSpecName);
    FileSystem fs = fsSpecConsumerPath.getFileSystem(new Configuration());
    OutputStream out = fs.create(fsSpecConsumerPath);

    dataFileWriter.create(AvroJobSpec.SCHEMA$, out);
    dataFileWriter.append(jobSpec);
    dataFileWriter.close();
  }

  @Test (expectedExceptions = {JobSpecNotFoundException.class})
  public void testFetchJobSpecs() throws ExecutionException, InterruptedException, URISyntaxException, JobSpecNotFoundException, IOException {
    //Test adding a JobSpec
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
    Assert.assertEquals(this.fs.listStatus(fsSpecConsumerPath).length, 0);

    //Test that the updated JobSpec has been added to the JobCatalog.
    String verb2 = SpecExecutor.Verb.UPDATE.name();
    String version2 = "2";
    addJobSpec(jobSpecUriString, version2, verb2);
    this.jobConfigurationManager.fetchJobSpecs();
    jobSpec = this._jobCatalog.getJobSpec(new URI(jobSpecUriString));
    Assert.assertTrue(jobSpec != null);
    Assert.assertTrue(jobSpec.getVersion().equals(version2));
    //Ensure the JobSpec is deleted from the FsSpecConsumer path.
    Assert.assertEquals(this.fs.listStatus(fsSpecConsumerPath).length, 0);

    //Test that the JobSpec has been deleted from the JobCatalog.
    String verb3 = SpecExecutor.Verb.DELETE.name();
    addJobSpec(jobSpecUriString, version2, verb3);
    this.jobConfigurationManager.fetchJobSpecs();
    Assert.assertEquals(this.fs.listStatus(fsSpecConsumerPath).length, 0);
    this._jobCatalog.getJobSpec(new URI(jobSpecUriString));
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