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
package org.apache.gobblin.runtime.api;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.util.ConfigUtils;


public class FsSpecProducerTest {
  private FsSpecProducer _fsSpecProducer;
  private FsSpecConsumer _fsSpecConsumer;
  private Config _config;
  private File workDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    this.workDir = Files.createTempDir();
    this.workDir.deleteOnExit();
    Config config = ConfigFactory.empty().withValue(FsSpecConsumer.SPEC_PATH_KEY, ConfigValueFactory.fromAnyRef(
        this.workDir.getAbsolutePath()));
    this._fsSpecProducer = new FsSpecProducer(config);
    this._fsSpecConsumer = new FsSpecConsumer(config);
    this._config = config;
  }

  private JobSpec createTestJobSpec() throws URISyntaxException {
    return createTestJobSpec("testJob");
  }

  private JobSpec createTestJobSpec(String jobSpecUri) throws URISyntaxException {
    Properties properties = new Properties();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    //Introduce a key which is a prefix of another key and ensure it is correctly handled in the code
    properties.put("key3.1", "val3");
    properties.put("key3.1.1", "val4");

    JobSpec jobSpec = JobSpec.builder(jobSpecUri)
        .withConfig(ConfigUtils.propertiesToConfig(properties))
        .withVersion("1")
        .withDescription("")
        .withTemplate(new URI("FS:///")).build();
    return jobSpec;
  }

  @Test
  public void testAddSpec()
      throws URISyntaxException, ExecutionException, InterruptedException, IOException {
    this._fsSpecProducer.addSpec(createTestJobSpec());

    // Add some random files(with non-avro extension name) into the folder observed by consumer, they shall not be picked up.
    File randomFile = new File(workDir, "random");
    Assert.assertTrue(randomFile.createNewFile());
    randomFile.deleteOnExit();

    List<Pair<SpecExecutor.Verb, Spec>> jobSpecs = this._fsSpecConsumer.changedSpecs().get();
    Assert.assertEquals(jobSpecs.size(), 1);
    Assert.assertEquals(jobSpecs.get(0).getLeft(), SpecExecutor.Verb.ADD);
    Assert.assertEquals(jobSpecs.get(0).getRight().getUri().toString(), "testJob");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key1"), "val1");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key2"), "val2");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key3.1" + ConfigUtils.STRIP_SUFFIX), "val3");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key3.1.1"), "val4");
    jobSpecs.clear();

    // If there are other jobSpec in .avro files added by testSpecProducer, they shall still be found.
    this._fsSpecProducer.addSpec(createTestJobSpec("newTestJob"));
    jobSpecs = this._fsSpecConsumer.changedSpecs().get();
    Assert.assertEquals(jobSpecs.size(), 2);
    Assert.assertEquals(jobSpecs.get(0).getLeft(), SpecExecutor.Verb.ADD);
    Assert.assertEquals(jobSpecs.get(1).getLeft(), SpecExecutor.Verb.ADD);
    List<String> uriList = jobSpecs.stream().map(s -> s.getRight().getUri().toString()).collect(Collectors.toList());
    Assert.assertTrue(uriList.contains( "testJob"));
    Assert.assertTrue(uriList.contains( "newTestJob"));
  }

  @Test (dependsOnMethods = "testAddSpec")
  public void testUpdateSpec() throws ExecutionException, InterruptedException, URISyntaxException {
    this._fsSpecProducer.updateSpec(createTestJobSpec());

    List<Pair<SpecExecutor.Verb, Spec>> jobSpecs = this._fsSpecConsumer.changedSpecs().get();
    Assert.assertEquals(jobSpecs.size(), 1);
    Assert.assertEquals(jobSpecs.get(0).getLeft(), SpecExecutor.Verb.UPDATE);
    Assert.assertEquals(jobSpecs.get(0).getRight().getUri().toString(), "testJob");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key1"), "val1");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key2"), "val2");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key3.1" + ConfigUtils.STRIP_SUFFIX), "val3");
    Assert.assertEquals(((JobSpec) jobSpecs.get(0).getRight()).getConfig().getString("key3.1.1"), "val4");
  }

  @Test (dependsOnMethods = "testUpdateSpec")
  public void testDeleteSpec() throws URISyntaxException, ExecutionException, InterruptedException {
    Properties headers = new Properties();
    headers.put("headerProp1", "headerValue1");
    this._fsSpecProducer.deleteSpec(new URI("testDeleteJob"), headers);
    List<Pair<SpecExecutor.Verb, Spec>> jobSpecs = this._fsSpecConsumer.changedSpecs().get();
    Assert.assertEquals(jobSpecs.size(), 1);
    Assert.assertEquals(jobSpecs.get(0).getLeft(), SpecExecutor.Verb.DELETE);
    Assert.assertEquals(jobSpecs.get(0).getRight().getUri().toString(), "testDeleteJob");
  }
}