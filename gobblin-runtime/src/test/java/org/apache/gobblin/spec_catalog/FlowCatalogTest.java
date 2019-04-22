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

package org.apache.gobblin.spec_catalog;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FlowCatalogTest {
  private static final Logger logger = LoggerFactory.getLogger(FlowCatalog.class);
  private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private static final String SPEC_STORE_PARENT_DIR = "/tmp";
  private static final String SPEC_STORE_DIR = "/tmp/flowTestSpecStore";
  private static final String SPEC_GROUP_DIR = "/tmp/flowTestSpecStore/flowTestGroupDir";
  private static final String SPEC_DESCRIPTION = "Test Flow Spec";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;

  private ServiceBasedAppLauncher serviceLauncher;
  private FlowCatalog flowCatalog;
  private FlowSpec flowSpec;

  @BeforeClass
  public void setup() throws Exception {
    File specStoreDir = new File(SPEC_STORE_DIR);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }

    Properties properties = new Properties();
    properties.put("specStore.fs.dir", SPEC_STORE_DIR);

    this.serviceLauncher = new ServiceBasedAppLauncher(properties, "FlowCatalogTest");

    this.flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(properties),
        Optional.of(logger));
    this.serviceLauncher.addService(flowCatalog);

    // Start Catalog
    this.serviceLauncher.start();

    // Create Spec to play with
    this.flowSpec = initFlowSpec();
  }

  private FlowSpec initFlowSpec() {
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", SPEC_STORE_DIR);
    properties.put("specExecInstance.capabilities", "source:destination");
    Config config = ConfigUtils.propertiesToConfig(properties);

    SpecExecutor specExecutorInstanceProducer = new InMemorySpecExecutor(config);

    FlowSpec.Builder flowSpecBuilder = null;
    try {
      flowSpecBuilder = FlowSpec.builder(computeFlowSpecURI())
          .withConfig(config)
          .withDescription(SPEC_DESCRIPTION)
          .withVersion(SPEC_VERSION)
          .withTemplate(new URI("templateURI"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return flowSpecBuilder.build();
  }

  @AfterClass
  public void cleanUp() throws Exception {
    // Shutdown Catalog
    this.serviceLauncher.stop();

    File specStoreDir = new File(SPEC_STORE_DIR);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  /**
   * Make sure that when there's on spec failed to be deserialized, the rest of spec in specStore can
   * still be taken care of.
   */
  @Test
  public void testGetSpecRobustness() throws Exception {

    File specDir = Files.createTempDir();
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.SPECSTORE_FS_DIR_KEY, specDir.getAbsolutePath());
    SpecSerDe serde = Mockito.mock(SpecSerDe.class);
    TestFsSpecStore fsSpecStore = new TestFsSpecStore(ConfigUtils.propertiesToConfig(properties), serde);

    // Version is specified as 0,1,2
    File specFileFail = new File(specDir, "spec_fail");
    Assert.assertTrue(specFileFail.createNewFile());
    File specFile1 = new File(specDir, "spec0");
    Assert.assertTrue(specFile1.createNewFile());
    File specFile2 = new File(specDir, "spec1");
    Assert.assertTrue(specFile2.createNewFile());
    File specFile3 = new File(specDir, "serDeFail");
    Assert.assertTrue(specFile3.createNewFile());

    FileSystem fs = FileSystem.getLocal(new Configuration());
    Assert.assertEquals(fs.getFileStatus(new Path(specFile3.getAbsolutePath())).getLen(), 0);

    Collection<Spec> specList = fsSpecStore.getSpecs();
    // The fail and serDe datasets wouldn't survive
    Assert.assertEquals(specList.size(), 2);
    for (Spec spec: specList) {
      Assert.assertTrue(!spec.getDescription().contains("spec_fail"));
      Assert.assertTrue(!spec.getDescription().contains("serDeFail"));
    }
  }

  class TestFsSpecStore extends FSSpecStore {
    public TestFsSpecStore(Config sysConfig, SpecSerDe specSerDe) throws IOException {
      super(sysConfig, specSerDe);
    }

    @Override
    protected Spec readSpecFromFile(Path path) throws IOException {
      if (path.getName().contains("fail")) {
        throw new IOException("Mean to fail in the test");
      } else if (path.getName().contains("serDeFail")) {

        // Simulate the way that a serDe exception
        FSDataInputStream fis = fs.open(path);
        SerializationUtils.deserialize(ByteStreams.toByteArray(fis));

        // This line should never be reached since we generate SerDe Exception on purpose.
        Assert.assertTrue(false);
        return null;
      }
      else return initFlowSpec();
    }
  }

  @Test
  public void createFlowSpec() {
    // List Current Specs
    Collection<Spec> specs = flowCatalog.getSpecs();
    logger.info("[Before Create] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      FlowSpec flowSpec = (FlowSpec) spec;
      logger.info("[Before Create] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    Assert.assertTrue(specs.size() == 0, "Spec store should be empty before addition");

    // Create and add Spec
    this.flowCatalog.put(flowSpec);

    // List Specs after adding
    specs = flowCatalog.getSpecs();
    logger.info("[After Create] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      flowSpec = (FlowSpec) spec;
      logger.info("[After Create] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    Assert.assertTrue(specs.size() == 1, "Spec store should contain 1 Spec after addition");
  }

  @Test (dependsOnMethods = "createFlowSpec")
  void testExist() throws Exception {
    Assert.assertTrue(flowCatalog.exists(flowSpec.getUri()));
  }

  @Test (dependsOnMethods = "testExist")
  public void deleteFlowSpec() throws SpecNotFoundException {
    // List Current Specs
    Collection<Spec> specs = flowCatalog.getSpecs();
    logger.info("[Before Delete] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      FlowSpec flowSpec = (FlowSpec) spec;
      logger.info("[Before Delete] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    Assert.assertTrue(specs.size() == 1, "Spec store should initially have 1 Spec before deletion");

    this.flowCatalog.remove(flowSpec.getUri());

    // List Specs after adding
    specs = flowCatalog.getSpecs();
    logger.info("[After Delete] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      flowSpec = (FlowSpec) spec;
      logger.info("[After Delete] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    Assert.assertTrue(specs.size() == 0, "Spec store should be empty after deletion");
  }

  public URI computeFlowSpecURI() {
    // Make sure this is relative
    URI uri = PathUtils.relativizePath(new Path(SPEC_GROUP_DIR), new Path(SPEC_STORE_PARENT_DIR)).toUri();
    return uri;
  }
}