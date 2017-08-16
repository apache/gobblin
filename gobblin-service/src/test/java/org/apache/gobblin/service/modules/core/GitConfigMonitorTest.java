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

package org.apache.gobblin.service.modules.core;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.util.FS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;

public class GitConfigMonitorTest {
  private static final Logger logger = LoggerFactory.getLogger(GitConfigMonitorTest.class);
  private Repository remoteRepo;
  private Git gitForPush;
  private static final String TEST_DIR = "/tmp/gitConfigTestDir/";
  private final File remoteDir = new File(TEST_DIR + "/remote");
  private final File cloneDir = new File(TEST_DIR + "/clone");
  private final File configDir = new File(cloneDir, "/gobblin-config");
  private static final String TEST_FLOW_FILE = "testFlow.pull";
  private static final String TEST_FLOW_FILE2 = "testFlow2.pull";
  private static final String TEST_FLOW_FILE3 = "testFlow3.pull";
  private final File testGroupDir = new File(configDir, "testGroup");
  private final File testFlowFile = new File(testGroupDir, TEST_FLOW_FILE);
  private final File testFlowFile2 = new File(testGroupDir, TEST_FLOW_FILE2);
  private final File testFlowFile3 = new File(testGroupDir, TEST_FLOW_FILE3);

  private RefSpec masterRefSpec = new RefSpec("master");
  private FlowCatalog flowCatalog;
  private Config config;
  private GitConfigMonitor gitConfigMonitor;


  @BeforeClass
  public void setup() throws Exception {
    cleanUpDir(TEST_DIR);

    // Create a bare repository
    RepositoryCache.FileKey fileKey = RepositoryCache.FileKey.exact(remoteDir, FS.DETECTED);
    this.remoteRepo = fileKey.open(false);
    this.remoteRepo.create(true);

    this.gitForPush = Git.cloneRepository().setURI(this.remoteRepo.getDirectory().getAbsolutePath()).setDirectory(cloneDir).call();

    // push an empty commit as a base for detecting changes
    this.gitForPush.commit().setMessage("First commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.GIT_CONFIG_MONITOR_REPO_URI, this.remoteRepo.getDirectory().getAbsolutePath())
        .addPrimitive(ConfigurationKeys.GIT_CONFIG_MONITOR_REPO_DIR, TEST_DIR + "/jobConfig")
        .addPrimitive(ConfigurationKeys.FLOWSPEC_STORE_DIR_KEY, TEST_DIR + "flowCatalog")
        .addPrimitive(ConfigurationKeys.GIT_CONFIG_MONITOR_POLLING_INTERVAL, 5)
        .build();

    this.flowCatalog = new FlowCatalog(config);
    this.flowCatalog.startAsync().awaitRunning();
    this.gitConfigMonitor = new GitConfigMonitor(this.config, this.flowCatalog);
    this.gitConfigMonitor.setActive(true);
  }

  private void cleanUpDir(String dir) {
    File specStoreDir = new File(dir);

    // cleanup is flaky on Travis, so retry a few times and then suppress the error if unsuccessful
    for (int i = 0; i < 5; i++) {
      try {
        if (specStoreDir.exists()) {
          FileUtils.deleteDirectory(specStoreDir);
        }
        // if delete succeeded then break out of loop
        break;
      } catch (IOException e) {
        logger.warn("Cleanup delete directory failed for directory: " + dir, e);
      }
    }
  }

  @AfterClass
  public void cleanUp() {
    if (this.flowCatalog != null) {
      this.flowCatalog.stopAsync().awaitTerminated();
    }

    cleanUpDir(TEST_DIR);
  }

  private String formConfigFilePath(String groupDir, String fileName) {
    return this.configDir.getName() + SystemUtils.FILE_SEPARATOR + groupDir + SystemUtils.FILE_SEPARATOR + fileName;
  }

  @Test
  public void testAddConfig() throws IOException, GitAPIException, URISyntaxException {
    // push a new config file
    this.testGroupDir.mkdirs();
    this.testFlowFile.createNewFile();
    Files.write("flow.name=testFlow\nflow.group=testGroup\nparam1=value1\n", testFlowFile, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile.getName()))
        .call();
    this.gitForPush.commit().setMessage("Second commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitConfigMonitor.processGitConfigChanges();

    Collection<Spec> specs = this.flowCatalog.getSpecs();

    Assert.assertTrue(specs.size() == 1);
    FlowSpec spec = (FlowSpec)(specs.iterator().next());
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value1");
  }

  @Test(dependsOnMethods = "testAddConfig")
  public void testUpdateConfig() throws IOException, GitAPIException, URISyntaxException {
    // push an updated config file
    Files.write("flow.name=testFlow\nflow.group=testGroup\nparam1=value2\n", testFlowFile, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile.getName()))
        .call();
    this.gitForPush.commit().setMessage("Third commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitConfigMonitor.processGitConfigChanges();

    Collection<Spec> specs = this.flowCatalog.getSpecs();

    Assert.assertTrue(specs.size() == 1);
    FlowSpec spec = (FlowSpec)(specs.iterator().next());
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value2");
  }

  @Test(dependsOnMethods = "testUpdateConfig")
  public void testDeleteConfig() throws IOException, GitAPIException, URISyntaxException {
    // delete a config file
    testFlowFile.delete();

    // flow catalog has 1 entry before the config is deleted
    Collection<Spec> specs = this.flowCatalog.getSpecs();
    Assert.assertTrue(specs.size() == 1);

    // add, commit, push
    DirCache ac = this.gitForPush.rm().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile.getName()))
        .call();
    RevCommit cc = this.gitForPush.commit().setMessage("Fourth commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitConfigMonitor.processGitConfigChanges();

    specs = this.flowCatalog.getSpecs();
    Assert.assertTrue(specs.size() == 0);
  }

  @Test(dependsOnMethods = "testDeleteConfig")
  public void testForcedPushConfig() throws IOException, GitAPIException, URISyntaxException {
    // push a new config file
    this.testGroupDir.mkdirs();
    this.testFlowFile.createNewFile();
    Files.write("flow.name=testFlow\nflow.group=testGroup\nparam1=value1\n", testFlowFile, Charsets.UTF_8);
    this.testFlowFile2.createNewFile();
    Files.write("flow.name=testFlow2\nflow.group=testGroup\nparam1=value2\n", testFlowFile2, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile.getName()))
        .call();
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile2.getName()))
        .call();
    this.gitForPush.commit().setMessage("Fifth commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitConfigMonitor.processGitConfigChanges();

    Collection<Spec> specs = this.flowCatalog.getSpecs();

    Assert.assertTrue(specs.size() == 2);
    List<Spec> specList = Lists.newArrayList(specs);
    specList.sort(new Comparator<Spec>() {
      @Override
      public int compare(Spec o1, Spec o2) {
        return o1.getUri().compareTo(o2.getUri());
      }
    });

    FlowSpec spec = (FlowSpec)specList.get(0);
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value1");

    spec = (FlowSpec)specList.get(1);
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow2"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow2");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value2");

    // go back in time to cause conflict
    this.gitForPush.reset().setMode(ResetCommand.ResetType.HARD).setRef("HEAD~1").call();
    this.gitForPush.push().setForce(true).setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    // add new files
    this.testGroupDir.mkdirs();
    this.testFlowFile2.createNewFile();
    Files.write("flow.name=testFlow2\nflow.group=testGroup\nparam1=value4\n", testFlowFile2, Charsets.UTF_8);
    this.testFlowFile3.createNewFile();
    Files.write("flow.name=testFlow3\nflow.group=testGroup\nparam1=value5\n", testFlowFile3, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile2.getName()))
        .call();
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile3.getName()))
        .call();
    this.gitForPush.commit().setMessage("Sixth commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitConfigMonitor.processGitConfigChanges();

    specs = this.flowCatalog.getSpecs();

    Assert.assertTrue(specs.size() == 2);

    specList = Lists.newArrayList(specs);
    specList.sort(new Comparator<Spec>() {
      @Override
      public int compare(Spec o1, Spec o2) {
        return o1.getUri().compareTo(o2.getUri());
      }
    });

    spec = (FlowSpec)specList.get(0);
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow2"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow2");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value4");

    spec = (FlowSpec)specList.get(1);
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow3"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow3");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value5");

    // reset for next test case
    this.gitForPush.reset().setMode(ResetCommand.ResetType.HARD).setRef("HEAD~4").call();
    this.gitForPush.push().setForce(true).setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitConfigMonitor.processGitConfigChanges();
    specs = this.flowCatalog.getSpecs();

    Assert.assertTrue(specs.size() == 0);
  }

  @Test(dependsOnMethods = "testForcedPushConfig")
  public void testPollingConfig() throws IOException, GitAPIException, URISyntaxException, InterruptedException {
    // push a new config file
    this.testGroupDir.mkdirs();
    this.testFlowFile.createNewFile();
    Files.write("flow.name=testFlow\nflow.group=testGroup\nparam1=value20\n", testFlowFile, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formConfigFilePath(this.testGroupDir.getName(), this.testFlowFile.getName()))
        .call();
    this.gitForPush.commit().setMessage("Seventh commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    Collection<Spec> specs = this.flowCatalog.getSpecs();
    Assert.assertTrue(specs.size() == 0);

    this.gitConfigMonitor.startAsync().awaitRunning();

    // polling is every 5 seconds, so wait twice as long and check
    TimeUnit.SECONDS.sleep(10);

    specs = this.flowCatalog.getSpecs();
    Assert.assertTrue(specs.size() == 1);

    FlowSpec spec = (FlowSpec)(specs.iterator().next());
    Assert.assertEquals(spec.getUri(), new URI("gobblin-flow:/testGroup/testFlow"));
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), "testFlow");
    Assert.assertEquals(spec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), "testGroup");
    Assert.assertEquals(spec.getConfig().getString("param1"), "value20");
  }
}
