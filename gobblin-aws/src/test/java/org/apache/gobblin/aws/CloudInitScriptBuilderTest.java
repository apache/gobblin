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

package gobblin.aws;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;


/**
 * Unit tests for {@link CloudInitScriptBuilder}.
 *
 * @author Abhishek Tiwari
 */
@Test(groups = { "gobblin.aws" })
public class CloudInitScriptBuilderTest {
  private static final String MASTER_CLOUD_INIT_SCRIPT = "masterCloudInit.sh";
  private static final String WORKER_CLOUD_INIT_SCRIPT = "workerCloudInit.sh";

  private String clusterName = "cluster";
  private String nfsParentDir = "/home/ec2-user/";
  private String awsConfDir = nfsParentDir + clusterName + "/cluster-conf/";
  private String appWorkDir = nfsParentDir + clusterName + "/work-dir/";
  private String sinkLogRootDir = nfsParentDir + clusterName + "/log-dir/";

  private String masterS3ConfUri = "https://s3-us-west-2.amazonaws.com/some-bucket/cluster-conf/";
  private String masterS3ConfFiles = "application.conf,log4j-aws.properties,quartz.properties";
  private String masterS3JarsUri = "https://s3-us-west-2.amazonaws.com/some-bucket/gobblin-jars/";
  private String masterS3JarFiles = "myjar1.jar,myjar2.jar,myjar3.jar,myjar4-\"${vr}\".jar";
  private String masterJarsDir = nfsParentDir + clusterName + "/gobblin-jars/";
  private String masterJvmMemory = "-Xms1G";
  private String masterPublicIp = "0.0.0.0";

  private String workerS3ConfUri = "https://s3-us-west-2.amazonaws.com/some-bucket/cluster-conf/";
  private String workerS3ConfFiles = "application.conf,log4j-aws.properties,quartz.properties";
  private String workerS3JarsUri = "https://s3-us-west-2.amazonaws.com/some-bucket/gobblin-jars/";
  private String workerS3JarFiles = "myjar1.jar,myjar2.jar,myjar3.jar,myjar4-\"${vr}\".jar";
  private String workerJarsDir = nfsParentDir + clusterName + "/gobblin-jars/";
  private String workerJvmMemory = "-Xms1G";

  private String expectedMasterCloudInitScript;
  private String expectedWorkerCloudInitScript;

  private Optional<String> gobblinVersion = Optional.of("0.7.1");

  @BeforeClass
  public void setup() throws Exception {
    this.expectedMasterCloudInitScript = IOUtils.toString(GobblinAWSClusterLauncherTest.class.getClassLoader()
        .getResourceAsStream(MASTER_CLOUD_INIT_SCRIPT), "UTF-8");
    this.expectedWorkerCloudInitScript = IOUtils.toString(GobblinAWSClusterLauncherTest.class.getClassLoader()
        .getResourceAsStream(WORKER_CLOUD_INIT_SCRIPT), "UTF-8");
  }

  @Test
  public void testBuildClusterMasterCommand() {
    final String script = CloudInitScriptBuilder.buildClusterMasterCommand(this.clusterName, this.nfsParentDir,
        this.sinkLogRootDir, this.awsConfDir, this.appWorkDir, this.masterS3ConfUri, this.masterS3ConfFiles,
        this.masterS3JarsUri, this.masterS3JarFiles, this.masterJarsDir, this.masterJvmMemory,
        Optional.<String>absent(), gobblinVersion);
    final String decodedScript = new String(Base64.decodeBase64(script));

    Assert.assertEquals(decodedScript, this.expectedMasterCloudInitScript,
        "Master launcher cloud-init script not built as expected");
  }

  @Test
  public void testBuildClusterWorkerCommand() {
    final String script = CloudInitScriptBuilder.buildClusterWorkerCommand(this.clusterName, this.nfsParentDir,
        this.sinkLogRootDir, this.awsConfDir, this.appWorkDir, this.masterPublicIp, this.workerS3ConfUri,
        this.workerS3ConfFiles, this.workerS3JarsUri, this.workerS3JarFiles, this.workerJarsDir, this.workerJvmMemory,
        Optional.<String>absent(), gobblinVersion);
    final String decodedScript = new String(Base64.decodeBase64(script));

    Assert.assertEquals(decodedScript, this.expectedWorkerCloudInitScript,
        "Worker launcher cloud-init script not built as expected");
  }
}
