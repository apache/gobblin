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
package org.apache.gobblin.elasticsearch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.gobblin.test.TestUtils;
import org.testng.Assert;

import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;


/**
 * A Test ElasticSearch server
 */
@Slf4j
@NotThreadSafe
public class ElasticsearchTestServer {


  private static final String ELASTICSEARCH_VERSION="5.6.8";
  private static final String TEST_ROOT_DIR="gobblin-modules/gobblin-elasticsearch/test-elasticsearch/";
  // The clean elasticsearch instance is installed here
  private static final String BASE_ELASTICSEARCH_INSTALL =TEST_ROOT_DIR + "elasticsearch-" + ELASTICSEARCH_VERSION;
  // Per-test elasticsearch instances are installed under a different directory
  private static final String TEST_INSTALL_PREFIX =TEST_ROOT_DIR + "es-test-install-";
  private static final String ELASTICSEARCH_BIN="/bin/elasticsearch";
  private static final String ELASTICSEARCH_CONFIG_FILE= "/config/elasticsearch.yml";
  private static final String ELASTICSEARCH_JVMOPTS_FILE="/config/jvm.options";
  private final String _testId;
  private final int _tcpPort;
  private Process elasticProcess;
  private final int _httpPort;
  private String _pid = ManagementFactory.getRuntimeMXBean().getName();
  private final String _testInstallDirectory;
  private AtomicBoolean _started = new AtomicBoolean(false);

  public ElasticsearchTestServer(String testId)
      throws IOException {
    this(testId, TestUtils.findFreePort(), TestUtils.findFreePort());
  }

  private ElasticsearchTestServer(String testId, int httpPort, int tcpPort)
      throws IOException {
    _testId = testId;
    _httpPort = httpPort;
    _tcpPort = tcpPort;
    _testInstallDirectory = TEST_INSTALL_PREFIX + _testId;
    try {
      createInstallation();
    }
    catch (Exception e) {
      throw new IOException("Failed to create a test installation of elasticsearch", e);
    }
    configure();
  }

  public ElasticsearchTestServer()
      throws IOException {
    this(TestUtils.generateRandomAlphaString(25));
  }

  private void createInstallation()
      throws IOException {
    File srcDir = new File(BASE_ELASTICSEARCH_INSTALL);
    if (!srcDir.exists()) {
      throw new IOException("Could not find base elasticsearch instance installed at " + srcDir.getAbsolutePath() + "\n"
          + "Run ./gradlew :gobblin-modules:gobblin-elasticsearch:installTestDependencies before running this test");
    }
    File destDir = new File(_testInstallDirectory);
    log.debug("About to recreate directory : {}", destDir.getPath());
    if (destDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(destDir);
    }

    String[] commands = {"cp", "-r", srcDir.getAbsolutePath(), destDir.getAbsolutePath()};
    try {
      log.debug("{}: Will run command: {}", this._pid, Arrays.toString(commands));
      Process copyProcess = new ProcessBuilder().inheritIO().command(commands).start();
      copyProcess.waitFor();
    } catch (Exception e) {
      log.error("Failed to create installation directory at {}", destDir.getPath(), e);
      Throwables.propagate(e);
    }
  }




  private void configure() throws IOException {
    File configFile = new File(_testInstallDirectory + ELASTICSEARCH_CONFIG_FILE);
    FileOutputStream configFileStream = new FileOutputStream(configFile);
    try {
      configFileStream.write(("cluster.name: " + _testId + "\n").getBytes("UTF-8"));
      configFileStream.write(("http.port: " + _httpPort + "\n").getBytes("UTF-8"));
      configFileStream.write(("transport.tcp.port: " + _tcpPort + "\n").getBytes("UTF-8"));
    }
    finally {
      configFileStream.close();
    }

    File jvmConfigFile = new File(_testInstallDirectory + ELASTICSEARCH_JVMOPTS_FILE);
    try (Stream<String> lines = Files.lines(jvmConfigFile.toPath())) {
      List<String> newLines = lines.map(line -> line.replaceAll("^\\s*(-Xm[s,x]).*$", "$1128m"))
      .collect(Collectors.toList());
      Files.write(jvmConfigFile.toPath(), newLines);
    }
  }

  public void start(int maxStartupTimeSeconds)
  {
    if (_started.get()) {
      log.warn("ElasticSearch server has already been attempted to be started... returning without doing anything");
      return;
    }
    _started.set(true);

    log.error("{}: Starting elasticsearch server on port {}", this._pid, this._httpPort);
    String[] commands = {_testInstallDirectory + ELASTICSEARCH_BIN};

    try {
      log.error("{}: Will run command: {}", this._pid, Arrays.toString(commands));
      elasticProcess = new ProcessBuilder().inheritIO().command(commands).start();
      if (elasticProcess != null) {
        // register destroy of process on shutdown in-case of unclean test termination
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            if (elasticProcess!=null) {
              elasticProcess.destroy();
            }
          }
        });
      }
    } catch (Exception e) {
      log.error("Failed to start elasticsearch server", e);
      Throwables.propagate(e);
    }

    boolean isUp = false;
    int numTries = maxStartupTimeSeconds * 2;
    while (!isUp && numTries-- > 0) {
      try {
        Thread.sleep(500); // wait 1/2 second
        isUp = isUp();
      } catch (Exception e) {

      }
    }
    Assert.assertTrue(isUp, "Server is not up!");
  }


  public boolean isUp()
  {
    try {
      URL url = new URL("http://localhost:" + _httpPort + "/_cluster/health?wait_for_status=green");
      long startTime = System.nanoTime();
      HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
      int responseCode = httpURLConnection.getResponseCode();
      log.info("Duration: {} seconds, Response code = {}",
          (System.nanoTime() - startTime) / 1000000000.0,
          responseCode);
      if (responseCode == 200) { return true; } else {return false;}
    }
    catch (Exception e) {
      Throwables.propagate(e);
      return false;
    }
  }

  public int getTransportPort() {
    return _tcpPort;
  }


  public int getHttpPort() { return _httpPort; }


  public void stop() {
    if (elasticProcess != null) {
      try {
        elasticProcess.destroy();
        elasticProcess = null; // set to null to prevent redundant call to destroy on shutdown
      } catch (Exception e) {
        log.warn("Failed to stop the ElasticSearch server", e);
      }
    }
  }
}
