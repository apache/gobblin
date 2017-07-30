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

package gobblin.kafka.writer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import org.testng.Assert;
import kafka.consumer.ConsumerIterator;
import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.KafkaTestBase;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;


/**
 * Tests that set up a complete standalone Gobblin pipeline along with a Kafka suite
 */
@Slf4j
public class Kafka08DataWriterIntegrationTest {

  private static final String JOB_PROPS_DIR="gobblin-modules/gobblin-kafka-08/resource/job-props/";
  private static final String TEST_LAUNCHER_PROPERTIES_FILE = JOB_PROPS_DIR + "testKafkaIngest.properties";
  private static final String TEST_INGEST_PULL_FILE = JOB_PROPS_DIR + "testKafkaIngest.pull";
  private Properties gobblinProps;
  private Properties jobProps;
  private KafkaTestBase kafkaTestHelper;

  private static final String TOPIC = Kafka08DataWriterIntegrationTest.class.getName();

  @BeforeClass
  public void setup() throws Exception {


    kafkaTestHelper = new KafkaTestBase();
    this.gobblinProps = new Properties();
    gobblinProps.load(new FileReader(TEST_LAUNCHER_PROPERTIES_FILE));

    this.jobProps = new Properties();
    jobProps.load(new FileReader(TEST_INGEST_PULL_FILE));

    replaceProperties(gobblinProps, "{$topic}", TOPIC);
    replaceProperties(gobblinProps, "{$kafkaPort}", ""+ kafkaTestHelper.getKafkaServerPort());
    replaceProperties(jobProps, "{$topic}", TOPIC);
    replaceProperties(jobProps, "{$kafkaPort}", ""+kafkaTestHelper.getKafkaServerPort());

    kafkaTestHelper.startServers();
  }

  private void replaceProperties(Properties props, String searchString, String replacementString) {
    for (String key: props.stringPropertyNames())
    {
      String value = props.getProperty(key);
      if (value.contains(searchString))
      {
        String replacedValue = value.replace(searchString, replacementString);
        props.setProperty(key, replacedValue);
      }
    }
  }



  @Test
  public void testErrors() throws Exception {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    int numRecordsPerExtract = 5;
    int numParallel = 2;
    int errorEvery = 2000;
    int totalRecords = numRecordsPerExtract * numParallel;
    int totalSuccessful = totalRecords / errorEvery + totalRecords%errorEvery;
    {
      Closer closer = Closer.create();

    try {

      kafkaTestHelper.provisionTopic(TOPIC);
      jobProps.setProperty("source.numRecordsPerExtract",""+numRecordsPerExtract);
      jobProps.setProperty("source.numParallelism",""+numParallel);
      jobProps.setProperty("writer.kafka.producerConfig.flaky.errorType","regex");
      // all records from partition 0 will be dropped.
      jobProps.setProperty("writer.kafka.producerConfig.flaky.regexPattern",":index:0.*");
      jobProps.setProperty("job.commit.policy","partial");
      jobProps.setProperty("publish.at.job.level","false");
      totalSuccessful = 5;  // number of records in partition 1
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(gobblinProps, jobProps));
      jobLauncher.launchJob(null);
    }
    catch (Exception e) {
      log.error("Failed to run job with exception ", e);
      Assert.fail("Should not throw exception on running the job");
    }
    finally
    {
      closer.close();
    }
    // test records written

      testRecordsWritten(totalSuccessful, TOPIC);
  }
    boolean trySecond = true;
    if (trySecond) {
      Closer closer = Closer.create();
      try {
        jobProps.setProperty("source.numRecordsPerExtract", "" + numRecordsPerExtract);
        jobProps.setProperty("source.numParallelism", "" + numParallel);
        jobProps.setProperty("writer.kafka.producerConfig.flaky.errorType", "nth");
        jobProps.setProperty("writer.kafka.producerConfig.flaky.errorEvery", "" + errorEvery);
        JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(gobblinProps, jobProps));
        jobLauncher.launchJob(null);
        totalSuccessful = totalRecords / errorEvery + totalRecords%errorEvery;
      } catch (Exception e) {
        log.error("Failed to run job with exception ", e);
        Assert.fail("Should not throw exception on running the job");
      } finally {
        closer.close();
      }
    }

      // test records written
      testRecordsWritten(totalSuccessful, TOPIC);

  }

  private void testRecordsWritten(int totalSuccessful, String topic)
      throws UnsupportedEncodingException {
    final ConsumerIterator<byte[], byte[]> iterator = kafkaTestHelper.getIteratorForTopic(topic);
    for (int i = 0; i < totalSuccessful; ++i) {
      String message = new String(iterator.next().message(), "UTF-8");
      log.debug(String.format("%d of %d: Message consumed: %s", (i+1), totalSuccessful, message));
    }

  }


  @AfterClass
  public void stopServers()
      throws IOException {
    try {
      kafkaTestHelper.stopClients();
    }
    finally
    {
      kafkaTestHelper.stopServers();
    }
  }

  @AfterClass
  @BeforeClass
  public void cleanup() throws Exception {
    File file = new File("gobblin-kafka/testOutput");
    FileUtils.deleteDirectory(file);
  }
}
