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
package org.apache.gobblin.elasticsearch.writer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.elasticsearch.ElasticsearchTestServer;
import org.apache.gobblin.test.AvroRecordGenerator;
import org.apache.gobblin.test.JsonRecordGenerator;
import org.apache.gobblin.test.PayloadType;
import org.apache.gobblin.test.RecordTypeGenerator;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.writer.AsyncWriterManager;
import org.apache.gobblin.writer.BatchAsyncDataWriter;
import org.apache.gobblin.writer.BufferedAsyncDataWriter;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.SequentialBasedBatchAccumulator;


@Slf4j
public class ElasticsearchWriterIntegrationTest {


  private ElasticsearchTestServer _esTestServer;
  private String pid = ManagementFactory.getRuntimeMXBean().getName();

  private List<WriterVariant> variants;
  private List<RecordTypeGenerator> recordGenerators;

  ElasticsearchWriterIntegrationTest() {
    variants = ImmutableList.of(new RestWriterVariant(),
        new TransportWriterVariant());
    recordGenerators = ImmutableList.of(new AvroRecordGenerator(), new JsonRecordGenerator());
  }

  @BeforeSuite(alwaysRun=true)
  public void startServers()
      throws IOException {
    log.error("{}: Starting Elasticsearch Server", pid);
    _esTestServer = new ElasticsearchTestServer();
    _esTestServer.start(60);
  }

  @AfterSuite(alwaysRun=true)
  public void stopServers() {
    log.error("{}: Stopping Elasticsearch Server", pid);
    if (_esTestServer != null ) {
      _esTestServer.stop();
    }
  }


  @Test
  public void testSingleRecordWrite()
      throws IOException {

    for (WriterVariant writerVariant : variants) {
      for (RecordTypeGenerator recordVariant : recordGenerators) {

        String indexName = "posts" + writerVariant.getName().toLowerCase();
        String indexType = recordVariant.getName();
        Config config = writerVariant.getConfigBuilder()
            .setIndexName(indexName)
            .setIndexType(indexType)
            .setTypeMapperClassName(recordVariant.getTypeMapperClassName())
            .setHttpPort(_esTestServer.getHttpPort())
            .setTransportPort(_esTestServer.getTransportPort())
            .build();

        TestClient testClient = writerVariant.getTestClient(config);
        SequentialBasedBatchAccumulator<Object> batchAccumulator = new SequentialBasedBatchAccumulator<>(config);
        BufferedAsyncDataWriter bufferedAsyncDataWriter = new BufferedAsyncDataWriter(batchAccumulator, writerVariant.getBatchAsyncDataWriter(config));


        String id = TestUtils.generateRandomAlphaString(10);
        Object testRecord = recordVariant.getRecord(id, PayloadType.STRING);

        DataWriter writer = AsyncWriterManager.builder().failureAllowanceRatio(0.0).retriesEnabled(false).config(config)
            .asyncDataWriter(bufferedAsyncDataWriter).build();

        try {
          testClient.recreateIndex(indexName);
          writer.write(testRecord);
          writer.commit();
        } finally {
          writer.close();
        }

        try {
          GetResponse response = testClient.get(new GetRequest(indexName, indexType, id));
          Assert.assertEquals(response.getId(), id, "Response id matches request");
          Assert.assertEquals(response.isExists(), true, "Document not found");
        } catch (Exception e) {
          Assert.fail("Failed to get a response", e);
        } finally {
          testClient.close();
        }
      }
    }
  }

  @Test
  public void testMalformedDocCombinations()
      throws IOException {
    for (WriterVariant writerVariant : variants) {
      for (RecordTypeGenerator recordVariant : recordGenerators) {
        for (MalformedDocPolicy policy : MalformedDocPolicy.values()) {
          testMalformedDocs(writerVariant, recordVariant, policy);
        }
      }
    }
  }



  /**
   * Sends two docs in a single batch with different field types
   * Triggers Elasticsearch server to send back an exception due to malformed docs
   * @throws IOException
   */
  public void testMalformedDocs(WriterVariant writerVariant, RecordTypeGenerator recordVariant, MalformedDocPolicy malformedDocPolicy)
      throws IOException {

    String indexName = writerVariant.getName().toLowerCase();
    String indexType = (recordVariant.getName()+malformedDocPolicy.name()).toLowerCase();
    Config config = writerVariant.getConfigBuilder()
        .setIdMappingEnabled(true)
        .setIndexName(indexName)
        .setIndexType(indexType)
        .setHttpPort(_esTestServer.getHttpPort())
        .setTransportPort(_esTestServer.getTransportPort())
        .setTypeMapperClassName(recordVariant.getTypeMapperClassName())
        .setMalformedDocPolicy(malformedDocPolicy)
        .build();


    TestClient testClient = writerVariant.getTestClient(config);
    testClient.recreateIndex(indexName);

    String id1=TestUtils.generateRandomAlphaString(10);
    String id2=TestUtils.generateRandomAlphaString(10);

    Object testRecord1 = recordVariant.getRecord(id1, PayloadType.LONG);
    Object testRecord2 = recordVariant.getRecord(id2, PayloadType.MAP);

    SequentialBasedBatchAccumulator<Object> batchAccumulator = new SequentialBasedBatchAccumulator<>(config);
    BatchAsyncDataWriter elasticsearchWriter = writerVariant.getBatchAsyncDataWriter(config);
    BufferedAsyncDataWriter bufferedAsyncDataWriter = new BufferedAsyncDataWriter(batchAccumulator, elasticsearchWriter);


    DataWriter writer = AsyncWriterManager.builder()
        .failureAllowanceRatio(0.0)
        .retriesEnabled(false)
        .config(config)
        .asyncDataWriter(bufferedAsyncDataWriter)
        .build();

    try {
      writer.write(testRecord1);
      writer.write(testRecord2);
      writer.commit();
      writer.close();
      if (malformedDocPolicy == MalformedDocPolicy.FAIL) {
        Assert.fail("Should have thrown an exception if malformed doc policy was set to Fail");
      }
    }
    catch (Exception e) {
      switch (malformedDocPolicy) {
        case IGNORE:case WARN:{
          Assert.fail("Should not have failed if malformed doc policy was set to ignore or warn", e);
          break;
        }
        case FAIL: {
          // pass through
          break;
        }
        default: {
          throw new RuntimeException("This test does not handle this policyType : " + malformedDocPolicy.toString());
        }
      }
    }

    // Irrespective of policy, first doc should be inserted and second doc should fail
    int docsIndexed = 0;
    try {
      {
        GetResponse response = testClient.get(new GetRequest(indexName, indexType, id1));
        Assert.assertEquals(response.getId(), id1, "Response id matches request");
        System.out.println(malformedDocPolicy + ":" + response.toString());
        if (response.isExists()) {
          docsIndexed++;
        }
      }
      {
        GetResponse response = testClient.get(new GetRequest(indexName, indexType, id2));
        Assert.assertEquals(response.getId(), id2, "Response id matches request");
        System.out.println(malformedDocPolicy + ":" + response.toString());
        if (response.isExists()) {
          docsIndexed++;
        }
      }
      // only one doc should be found
      Assert.assertEquals(docsIndexed, 1, "Only one document should be indexed");
    }
    catch (Exception e) {
      Assert.fail("Failed to get a response", e);
    }
    finally {
      testClient.close();
    }
  }




}
