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

package gobblin.runtime.job_monitor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.testing.AssertWithBackoff;
import gobblin.util.Either;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class MockedKafkaJobMonitor extends KafkaJobMonitor {

  private static final Splitter SPLITTER_COMMA = Splitter.on(",");
  private static final Splitter SPLITTER_COLON = Splitter.on(":");
  public static final String REMOVE = "remove";

  @Getter
  private final Map<URI, JobSpec> jobSpecs;
  @Getter
  private final MockKafkaStream mockKafkaStream;

  public static MockedKafkaJobMonitor create(Config config) {
    return new MockedKafkaJobMonitor(config, Maps.<URI, JobSpec>newConcurrentMap());
  }

  private MockedKafkaJobMonitor(Config config, Map<URI, JobSpec> jobSpecs) {
    super("topic", createMockCatalog(jobSpecs), config);

    this.jobSpecs = jobSpecs;
    this.mockKafkaStream = new MockKafkaStream(1);
  }

  private static MutableJobCatalog createMockCatalog(final Map<URI, JobSpec> jobSpecs) {
    MutableJobCatalog jobCatalog = Mockito.mock(MutableJobCatalog.class);

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation)
          throws Throwable {
        JobSpec jobSpec = (JobSpec) invocation.getArguments()[0];
        jobSpecs.put(jobSpec.getUri(), jobSpec);
        return null;
      }
    }).when(jobCatalog).put(Mockito.any(JobSpec.class));

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation)
          throws Throwable {
        URI uri = (URI) invocation.getArguments()[0];
        jobSpecs.remove(uri);
        return null;
      }
    }).when(jobCatalog).remove(Mockito.any(URI.class));

    return jobCatalog;
  }

  @Override
  public Collection<Either<JobSpec, URI>> parseJobSpec(byte[] message)
      throws IOException {
    try {
      String messageString = new String(message, Charsets.UTF_8);
      List<Either<JobSpec, URI>> jobSpecs = Lists.newArrayList();

      for (String oneInstruction : SPLITTER_COMMA.split(messageString)) {

        List<String> tokens = SPLITTER_COLON.splitToList(oneInstruction);

        if (tokens.get(0).equals(REMOVE)) {
          URI uri = new URI(tokens.get(1));
          jobSpecs.add(Either.<JobSpec, URI>right(uri));
        } else {
          URI uri = new URI(tokens.get(0));
          String version = tokens.get(1);
          JobSpec jobSpec = new JobSpec.Builder(uri).withConfig(ConfigFactory.empty()).withVersion(version).build();
          jobSpecs.add(Either.<JobSpec, URI>left(jobSpec));
        }
      }
      return jobSpecs;
    } catch (URISyntaxException use) {
      throw new IOException(use);
    }
  }

  @Override
  protected List<KafkaStream<byte[], byte[]>> createStreams() {
    return this.mockKafkaStream.getMockStreams();
  }

  @Override
  protected ConsumerConnector createConsumerConnector() {
    return Mockito.mock(ConsumerConnector.class);
  }

  @Override
  public void shutDown() {
    this.mockKafkaStream.shutdown();
    super.shutDown();
  }

  public void awaitExactlyNSpecs(final int n) throws Exception {
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(@Nullable Void input) {
        return MockedKafkaJobMonitor.this.jobSpecs.size() == n;
      }
    }, 30000, n + " specs", log, 2, 1000);
  }
}
