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
package org.apache.gobblin.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SimpleScope;
import org.apache.gobblin.broker.SimpleScopeType;
import org.apache.gobblin.broker.iface.NoSuchScopeException;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.capability.Capability;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;

import lombok.Getter;


/**
 * Tests for DataPublisherFactory
 */
public class DataPublisherFactoryTest {

  @Test
  public void testGetNonThreadSafePublisher()
      throws IOException {
    SharedResourcesBroker broker =
        SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(ConfigFactory.empty(),
            SimpleScopeType.GLOBAL.defaultScopeInstance());

    DataPublisher publisher1 = DataPublisherFactory.get(TestNonThreadsafeDataPublisher.class.getName(), null, broker);
    DataPublisher publisher2 = DataPublisherFactory.get(TestNonThreadsafeDataPublisher.class.getName(), null, broker);

    // should get different publishers
    Assert.assertNotEquals(publisher1, publisher2);

    // Check capabilities
    Assert.assertTrue(publisher1.supportsCapability(DataPublisher.REUSABLE, Collections.EMPTY_MAP));
    Assert.assertFalse(publisher1.supportsCapability(Capability.THREADSAFE, Collections.EMPTY_MAP));
  }

  @Test
  public void testGetThreadSafePublisher()
      throws IOException, NotConfiguredException, NoSuchScopeException {
    SharedResourcesBroker<SimpleScopeType> broker =
        SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(ConfigFactory.empty(),
            SimpleScopeType.GLOBAL.defaultScopeInstance());

    SharedResourcesBroker<SimpleScopeType> localBroker1 =
        broker.newSubscopedBuilder(new SimpleScope<>(SimpleScopeType.LOCAL, "local1")).build();

    TestThreadsafeDataPublisher publisher1 = (TestThreadsafeDataPublisher)DataPublisherFactory.get(TestThreadsafeDataPublisher.class.getName(), null, broker);
    TestThreadsafeDataPublisher publisher2 = (TestThreadsafeDataPublisher)DataPublisherFactory.get(TestThreadsafeDataPublisher.class.getName(), null, broker);

    // should get the same publisher
    Assert.assertEquals(publisher1, publisher2);

    TestThreadsafeDataPublisher publisher3 =
        (TestThreadsafeDataPublisher)localBroker1.getSharedResource(new DataPublisherFactory<>(),
            new DataPublisherKey(TestThreadsafeDataPublisher.class.getName(), null));

    // should not get the same publisher
    Assert.assertNotEquals(publisher2, publisher3);

    TestThreadsafeDataPublisher publisher4 =
        (TestThreadsafeDataPublisher)localBroker1.getSharedResourceAtScope(new DataPublisherFactory<>(),
            new DataPublisherKey(TestThreadsafeDataPublisher.class.getName(), null), SimpleScopeType.LOCAL);

    // should get the same publisher
    Assert.assertEquals(publisher3, publisher4);

    // Check capabilities
    Assert.assertTrue(publisher1.supportsCapability(DataPublisher.REUSABLE, Collections.EMPTY_MAP));
    Assert.assertTrue(publisher1.supportsCapability(Capability.THREADSAFE, Collections.EMPTY_MAP));

    // Check data publisher is not closed
    Assert.assertFalse(publisher1.isClosed());
    Assert.assertFalse(publisher2.isClosed());
    Assert.assertFalse(publisher3.isClosed());
    Assert.assertFalse(publisher4.isClosed());
    broker.close();
    // Check all publishers are closed
    Assert.assertTrue(publisher1.isClosed());
    Assert.assertTrue(publisher2.isClosed());
    Assert.assertTrue(publisher3.isClosed());
    Assert.assertTrue(publisher4.isClosed());
  }

  @Test()
  public void testMultiThreadedGetNonThreadSafePublisher()
      throws InterruptedException, ExecutionException, IOException {
    SharedResourcesBroker broker =
        SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(ConfigFactory.empty(),
            SimpleScopeType.GLOBAL.defaultScopeInstance());

    ExecutorService service = Executors.newFixedThreadPool(40);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 100000; i++) {
      futures.add(service.submit(new GetNonThreadSafePublisher(broker)));
    }

    for (Future f: futures) {
      f.get();
    }
    service.shutdown();
    service.awaitTermination(100, TimeUnit.SECONDS);
  }

  private static class GetNonThreadSafePublisher implements Runnable {
    private final SharedResourcesBroker broker;
    private static long count = 0;

    GetNonThreadSafePublisher(SharedResourcesBroker broker) {
      this.broker = broker;
    }

    @Override
    public void run() {
      try {
        DataPublisher publisher1 = DataPublisherFactory.get(TestNonThreadsafeDataPublisher.class.getName(), null, this.broker);
        Assert.assertNotNull(publisher1);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }


  private static class TestNonThreadsafeDataPublisher extends DataPublisher {
    @Getter
    private boolean isClosed = false;

    public TestNonThreadsafeDataPublisher(State state) {
      super(state);
    }

    @Override
    public void initialize() throws IOException {
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    }

    @Override
    public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    }

    @Override
    public void close() throws IOException {
      isClosed = true;
    }

    @Override
    public boolean supportsCapability(Capability c, Map<String, Object> properties) {
      return c == DataPublisher.REUSABLE;
    }
  }

  private static class TestThreadsafeDataPublisher extends TestNonThreadsafeDataPublisher {
    public TestThreadsafeDataPublisher(State state) {
      super(state);
    }

    @Override
    public boolean supportsCapability(Capability c, Map<String, Object> properties) {
      return (c == Capability.THREADSAFE || c == DataPublisher.REUSABLE);
    }
  }
}
