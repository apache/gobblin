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

package org.apache.gobblin.runtime.spec_executorInstance;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Future;

import org.mockito.Mockito;

import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;


@EqualsAndHashCode(of = "config", callSuper=false)
public class MockedSpecExecutor extends InMemorySpecExecutor {
  private final SpecProducer<Spec> mockedSpecProducer;
  private final Config config;

  public MockedSpecExecutor(Config config) {
    super(config);
    this.config = config;
    this.mockedSpecProducer = Mockito.mock(SpecProducer.class);
    when(mockedSpecProducer.addSpec(any())).thenReturn(new CompletedFuture(Boolean.TRUE, null));
    when(mockedSpecProducer.serializeAddSpecResponse(any())).thenReturn("");
    when(mockedSpecProducer.deserializeAddSpecResponse(any())).thenReturn(new CompletedFuture(Boolean.TRUE, null));
    when(mockedSpecProducer.cancelJob(any(), any())).thenReturn(new CompletedFuture(Boolean.TRUE, null));
    }

  public static SpecExecutor createDummySpecExecutor(URI uri) {
    return new MockedSpecExecutor(makeDummyConfigsForSpecExecutor(uri.toString()));
  }

  public static Config makeDummyConfigsForSpecExecutor(String specUriInString) {
    String specStoreDir = "/tmp/specStoreDir";
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", specStoreDir);
    properties.put("specExecInstance.capabilities", "source:destination");
    properties.put(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, specUriInString);
    properties.put("uri", specUriInString);
    return ConfigUtils.propertiesToConfig(properties);
  }

  @Override
  public Future<? extends SpecProducer<Spec>> getProducer(){
    return new CompletedFuture<>(this.mockedSpecProducer, null);
  }
}
