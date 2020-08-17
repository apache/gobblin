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

package org.apache.gobblin.multistage.factory;

import org.apache.gobblin.configuration.State;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@PrepareForTest({HttpClientBuilder.class})
public class ApacheHttpClientFactoryTest extends PowerMockTestCase {
  @Mock
  private HttpClientBuilder httpClientBuilder;

  @Mock
  private CloseableHttpClient closeableHttpClient;

  /**
   * Test whether an Apache HttpClient is produced as expected
   */
  @Test
  public void testGet() {
    ApacheHttpClientFactory factory = new ApacheHttpClientFactory();
    PowerMockito.mockStatic(HttpClientBuilder.class);
    PowerMockito.when(HttpClientBuilder.create()).thenReturn(httpClientBuilder);
    when(httpClientBuilder.build()).thenReturn(closeableHttpClient);
    Assert.assertEquals(factory.get(new State()), closeableHttpClient);
  }
}