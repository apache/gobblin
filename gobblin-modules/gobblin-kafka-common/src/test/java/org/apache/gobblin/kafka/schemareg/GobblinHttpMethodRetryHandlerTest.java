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
package org.apache.gobblin.kafka.schemareg;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpMethod;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GobblinHttpMethodRetryHandlerTest {

  @Test
  public void testRetryMethod() {
    GobblinHttpMethodRetryHandler gobblinHttpMethodRetryHandler = new GobblinHttpMethodRetryHandler(1, false);
    HttpMethod mockHttpMethod = Mockito.mock(HttpMethod.class);

    //GobblinHttpHandler.retryMethod should return true on UnknownHostException
    Assert.assertTrue(gobblinHttpMethodRetryHandler.retryMethod(mockHttpMethod, new UnknownHostException("dummyException"), 0));
    Assert.assertTrue(gobblinHttpMethodRetryHandler.retryMethod(mockHttpMethod, new UnknownHostException("dummyException"), 1));
    //Return false when the retry count is exceeded
    Assert.assertFalse(gobblinHttpMethodRetryHandler.retryMethod(mockHttpMethod, new UnknownHostException("dummyException"), 2));

    //Ensure the GobblinHttpMethodRetryHandler has the same behavior as the DefaultHttpMethodRetryHandler for a normal
    //IOException
    DefaultHttpMethodRetryHandler defaultHttpMethodRetryHandler = new DefaultHttpMethodRetryHandler(1, false);
    boolean shouldRetryWithGobblinRetryHandler = gobblinHttpMethodRetryHandler.retryMethod(mockHttpMethod, new IOException("dummyException"), 0);
    boolean shouldRetryWithDefaultRetryHandler = defaultHttpMethodRetryHandler.retryMethod(mockHttpMethod, new IOException("dummyException"), 0);
    Assert.assertTrue(shouldRetryWithGobblinRetryHandler);
    Assert.assertEquals(shouldRetryWithDefaultRetryHandler, shouldRetryWithGobblinRetryHandler);

    shouldRetryWithGobblinRetryHandler = gobblinHttpMethodRetryHandler.retryMethod(mockHttpMethod, new IOException("dummyException"), 2);
    shouldRetryWithDefaultRetryHandler = defaultHttpMethodRetryHandler.retryMethod(mockHttpMethod, new IOException("dummyException"), 2);
    Assert.assertFalse(shouldRetryWithGobblinRetryHandler);
    Assert.assertEquals(shouldRetryWithDefaultRetryHandler, shouldRetryWithGobblinRetryHandler);
  }
}