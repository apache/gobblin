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
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpMethod;

import org.apache.gobblin.annotation.Alias;

/**
 * An extension of {@link DefaultHttpMethodRetryHandler} that retries the HTTP request on network errors such as
 * {@link java.net.UnknownHostException} and {@link java.net.NoRouteToHostException}.
 */
@Alias (value = "gobblinhttpretryhandler")
public class GobblinHttpMethodRetryHandler extends DefaultHttpMethodRetryHandler {

  public GobblinHttpMethodRetryHandler() {
    this(3, false);
  }

  public GobblinHttpMethodRetryHandler(int retryCount, boolean requestSentRetryEnabled) {
    super(retryCount, requestSentRetryEnabled);
  }

  @Override
  public boolean retryMethod(final HttpMethod method, final IOException exception, int executionCount) {
    if (method == null) {
      throw new IllegalArgumentException("HTTP method may not be null");
    }
    if (exception == null) {
      throw new IllegalArgumentException("Exception parameter may not be null");
    }
    if (executionCount > super.getRetryCount()) {
      // Do not retry if over max retry count
      return false;
    }
    //Override the behavior of DefaultHttpMethodRetryHandler to retry in case of UnknownHostException
    // and NoRouteToHostException.
    if (exception instanceof UnknownHostException || exception instanceof NoRouteToHostException) {
      return true;
    }
    return super.retryMethod(method, exception, executionCount);
  }
}
