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

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import lombok.Setter;

import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * An implementation of {@link BasePooledObjectFactory} for {@link HttpClient}.
 *
 * @author mitu
 */
public class HttpClientFactory extends BasePooledObjectFactory<HttpClient>{

  @Setter private int soTimeout = -1;
  @Setter private int connTimeout = -1;
  @Setter private int httpMethodRetryCount = 3;
  @Setter private boolean httpRequestSentRetryEnabled = false;
  @Setter private String httpMethodRetryHandlerClass = DefaultHttpMethodRetryHandler.class.getName();

  public HttpClientFactory() {
  }

  @Override
  public HttpClient create() {
    HttpClient client = new HttpClient();
    if (soTimeout >= 0) {
      client.getParams().setSoTimeout(soTimeout);
    }

    ClassAliasResolver<HttpMethodRetryHandler> aliasResolver = new ClassAliasResolver<>(HttpMethodRetryHandler.class);
    HttpMethodRetryHandler httpMethodRetryHandler;
    try {
      httpMethodRetryHandler = GobblinConstructorUtils.invokeLongestConstructor(aliasResolver.resolveClass(httpMethodRetryHandlerClass), httpMethodRetryCount, httpRequestSentRetryEnabled);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
    client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, httpMethodRetryHandler);

    if (connTimeout >= 0) {
      client.getHttpConnectionManager().getParams().setConnectionTimeout(connTimeout);
    }

    return client;
  }

  @Override
  public PooledObject<HttpClient> wrap(HttpClient obj) {
    return new DefaultPooledObject<>(obj);
  }

}
