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
package org.apache.gobblin.writer;

import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.http.ApacheHttpClient;
import org.apache.gobblin.http.ApacheHttpResponseHandler;
import org.apache.gobblin.http.ApacheHttpRequestBuilder;
import org.apache.gobblin.utils.HttpConstants;
import org.apache.gobblin.utils.HttpUtils;


@Slf4j
public class AvroHttpWriterBuilder extends AsyncHttpWriterBuilder<GenericRecord, HttpUriRequest, CloseableHttpResponse> {

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.CONTENT_TYPE, "application/json")
          .build());

  @Override
  public AvroHttpWriterBuilder fromConfig(Config config) {
    config = config.withFallback(FALLBACK);
    ApacheHttpClient client = new ApacheHttpClient(HttpClientBuilder.create(), config, broker);
    this.client = client;

    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);
    this.asyncRequestBuilder = new ApacheHttpRequestBuilder(urlTemplate, verb, contentType);

    Set<String> errorCodeWhitelist = HttpUtils.getErrorCodeWhitelist(config);
    this.responseHandler = new ApacheHttpResponseHandler(errorCodeWhitelist);
    return this;
  }
}
