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
package org.apache.gobblin.writer.http;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import com.google.common.base.Optional;

/**
 * Writes via RESTful API that accepts plain text as a body
 */
public class HttpWriter<D> extends AbstractHttpWriter<D> {
  @SuppressWarnings("rawtypes")
  public HttpWriter(AbstractHttpWriterBuilder builder) {
    super(builder);
  }

  @Override
  public URI chooseServerHost() {
    return getCurServerHost();
  }

  @Override
  public void onConnect(URI serverHost) throws IOException {}

  @Override
  public Optional<HttpUriRequest> onNewRecord(D record) {
    try {
      HttpUriRequest uriRequest = RequestBuilder.post()
          .addHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType())
          .setUri(getCurServerHost())
          .setEntity(new StringEntity(record.toString(), ContentType.TEXT_PLAIN.toString()))
          .build();
      return Optional.of(uriRequest);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  URI combineUrl(URI uri, Optional<String> resourcePath) {
    if (!resourcePath.isPresent()) {
      return uri;
    }

    try {
      return new URL(getCurServerHost().toURL(), resourcePath.get()).toURI();
    } catch (MalformedURLException | URISyntaxException e) {
      throw new RuntimeException("Failed combining URL", e);
    }
  }
}
