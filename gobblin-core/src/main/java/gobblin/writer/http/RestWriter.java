/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.writer.http;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import com.google.common.base.Optional;

import gobblin.converter.http.RestEntry;

/**
 * Writes via RESTful API that accepts plain text as a body and resource path from RestEntry
 */
public class RestWriter extends HttpWriter<RestEntry<String>> {

  public RestWriter(RestWriterBuilder builder) {
    super(builder);
  }

  @Override
  public Optional<HttpUriRequest> onNewRecord(RestEntry<String> record) {
    HttpUriRequest uriRequest = RequestBuilder.post()
        .addHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType())
        .setUri(combineUrl(getCurServerHost(), record.getResourcePath()))
        .setEntity(new StringEntity(record.getRestEntryVal(), ContentType.TEXT_PLAIN))
        .build();
    return Optional.of(uriRequest);
  }
}
