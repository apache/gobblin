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
package org.apache.gobblin.converter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.async.AsyncRequestBuilder;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.http.HttpClient;
import org.apache.gobblin.http.HttpRequestResponseRecord;
import org.apache.gobblin.http.ResponseHandler;
import org.apache.gobblin.http.ResponseStatus;
import org.apache.gobblin.r2.R2ClientFactory;
import org.apache.gobblin.r2.R2Client;
import org.apache.gobblin.r2.R2ResponseStatus;
import org.apache.gobblin.r2.R2RestRequestBuilder;
import org.apache.gobblin.r2.R2RestResponseHandler;
import org.apache.gobblin.utils.HttpConstants;


@Slf4j
public class AvroR2JoinConverter extends AvroHttpJoinConverter<RestRequest, RestResponse>{

  @Override
  protected void fillHttpOutputData(Schema schema, GenericRecord outputRecord, RestRequest restRequest,
      ResponseStatus status)
      throws IOException {
    R2ResponseStatus r2ResponseStatus = (R2ResponseStatus) status;
    HttpRequestResponseRecord record = new HttpRequestResponseRecord();
    record.setRequestUrl(restRequest.getURI().toASCIIString());
    record.setMethod(restRequest.getMethod());
    record.setStatusCode(r2ResponseStatus.getStatusCode());
    record.setContentType(r2ResponseStatus.getContentType());
    record.setBody(r2ResponseStatus.getContent() == null? null: r2ResponseStatus.getContent().asByteBuffer());
    outputRecord.put("HttpRequestResponse", record);
  }

  @Override
  protected HttpClient<RestRequest, RestResponse> createHttpClient(Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);

    // By default, use http schema
    R2ClientFactory.Schema schema = R2ClientFactory.Schema.HTTP;
    if (urlTemplate.startsWith(HttpConstants.SCHEMA_D2)) {
      schema = R2ClientFactory.Schema.D2;
    }

    R2ClientFactory factory = new R2ClientFactory(schema);
    Client client = factory.createInstance(config);
    return new R2Client(client, config, broker);
  }

  @Override
  protected ResponseHandler<RestRequest, RestResponse> createResponseHandler(Config config) {
    return new R2RestResponseHandler();
  }

  @Override
  protected AsyncRequestBuilder<GenericRecord, RestRequest> createRequestBuilder(Config config) {
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);

    return new R2RestRequestBuilder(urlTemplate, verb, contentType);
  }

}
