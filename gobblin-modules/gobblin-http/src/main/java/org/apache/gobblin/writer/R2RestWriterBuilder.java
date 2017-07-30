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

package gobblin.writer;

import com.google.common.collect.ImmutableMap;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gobblin.r2.R2Client;
import gobblin.r2.R2ClientFactory;
import gobblin.r2.R2RestRequestBuilder;
import gobblin.r2.R2RestResponseHandler;
import gobblin.utils.HttpConstants;
import gobblin.utils.HttpUtils;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class R2RestWriterBuilder extends AsyncHttpWriterBuilder<GenericRecord, RestRequest, RestResponse> {

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.PROTOCOL_VERSION, "2.0.0")
          .build());

  @Override
  public R2RestWriterBuilder fromConfig(Config config) {
    config = config.withFallback(FALLBACK);
    this.client = createClient(config);

    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String protocolVersion = config.getString(HttpConstants.PROTOCOL_VERSION);
    asyncRequestBuilder = new R2RestRequestBuilder(urlTemplate, verb, protocolVersion);

    Set<String> errorCodeWhitelist = HttpUtils.getErrorCodeWhitelist(config);
    responseHandler = new R2RestResponseHandler(errorCodeWhitelist, metricContext);
    return this;
  }

  protected R2Client createClient(Config config) {
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);

    // By default, use http schema
    R2ClientFactory.Schema schema = R2ClientFactory.Schema.HTTP;
    if (urlTemplate.startsWith(HttpConstants.SCHEMA_D2)) {
      schema = R2ClientFactory.Schema.D2;
    }

    R2ClientFactory factory = new R2ClientFactory(schema);
    Client client = factory.createInstance(config);
    return new R2Client(client, config, getBroker());
  }
}

