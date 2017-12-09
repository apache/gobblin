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
package org.apache.gobblin.r2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Queue;

import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.restli.common.RestConstants;

import org.apache.gobblin.HttpTestUtils;
import org.apache.gobblin.async.AsyncRequest;
import org.apache.gobblin.async.BufferedRecord;
import org.apache.gobblin.r2.R2RestRequestBuilder;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


@Test
public class R2RestRequestBuilderTest {
  private static final JacksonDataCodec JACKSON_DATA_CODEC = new JacksonDataCodec();
  /**
   * Build a {@link RestRequest} from a {@link GenericRecord}
   */
  public void testBuildWriteRequest()
      throws URISyntaxException, IOException {
    String urlTemplate = "http://www.test.com/a/part1:${part1}/a/part2:${part2}";
    String verb = "update";
    String protocolVersion = "2.0.0";

    R2RestRequestBuilder builder = spy(new R2RestRequestBuilder(urlTemplate, verb, protocolVersion));
    ArgumentCaptor<RestRequestBuilder> requestBuilderArgument = ArgumentCaptor.forClass(RestRequestBuilder.class);

    Queue<BufferedRecord<GenericRecord>> queue = HttpTestUtils.createQueue(1, false);
    AsyncRequest<GenericRecord, RestRequest> request = builder.buildRequest(queue);
    verify(builder).build(requestBuilderArgument.capture());

    RestRequestBuilder expected = new RestRequestBuilder(new URI("http://www.test.com/a/part1:01/a/part2:02?param1=01"));
    expected.setMethod("PUT");
    expected.setHeader(RestConstants.HEADER_RESTLI_PROTOCOL_VERSION, protocolVersion);
    expected.setHeader(RestConstants.HEADER_RESTLI_REQUEST_METHOD, verb.toLowerCase());
    expected.setHeader(RestConstants.HEADER_CONTENT_TYPE, RestConstants.HEADER_VALUE_APPLICATION_JSON);

    DataMap data = new DataMap();
    data.put("id", "id0");
    expected.setEntity(JACKSON_DATA_CODEC.mapToBytes(data));

    HttpTestUtils.assertEqual(requestBuilderArgument.getValue(), expected);
    Assert.assertEquals(request.getRecordCount(), 1);
    Assert.assertEquals(queue.size(), 0);
  }
}
