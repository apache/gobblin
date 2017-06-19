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

package gobblin.http;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;

import gobblin.async.AsyncRequest;


/**
 * A specific {@link AsyncRequest} related to a {@link HttpUriRequest} and its associated record information
 */
public class ApacheHttpRequest<D> extends AsyncRequest<D, HttpUriRequest> {
  @Override
  public String toString() {
    HttpUriRequest request = getRawRequest();
    StringBuilder outBuffer = new StringBuilder();
    String endl = "\n";
    outBuffer.append("ApacheHttpRequest Info").append(endl);
    outBuffer.append("type: HttpUriRequest").append(endl);
    outBuffer.append("uri: ").append(request.getURI().toString()).append(endl);
    outBuffer.append("headers: ");
    Arrays.stream(request.getAllHeaders()).forEach(header ->
        outBuffer.append("[").append(header.getName()).append(":").append(header.getValue()).append("] ")
    );
    outBuffer.append(endl);

    if (request instanceof HttpEntityEnclosingRequest) {
      try {
        String body = EntityUtils.toString(((HttpEntityEnclosingRequest) request).getEntity());
        outBuffer.append("body: ").append(body).append(endl);
      } catch (IOException e) {
        outBuffer.append("body: ").append(e.getMessage()).append(endl);
      }
    }
    return outBuffer.toString();
  }
}
