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

package gobblin.r2;

import java.nio.charset.Charset;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;

import gobblin.async.AsyncRequest;


/**
 * A specific {@link AsyncRequest} related to a {@link RestRequest} and its associated record information
 */
public class R2Request<D> extends AsyncRequest<D, RestRequest> {
  @Override
  public String toString() {
    RestRequest request = getRawRequest();
    StringBuilder outBuffer = new StringBuilder();
    String endl = "\n";
    outBuffer.append("R2Request Info").append(endl);
    outBuffer.append("type: RestRequest").append(endl);
    outBuffer.append("uri: ").append(request.getURI().toString()).append(endl);
    outBuffer.append("headers: ");
    request.getHeaders().forEach((k, v) ->
        outBuffer.append("[").append(k).append(":").append(v).append("] ")
    );
    outBuffer.append(endl);

    ByteString entity = request.getEntity();
    if (entity != null) {
      outBuffer.append("body: ").append(entity.asString(Charset.defaultCharset())).append(endl);
    }
    return outBuffer.toString();
  }
}
