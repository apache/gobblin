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

package gobblin.util.limiter;

import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import gobblin.restli.throttling.PermitAllocation;
import gobblin.restli.throttling.PermitRequest;
import gobblin.restli.throttling.PermitsGetRequestBuilder;
import gobblin.restli.throttling.PermitsRequestBuilders;
import java.io.Closeable;
import lombok.AllArgsConstructor;


/**
 * A {@link Limiter} that forwards permit requests to a Rest.li throttling service endpoint.
 */
@AllArgsConstructor
public class RestliServiceBasedLimiter implements Limiter {

  /** {@link RestClient} pointing to a Rest.li server with a throttling service endpoint. */
  private final RestClient restClient;
  /** Identifier of the resource that is being limited. The throttling service has different throttlers for each resource. */
  private final String resourceLimited;
  /** Identifier for the service performing the permit request. */
  private final String serviceIdentifier;

  @Override
  public void start() {
    // Do nothing
  }

  @Override
  public Closeable acquirePermits(long permits) throws InterruptedException {

    try {
      PermitRequest permitRequest = new PermitRequest();
      permitRequest.setPermits(permits);
      permitRequest.setResource(this.resourceLimited);
      permitRequest.setRequestorIdentifier(this.serviceIdentifier);

      PermitsGetRequestBuilder getBuilder = new PermitsRequestBuilders().get();

      Request<PermitAllocation> request = getBuilder.id(new ComplexResourceKey<>(permitRequest, new EmptyRecord())).build();
      ResponseFuture<PermitAllocation> responseFuture = this.restClient.sendRequest(request);
      Response<PermitAllocation> response = responseFuture.getResponse();

      if (response.hasError() || response.getEntity().getPermits() < permits) {
        return null;
      }

      return new DummyCloseablePermit();
    } catch (RemoteInvocationException exc) {
      throw new RuntimeException("Could not acquire permits from remote server.", exc);
    }
  }

  @Override
  public void stop() {
    // Do nothing
  }
}
