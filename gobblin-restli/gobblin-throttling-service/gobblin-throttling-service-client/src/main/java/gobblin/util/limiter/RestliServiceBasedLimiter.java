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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.restli.throttling.PermitAllocation;
import gobblin.restli.throttling.PermitRequest;
import gobblin.restli.throttling.PermitsGetRequestBuilder;
import gobblin.restli.throttling.PermitsRequestBuilders;
import gobblin.util.NoopCloseable;
import java.io.Closeable;
import java.io.IOException;
import lombok.Builder;


/**
 * A {@link Limiter} that forwards permit requests to a Rest.li throttling service endpoint.
 */
public class RestliServiceBasedLimiter implements Limiter {

  public static final String REQUEST_TIMER_NAME = "limiter.restli.requestTimer";
  public static final String PERMITS_REQUESTED_METER_NAME = "limiter.restli.permitsRequested";
  public static final String PERMITS_GRANTED_METER_NAME = "limiter.restli.permitsGranted";

  /** {@link RestClient} pointing to a Rest.li server with a throttling service endpoint. */
  private final RestClient restClient;
  /** Identifier of the resource that is being limited. The throttling service has different throttlers for each resource. */
  private final String resourceLimited;
  /** Identifier for the service performing the permit request. */
  private final String serviceIdentifier;

  private final Optional<MetricContext> metricContext;

  private final Optional<Timer> requestTimer;
  private final Optional<Meter> permitsRequestedMeter;
  private final Optional<Meter> permitsGrantedMeter;

  @Builder
  private RestliServiceBasedLimiter(RestClient restClient, String resourceLimited, String serviceIdentifier,
      MetricContext metricContext) {
    this.restClient = restClient;
    this.resourceLimited = resourceLimited;
    this.serviceIdentifier = serviceIdentifier;
    this.metricContext = Optional.fromNullable(metricContext);
    if (this.metricContext.isPresent()) {
      this.requestTimer = Optional.of(this.metricContext.get().timer(REQUEST_TIMER_NAME));
      this.permitsRequestedMeter = Optional.of(this.metricContext.get().meter(PERMITS_REQUESTED_METER_NAME));
      this.permitsGrantedMeter = Optional.of(this.metricContext.get().meter(PERMITS_GRANTED_METER_NAME));
    } else {
      this.requestTimer = Optional.absent();
      this.permitsRequestedMeter = Optional.absent();
      this.permitsGrantedMeter = Optional.absent();
    }
  }

  @Override
  public void start() {
    // Do nothing
  }

  @Override
  public Closeable acquirePermits(long permits) throws InterruptedException {

    Instrumented.markMeter(this.permitsRequestedMeter, permits);

    try(Closeable context = this.requestTimer.isPresent() ? this.requestTimer.get().time() : NoopCloseable.INSTANCE) {
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

      Instrumented.markMeter(this.permitsGrantedMeter, permits);
      return new DummyCloseablePermit();
    } catch (RemoteInvocationException exc) {
      throw new RuntimeException("Could not acquire permits from remote server.", exc);
    } catch (IOException ioe) {
      // This should never happen
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void stop() {
    // Do nothing
  }
}
