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
import com.linkedin.restli.client.RestClient;

import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.util.NoopCloseable;
import java.io.Closeable;
import java.io.IOException;
import lombok.Builder;


/**
 * A {@link Limiter} that forwards permit requests to a Rest.li throttling service endpoint.
 */
public class RestliServiceBasedLimiter implements Limiter {

  public static final String PERMITS_REQUESTED_METER_NAME = "limiter.restli.permitsRequested";
  public static final String PERMITS_GRANTED_METER_NAME = "limiter.restli.permitsGranted";

  private final BatchedPermitsRequester bachedPermitsContainer;

  private final Optional<MetricContext> metricContext;

  private final Optional<Meter> permitsRequestedMeter;
  private final Optional<Meter> permitsGrantedMeter;

  @Builder
  private RestliServiceBasedLimiter(RestClient restClient, String resourceLimited, String serviceIdentifier,
      MetricContext metricContext) {
    this.bachedPermitsContainer = BatchedPermitsRequester.builder().restClient(restClient).resourceId(resourceLimited)
        .requestorIdentifier(serviceIdentifier).build();

    this.metricContext = Optional.fromNullable(metricContext);
    if (this.metricContext.isPresent()) {
      this.permitsRequestedMeter = Optional.of(this.metricContext.get().meter(PERMITS_REQUESTED_METER_NAME));
      this.permitsGrantedMeter = Optional.of(this.metricContext.get().meter(PERMITS_GRANTED_METER_NAME));
    } else {
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

    boolean permitsGranted = this.bachedPermitsContainer.getPermits(permits);
    Instrumented.markMeter(this.permitsGrantedMeter, permits);
    return permitsGranted ? NoopCloseable.INSTANCE : null;
  }

  @Override
  public void stop() {
    // Do nothing
  }
}
