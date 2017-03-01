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

package gobblin.restli.throttling;

import java.io.Closeable;
import java.io.IOException;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import gobblin.broker.SimpleScopeType;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.MetricContext;
import gobblin.metrics.broker.MetricContextFactory;
import gobblin.metrics.broker.SubTaggedMetricContextKey;
import gobblin.util.NoopCloseable;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;
import gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.extern.slf4j.Slf4j;

/**
 * Restli resource for allocating permits through Rest calls. Simply calls a {@link Limiter} in the server configured
 * through {@link SharedResourcesBroker}.
 */
@Slf4j
@RestLiCollection(name = "permits", namespace = "gobblin.restli.throttling")
public class LimiterServerResource extends ComplexKeyResourceTemplate<PermitRequest, EmptyRecord, PermitAllocation> {

  public static final String BROKER_INJECT_NAME = "broker";
  public static final String METRIC_CONTEXT_INJECT_NAME = "limiterResourceMetricContext";
  public static final String REQUEST_TIMER_INJECT_NAME = "limiterResourceRequestTimer";

  public static final String REQUEST_TIMER_NAME = "limiterServer.requestTimer";
  public static final String PERMITS_REQUESTED_METER_NAME = "limiterServer.permitsRequested";
  public static final String PERMITS_GRANTED_METER_NAME = "limiterServer.permitsGranted";
  public static final String LIMITER_TIMER_NAME = "limiterServer.limiterTimer";
  public static final String RESOURCE_ID_TAG = "resourceId";

  @Inject @javax.inject.Inject @javax.inject.Named(BROKER_INJECT_NAME)
  SharedResourcesBroker broker;

  @javax.inject.Inject @javax.inject.Named(METRIC_CONTEXT_INJECT_NAME)
  MetricContext metricContext;

  @javax.inject.Inject @javax.inject.Named(REQUEST_TIMER_INJECT_NAME)
  Timer requestTimer;

  /**
   * Request permits from the limiter server. The returned {@link PermitAllocation} specifies the number of permits
   * that the client can use.
   */
  @Override
  public PermitAllocation get(ComplexResourceKey<PermitRequest, EmptyRecord> key) {
    try (Closeable context = this.requestTimer == null ? NoopCloseable.INSTANCE : this.requestTimer.time()) {

      log.debug("Allocating request {}", key);

      PermitRequest request = key.getKey();
      String resourceId = request.getResource();

      MetricContext resourceContext = (MetricContext) broker.getSharedResource(new MetricContextFactory(),
          new SubTaggedMetricContextKey(resourceId, ImmutableMap.of(RESOURCE_ID_TAG, resourceId)));
      Meter permitsRequestedMeter = resourceContext.meter(PERMITS_REQUESTED_METER_NAME);
      Meter permitsGrantedMeter = resourceContext.meter(PERMITS_GRANTED_METER_NAME);
      Timer limiterTimer = resourceContext.timer(LIMITER_TIMER_NAME);

      permitsRequestedMeter.mark(request.getPermits());

      Limiter limiter = (Limiter) this.broker.getSharedResource(new SharedLimiterFactory<SimpleScopeType>(),
          new SharedLimiterKey(request.getResource(), SharedLimiterKey.GlobalLimiterPolicy.LOCAL_ONLY));

      Closeable permit;
      try (Closeable thisContext = limiterTimer.time()) {
        permit = limiter.acquirePermits(request.getPermits());
      }

      long permitsGranted = permit == null ? 0 : key.getKey().getPermits();
      permitsGrantedMeter.mark(permitsGranted);

      PermitAllocation allocation = new PermitAllocation();
      allocation.setPermits(permitsGranted);
      allocation.setExpiration(Long.MAX_VALUE);
      return allocation;
    } catch (NotConfiguredException nce) {
      throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, "No configuration for the requested resource.");
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    } catch (IOException ioe) {
      // Failed to close timer context. This should never happen
      throw new RuntimeException(ioe);
    }
  }
}
