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
import java.net.URI;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.data.DataMap;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.CallbackParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.ComplexKeyResourceAsyncTemplate;

import gobblin.annotation.Alpha;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.MetricContext;
import gobblin.metrics.broker.MetricContextFactory;
import gobblin.metrics.broker.SubTaggedMetricContextKey;
import gobblin.util.NoopCloseable;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.broker.SharedLimiterKey;

import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/**
 * Restli resource for allocating permits through Rest calls. Simply calls a {@link Limiter} in the server configured
 * through {@link SharedResourcesBroker}.
 */
@Alpha
@Slf4j
@RestLiCollection(name = "permits", namespace = "gobblin.restli.throttling")
public class LimiterServerResource extends ComplexKeyResourceAsyncTemplate<PermitRequest, EmptyRecord, PermitAllocation> {

  public static final long TIMEOUT_MILLIS = 7000; // resli client times out after 10 seconds

  public static final String BROKER_INJECT_NAME = "broker";
  public static final String METRIC_CONTEXT_INJECT_NAME = "limiterResourceMetricContext";
  public static final String REQUEST_TIMER_INJECT_NAME = "limiterResourceRequestTimer";
  public static final String LEADER_FINDER_INJECT_NAME = "leaderFinder";

  public static final String REQUEST_TIMER_NAME = "limiterServer.requestTimer";
  public static final String PERMITS_REQUESTED_METER_NAME = "limiterServer.permitsRequested";
  public static final String PERMITS_GRANTED_METER_NAME = "limiterServer.permitsGranted";
  public static final String LIMITER_TIMER_NAME = "limiterServer.limiterTimer";
  public static final String RESOURCE_ID_TAG = "resourceId";
  public static final String LOCATION_301 = "Location";

  @Inject @Named(BROKER_INJECT_NAME)
  SharedResourcesBroker broker;

  @Inject @Named(METRIC_CONTEXT_INJECT_NAME)
  MetricContext metricContext;

  @Inject @Named(REQUEST_TIMER_INJECT_NAME)
  Timer requestTimer;

  @Inject @Named(LEADER_FINDER_INJECT_NAME)
  Optional<LeaderFinder<URIMetadata>> leaderFinderOpt;

  /**
   * Request permits from the limiter server. The returned {@link PermitAllocation} specifies the number of permits
   * that the client can use.
   */
  @Override
  @RestMethod.Get
  public void get(
      ComplexResourceKey<PermitRequest, EmptyRecord> key,
      @CallbackParam final Callback<PermitAllocation> callback) {
    try (Closeable context = this.requestTimer == null ? NoopCloseable.INSTANCE : this.requestTimer.time()) {

      PermitRequest request = key.getKey();
      String resourceId = request.getResource();

      MetricContext resourceContext = (MetricContext) broker.getSharedResource(new MetricContextFactory(),
          new SubTaggedMetricContextKey(resourceId, ImmutableMap.of(RESOURCE_ID_TAG, resourceId)));
      Meter permitsRequestedMeter = resourceContext.meter(PERMITS_REQUESTED_METER_NAME);
      Meter permitsGrantedMeter = resourceContext.meter(PERMITS_GRANTED_METER_NAME);
      Timer limiterTimer = resourceContext.timer(LIMITER_TIMER_NAME);

      permitsRequestedMeter.mark(request.getPermits());

      if (this.leaderFinderOpt.isPresent() && !this.leaderFinderOpt.get().isLeader()) {
        URI leaderUri = this.leaderFinderOpt.get().getLeaderMetadata().getUri();

        RestLiServiceException exception = new RestLiServiceException(HttpStatus.S_301_MOVED_PERMANENTLY,
            String.format("New leader <a href=\"%s\">%s</a>", leaderUri, leaderUri));
        exception.setErrorDetails(new DataMap(ImmutableMap.of(LOCATION_301, leaderUri.toString())));
        throw exception;
      } else {
        ThrottlingPolicy policy = (ThrottlingPolicy) this.broker.getSharedResource(new ThrottlingPolicyFactory(),
            new SharedLimiterKey(request.getResource()));

        PermitAllocation allocation;
        try (Closeable thisContext = limiterTimer.time()) {
          allocation = policy.computePermitAllocation(request);
        }
        permitsGrantedMeter.mark(allocation.getPermits());

        callback.onSuccess(allocation);
      }


    } catch (NotConfiguredException nce) {
      throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, "No configuration for the requested resource.");
    } catch (IOException ioe) {
      // Failed to close timer context. This should never happen
      throw new RuntimeException(ioe);
    }

  }

  /**
   * Request permits from the limiter server. The returned {@link PermitAllocation} specifies the number of permits
   * that the client can use.
   */
  public PermitAllocation getSync(ComplexResourceKey<PermitRequest, EmptyRecord> key) {
    try {
      FutureCallback<PermitAllocation> callback = new FutureCallback<>();

      get(key, callback);
      return callback.get();
    } catch (ExecutionException ee) {
      Throwable t = ee.getCause();
      if (t instanceof RestLiServiceException) {
        throw (RestLiServiceException) t;
      } else {
        throw new RuntimeException(t);
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
