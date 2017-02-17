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

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
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

  @Inject @javax.inject.Inject @javax.inject.Named(BROKER_INJECT_NAME)
  SharedResourcesBroker broker;

  /**
   * Request permits from the limiter server. The returned {@link PermitAllocation} specifies the number of permits
   * that the client can use.
   */
  @Override
  public PermitAllocation get(ComplexResourceKey<PermitRequest, EmptyRecord> key) {
    try {

      log.debug("Allocating request {}", key);
      PermitRequest request = key.getKey();

      Limiter limiter = (Limiter) this.broker.<Limiter, SharedLimiterKey>getSharedResource(new SharedLimiterFactory(),
          new SharedLimiterKey(request.getResource(), SharedLimiterKey.GlobalLimiterPolicy.LOCAL_ONLY));

      Closeable permit = limiter.acquirePermits(request.getPermits());

      PermitAllocation allocation = new PermitAllocation();
      allocation.setPermits(permit == null ? 0 : key.getKey().getPermits());
      allocation.setExpiration(Long.MAX_VALUE);
      return allocation;
    } catch (NotConfiguredException nce) {
      throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, "No configuration for the requested resource.");
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
