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

package org.apache.gobblin.util.io;

import java.io.InputStream;
import java.net.URI;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.MultiLimiter;
import org.apache.gobblin.util.limiter.NoopLimiter;
import org.apache.gobblin.util.limiter.broker.SharedLimiterFactory;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;


/**
 * A class used to throttle {@link InputStream}s.
 * @param <S>
 */
@Slf4j
@AllArgsConstructor
public class StreamThrottler<S extends ScopeType<S>> {

  /**
   * A {@link SharedResourceFactory} that creates {@link StreamThrottler}.
   */
  public static class Factory<S extends ScopeType<S>> implements SharedResourceFactory<StreamThrottler<S>, EmptyKey, S> {
    public static final String NAME = "streamThrottler";

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public SharedResourceFactoryResponse<StreamThrottler<S>> createResource(SharedResourcesBroker<S> broker,
        ScopedConfigView<S, EmptyKey> config) throws NotConfiguredException {
      return new ResourceInstance<>(new StreamThrottler<>(broker));
    }

    @Override
    public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, EmptyKey> config) {
      return broker.selfScope().getType();
    }
  }

  private final SharedResourcesBroker<S> broker;

  /**
   * Throttles an {@link InputStream} if throttling is configured.
   * @param inputStream {@link InputStream} to throttle.
   * @param sourceURI used for selecting the throttling policy.
   * @param targetURI used for selecting the throttling policy.
   */
  @Builder(builderMethodName = "throttleInputStream", builderClassName = "InputStreamThrottler")
  private ThrottledInputStream doThrottleInputStream(InputStream inputStream, URI sourceURI, URI targetURI) {
    Preconditions.checkNotNull(inputStream, "InputStream cannot be null.");

    Limiter limiter = new NoopLimiter();
    if (sourceURI != null && targetURI != null) {
      StreamCopierSharedLimiterKey key = new StreamCopierSharedLimiterKey(sourceURI, targetURI);
      try {
        limiter = new MultiLimiter(limiter, this.broker.getSharedResource(new SharedLimiterFactory<S>(),
            key));
      } catch (NotConfiguredException nce) {
        log.warn("Could not create a Limiter for key " + key, nce);
      }
    } else {
      log.info("Not throttling input stream because source or target URIs are not defined.");
    }

    Optional<MeteredInputStream> meteredStream = MeteredInputStream.findWrappedMeteredInputStream(inputStream);
    if (!meteredStream.isPresent()) {
      meteredStream = Optional.of(MeteredInputStream.builder().in(inputStream).build());
      inputStream = meteredStream.get();
    }

    return new ThrottledInputStream(inputStream, limiter, meteredStream.get());
  }

}
