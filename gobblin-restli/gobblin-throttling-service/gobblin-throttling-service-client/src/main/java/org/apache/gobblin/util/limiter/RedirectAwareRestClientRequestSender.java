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

package org.apache.gobblin.util.limiter;

import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.callback.Callback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.RetriableRequestException;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;

import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.restli.SharedRestClientFactory;
import org.apache.gobblin.restli.SharedRestClientKey;
import org.apache.gobblin.restli.UriRestClientKey;
import org.apache.gobblin.restli.throttling.PermitAllocation;
import org.apache.gobblin.restli.throttling.PermitRequest;
import org.apache.gobblin.util.ExponentialBackoff;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestSender} that handles redirects and unreachable uris transparently.
 */
@Slf4j
public class RedirectAwareRestClientRequestSender extends RestClientRequestSender {

  private static final int MIN_RETRIES = 3;

  /**
   * A {@link SharedResourceFactory} that creates {@link RedirectAwareRestClientRequestSender}s.
   * @param <S>
   */
  public static class Factory<S extends ScopeType<S>> implements SharedResourceFactory<RequestSender, SharedRestClientKey, S> {
    @Override
    public String getName() {
      return SharedRestClientFactory.FACTORY_NAME;
    }

    @Override
    public SharedResourceFactoryResponse<RequestSender> createResource(
        SharedResourcesBroker<S> broker, ScopedConfigView<S, SharedRestClientKey> config)
        throws NotConfiguredException {
      try {
        List<String> connectionPrefixes = SharedRestClientFactory.parseConnectionPrefixes(config.getConfig(), config.getKey());

        return new ResourceInstance<>(
            new RedirectAwareRestClientRequestSender(broker, connectionPrefixes));
      } catch (URISyntaxException use) {
        throw new RuntimeException(use);
      }
    }

    @Override
    public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedRestClientKey> config) {
      return broker.selfScope().getType().rootScope();
    }
  }

  private final SharedResourcesBroker<?> broker;
  private final List<String> connectionPrefixes;

  private volatile int lastPrefixAttempted = -1;
  private volatile RestClient restClient;
  @Getter
  private volatile String currentServerPrefix;

  private String lastLogPrefix = "";
  private AtomicInteger requestsSinceLastLog = new AtomicInteger(0);
  private long lastLogTimeNanos = 0;

  /**
   * @param broker {@link SharedResourcesBroker} used to create {@link RestClient}s.
   * @param connectionPrefixes List of uri prefixes of available servers.
   * @throws NotConfiguredException
   */
  public RedirectAwareRestClientRequestSender(SharedResourcesBroker<?> broker, List<String> connectionPrefixes)
      throws NotConfiguredException {
    this.broker = broker;
    this.connectionPrefixes = connectionPrefixes;
    updateRestClient(getNextConnectionPrefix(), "service start", null);
  }

  private String getNextConnectionPrefix() {
    if (this.lastPrefixAttempted < 0) {
      this.lastPrefixAttempted = new Random().nextInt(this.connectionPrefixes.size());
    }
    this.lastPrefixAttempted = (this.lastPrefixAttempted + 1) % this.connectionPrefixes.size();
    log.info("Round robin: " + this.lastPrefixAttempted);
    return this.connectionPrefixes.get(this.lastPrefixAttempted);
  }

  @Override
  public void sendRequest(PermitRequest request, Callback<Response<PermitAllocation>> callback) {
    logRequest();
    super.sendRequest(request, callback);
  }

  private void logRequest() {
    String prefix = getCurrentServerPrefix();

    if (!prefix.equals(this.lastLogPrefix)) {
      logAggregatedRequests(this.lastLogPrefix);
      log.info("Sending request to " + prefix);
      this.lastLogPrefix = prefix;
      return;
    }

    this.requestsSinceLastLog.incrementAndGet();
    log.debug("Sending request to {}", prefix);

    if (TimeUnit.SECONDS.convert(System.nanoTime() - this.lastLogTimeNanos, TimeUnit.NANOSECONDS) > 60) { // 1 minute
      logAggregatedRequests(prefix);
    }
  }

  private void logAggregatedRequests(String prefix) {
    int requests = this.requestsSinceLastLog.getAndSet(0);
    long time = System.nanoTime();
    long elapsedMillis = TimeUnit.MILLISECONDS.convert(time - this.lastLogTimeNanos, TimeUnit.NANOSECONDS);
    this.lastLogTimeNanos = time;

    if (requests > 0) {
      log.info(String.format("Made %d requests to %s over the last %d millis.", requests, prefix, elapsedMillis));
    }
  }

  @Override
  protected RestClient getRestClient() {
    return this.restClient;
  }

  @Override
  protected Callback<Response<PermitAllocation>> decorateCallback(PermitRequest request,
      Callback<Response<PermitAllocation>> callback) {
    if (callback instanceof CallbackDecorator) {
      return callback;
    }
    return new CallbackDecorator(request, callback);
  }

  @VisibleForTesting
  void updateRestClient(String uri, String reason, Throwable errorCause) throws NotConfiguredException {
    if (errorCause == null) {
      log.info(String.format("Switching to server prefix %s due to: %s", uri, reason));
    } else {
      log.error(String.format("Switching to server prefix %s due to: %s", uri, reason), errorCause);
    }
    this.currentServerPrefix = uri;
    this.restClient = (RestClient) this.broker.getSharedResource(new SharedRestClientFactory(),
          new UriRestClientKey(RestliLimiterFactory.RESTLI_SERVICE_NAME, uri));
  }

  /**
   * A {@link Callback} decorator that intercepts certain errors (301 redirects and {@link ConnectException}s) and
   * retries transparently.
   */
  @RequiredArgsConstructor
  private class CallbackDecorator implements Callback<Response<PermitAllocation>> {
    private final PermitRequest originalRequest;
    private final Callback<Response<PermitAllocation>> underlying;
    private final ExponentialBackoff exponentialBackoff = ExponentialBackoff.builder().maxDelay(10000L).initialDelay(500L).build();
    private int redirects = 0;
    private int retries = 0;

    @Override
    public void onError(Throwable error) {
      try {
        if (error instanceof RestLiResponseException &&
            ((RestLiResponseException) error).getStatus() == HttpStatus.S_301_MOVED_PERMANENTLY.getCode()) {
          this.redirects++;
          if (this.redirects >= 5) {
            this.underlying.onError(new NonRetriableException("Too many redirects."));
          }
          RestLiResponseException responseExc = (RestLiResponseException) error;
          String newUri = (String) responseExc.getErrorDetails().get("Location");
          RedirectAwareRestClientRequestSender.this.updateRestClient(
              SharedRestClientFactory.resolveUriPrefix(new URI(newUri)), "301 redirect", null);
          this.exponentialBackoff.awaitNextRetry();
          sendRequest(this.originalRequest, this);
        } else if (error instanceof RemoteInvocationException
            && shouldCatchExceptionAndSwitchUrl((RemoteInvocationException) error)) {
          this.retries++;
          if (this.retries > RedirectAwareRestClientRequestSender.this.connectionPrefixes.size() + MIN_RETRIES) {
            this.underlying.onError(new NonRetriableException("Failed to connect to all available connection prefixes.", error));
          }
          updateRestClient(getNextConnectionPrefix(), "Failed to communicate with " + getCurrentServerPrefix(), error);
          this.exponentialBackoff.awaitNextRetry();
          sendRequest(this.originalRequest, this);
        } else {
          this.underlying.onError(error);
        }
      } catch (Throwable t) {
        this.underlying.onError(t);
      }
    }

    @Override
    public void onSuccess(Response<PermitAllocation> result) {
      this.underlying.onSuccess(result);
    }
  }

  public boolean shouldCatchExceptionAndSwitchUrl(RemoteInvocationException exc) {
    return exc.getCause() instanceof RetriableRequestException || exc.getCause() instanceof ConnectException
        || exc.getCause() instanceof TimeoutException;
  }
}
