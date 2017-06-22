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

import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.callback.Callback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.RetriableRequestException;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;

import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceFactoryResponse;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.restli.SharedRestClientFactory;
import gobblin.restli.SharedRestClientKey;
import gobblin.restli.UriRestClientKey;
import gobblin.restli.throttling.PermitAllocation;
import gobblin.restli.throttling.PermitRequest;
import gobblin.util.ExponentialBackoff;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestSender} that handles redirects and unreachable uris transparently.
 */
@Slf4j
public class RedirectAwareRestClientRequestSender extends RestClientRequestSender {

  /**
   * A {@link SharedResourceFactory} that creates {@link RedirectAwareRestClientRequestSender}s.
   * @param <S>
   */
  public static class Factory<S extends ScopeType<S>> implements SharedResourceFactory<RedirectAwareRestClientRequestSender, SharedRestClientKey, S> {
    @Override
    public String getName() {
      return SharedRestClientFactory.FACTORY_NAME;
    }

    @Override
    public SharedResourceFactoryResponse<RedirectAwareRestClientRequestSender> createResource(
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

  /**
   * @param broker {@link SharedResourcesBroker} used to create {@link RestClient}s.
   * @param connectionPrefixes List of uri prefixes of available servers.
   * @throws NotConfiguredException
   */
  public RedirectAwareRestClientRequestSender(SharedResourcesBroker<?> broker, List<String> connectionPrefixes)
      throws NotConfiguredException {
    this.broker = broker;
    this.connectionPrefixes = connectionPrefixes;
    updateRestClient(getNextConnectionPrefix(), "service start");
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
    log.info("Sending request to " + getCurrentServerPrefix());
    super.sendRequest(request, callback);
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
  void updateRestClient(String uri, String reason) throws NotConfiguredException {
    log.info(String.format("Switching to server prefix %s due to: %s", uri, reason));
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
    private final ExponentialBackoff exponentialBackoff = ExponentialBackoff.builder().maxDelay(10000L).build();
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
              SharedRestClientFactory.resolveUriPrefix(new URI(newUri)), "301 redirect");
          this.exponentialBackoff.awaitNextRetry();
          sendRequest(this.originalRequest, this);
        } else if (error instanceof RemoteInvocationException
            && shouldCatchExceptionAndSwitchUrl((RemoteInvocationException) error)) {
          this.retries++;
          if (this.retries > RedirectAwareRestClientRequestSender.this.connectionPrefixes.size()) {
            this.underlying.onError(new NonRetriableException("Failed to connect to all available connection prefixes."));
          }
          log.info("Retries " + this.retries + " this " + hashCode());
          updateRestClient(getNextConnectionPrefix(), "Failed to communicate with " + getCurrentServerPrefix());
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
