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
package org.apache.gobblin.r2;

import com.google.common.collect.Maps;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.http.ResponseHandler;
import org.apache.gobblin.http.StatusType;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.FailureEventBuilder;
import org.apache.gobblin.net.Request;
import org.apache.gobblin.utils.HttpConstants;
import org.apache.gobblin.utils.HttpUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;


/**
 * Basic logic to handle a {@link RestResponse} from a restli service
 *
 * <p>
 *   A more specific handler understands the content inside the response and is able to customize
 *   the behavior as needed. For example: parsing the entity from a get response, extracting data
 *   sent from the service for a post response, executing more detailed status code handling, etc.
 * </p>
 */
@Slf4j
public class R2RestResponseHandler implements ResponseHandler<RestRequest, RestResponse> {

  public static final String CONTENT_TYPE_HEADER = "Content-Type";
  private final String R2_RESPONSE_EVENT_NAMESPACE = "r2.response";
  private final String R2_FAILED_REQUEST_EVENT = "r2FailedRequest";
  private final Set<String> errorCodeWhitelist;
  private MetricContext metricsContext;

  public R2RestResponseHandler() {
    this(new HashSet<>(), Instrumented.getMetricContext(new State(), R2RestResponseHandler.class));
  }

  public R2RestResponseHandler(Set<String> errorCodeWhitelist, MetricContext metricContext) {
    this.errorCodeWhitelist = errorCodeWhitelist;
    this.metricsContext = metricContext;
  }

  @Override
  public R2ResponseStatus handleResponse(Request<RestRequest> request, RestResponse response) {
    R2ResponseStatus status = new R2ResponseStatus(StatusType.OK);
    int statusCode = response.getStatus();
    status.setStatusCode(statusCode);
    HttpUtils.updateStatusType(status, statusCode, errorCodeWhitelist);

    if (status.getType() == StatusType.OK) {
      status.setContent(response.getEntity());
      status.setContentType(response.getHeader(CONTENT_TYPE_HEADER));
    } else {
      log.info("Receive an unsuccessful response with status code: " + statusCode);

      Map<String, String> metadata = Maps.newHashMap();
      metadata.put(HttpConstants.STATUS_CODE, String.valueOf(statusCode));
      metadata.put(HttpConstants.REQUEST, request.toString());
      if (status.getType() != StatusType.CONTINUE) {
        FailureEventBuilder failureEvent = new FailureEventBuilder(R2_FAILED_REQUEST_EVENT);
        failureEvent.addAdditionalMetadata(metadata);
        failureEvent.submit(metricsContext);
      } else {
        GobblinTrackingEvent event =
            new GobblinTrackingEvent(0L, R2_RESPONSE_EVENT_NAMESPACE, R2_FAILED_REQUEST_EVENT, metadata);
        metricsContext.submitEvent(event);
      }
    }

    return status;
  }
}
