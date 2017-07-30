package gobblin.r2;

import com.google.common.collect.Maps;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import gobblin.configuration.State;
import gobblin.http.ResponseHandler;
import gobblin.http.StatusType;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.net.Request;
import gobblin.utils.HttpConstants;
import gobblin.utils.HttpUtils;
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
  private final String R2_FAILED_REQUEST = "R2FailedRequest";
  private final Set<String> errorCodeWhitelist;
  MetricContext metricsContext;
  EventSubmitter eventSubmitter;

  public R2RestResponseHandler() {
    this(new HashSet<>(), Instrumented.getMetricContext(new State(), R2RestResponseHandler.class));
  }

  public R2RestResponseHandler(Set<String> errorCodeWhitelist, MetricContext metricContext) {
    this.errorCodeWhitelist = errorCodeWhitelist;
    this.metricsContext = metricContext;
    eventSubmitter = new EventSubmitter.Builder(metricsContext, R2_RESPONSE_EVENT_NAMESPACE).build();
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
      Map<String, String> metadata = Maps.newHashMap();
      metadata.put(HttpConstants.REQUEST, request.toString());
      metadata.put(HttpConstants.STATUS_CODE, String.valueOf(statusCode));
      eventSubmitter.submit(R2_FAILED_REQUEST, metadata);
      log.info("Receive an unsuccessful response with status code: " + statusCode);
    }

    return status;
  }
}
