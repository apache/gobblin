package gobblin.restli;

import com.linkedin.r2.message.rest.RestResponse;

import gobblin.http.HttpResponseHandler;
import gobblin.http.HttpResponseStatus;
import gobblin.http.StatusType;


/**
 * Basic logic to handle a {@link RestResponse} from a restli service
 *
 * <p>
 *   A more specific handler understands the content inside the response and is able to customize
 *   the behavior as needed. For example: parsing the entity from a get response, extracting data
 *   sent from the service for a post response, executing more detailed status code handling, etc.
 * </p>
 */
public class R2RestResponseHandler implements HttpResponseHandler<RestResponse> {

  @Override
  public HttpResponseStatus handleResponse(RestResponse response) {
    HttpResponseStatus status = new HttpResponseStatus(StatusType.OK);
    int statusCode = response.getStatus();

    status.setStatusCode(statusCode);
    status.setContent(response.getEntity().asByteBuffer().array());
    status.setContentType(response.getHeader("Content-Type"));

    if (statusCode > 300 & statusCode < 500) {
      status.setType(StatusType.CLIENT_ERROR);
    } else if (statusCode >= 500) {
      status.setType(StatusType.SERVER_ERROR);
    }

    return status;
  }
}
