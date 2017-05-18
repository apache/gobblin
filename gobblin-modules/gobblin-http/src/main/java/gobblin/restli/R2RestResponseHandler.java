package gobblin.restli;

import com.linkedin.r2.message.rest.RestResponse;

import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
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
public class R2RestResponseHandler implements ResponseHandler<RestResponse> {

  @Override
  public ResponseStatus handleResponse(RestResponse response) {
    ResponseStatus status = new ResponseStatus(StatusType.OK);

    int statusCode = response.getStatus();
    if (statusCode > 300 & statusCode < 500) {
      status.setType(StatusType.CLIENT_ERROR);
    } else if (statusCode >= 500) {
      status.setType(StatusType.SERVER_ERROR);
    }

    return status;
  }
}
