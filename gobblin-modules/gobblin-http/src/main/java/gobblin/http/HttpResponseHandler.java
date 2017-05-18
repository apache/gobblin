package gobblin.http;

import org.apache.http.client.methods.CloseableHttpResponse;


/**
 * Basic logic to handle a {@link CloseableHttpResponse} from a http service
 *
 * <p>
 *   A more specific handler understands the content inside the response and is able to customize
 *   the behavior as needed. For example: parsing the entity from a get response, extracting data
 *   sent from the service for a post response, executing more detailed status code handling, etc.
 * </p>
 */
public class HttpResponseHandler implements ResponseHandler<CloseableHttpResponse> {

  @Override
  public ResponseStatus handleResponse(CloseableHttpResponse response) {
    ResponseStatus status = new ResponseStatus(StatusType.OK);

    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode > 300 & statusCode < 500) {
      status.setType(StatusType.CLIENT_ERROR);
    } else if (statusCode >= 500) {
      status.setType(StatusType.SERVER_ERROR);
    }

    return status;
  }
}
