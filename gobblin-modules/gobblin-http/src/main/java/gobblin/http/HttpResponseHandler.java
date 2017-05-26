package gobblin.http;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOG = LoggerFactory.getLogger(HttpResponseHandler.class);

  @Override
  public ResponseStatus handleResponse(CloseableHttpResponse response) {
    ResponseStatus status = new ResponseStatus(StatusType.OK);
    int statusCode = response.getStatusLine().getStatusCode();
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      try {
        EntityUtils.consume(response.getEntity());
        /*
         * TODO process entityStr
         * String entityStr = EntityUtils.toString(response.getEntity());
         */
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (statusCode > 300 & statusCode < 500) {
      status.setType(StatusType.CLIENT_ERROR);
    } else if (statusCode >= 500) {
      status.setType(StatusType.SERVER_ERROR);
    }

    return status;
  }
}
