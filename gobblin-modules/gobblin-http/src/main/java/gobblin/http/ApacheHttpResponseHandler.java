package gobblin.http;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Basic logic to handle a {@link CloseableHttpResponse} from a http service
 *
 * <p>
 *   A more specific handler understands the content inside the response and is able to customize
 *   the behavior as needed. For example: parsing the entity from a get response, extracting data
 *   sent from the service for a post response, executing more detailed status code handling, etc.
 * </p>
 */
@Slf4j
public class ApacheHttpResponseHandler implements ResponseHandler<CloseableHttpResponse> {

  @Override
  public ApacheHttpResponseStatus handleResponse(CloseableHttpResponse response) {
    ApacheHttpResponseStatus status = new ApacheHttpResponseStatus(StatusType.OK);
    int statusCode = response.getStatusLine().getStatusCode();
    status.setStatusCode(statusCode);

    if (statusCode >= 300 & statusCode < 500) {
      status.setType(StatusType.CLIENT_ERROR);
    } else if (statusCode >= 500) {
      status.setType(StatusType.SERVER_ERROR);
    }

    if (status.getType() == StatusType.OK) {
      status.setContent(getEntityAsByteArray(response.getEntity()));
      status.setContentType(response.getEntity().getContentType().getValue());
    }

    HttpEntity entity = response.getEntity();
    if (entity != null) {
      consumeEntity(entity);
    }

    return status;
  }

  private byte[] getEntityAsByteArray(HttpEntity entity) {
    try {
      return EntityUtils.toByteArray(entity);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void consumeEntity(HttpEntity entity) {
    try {
      EntityUtils.consume(entity);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
