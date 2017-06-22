package gobblin.http;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.restli.R2RestResponseHandler;
import gobblin.utils.HttpUtils;


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
  private static final Logger LOG = LoggerFactory.getLogger(R2RestResponseHandler.class);
  private final Set<String> errorCodeWhitelist;

  public ApacheHttpResponseHandler() {
    this(new HashSet<>());
  }

  public ApacheHttpResponseHandler(Set<String> errorCodeWhitelist) {
    this.errorCodeWhitelist = errorCodeWhitelist;
  }

  @Override
  public ApacheHttpResponseStatus handleResponse(CloseableHttpResponse response) {
    ApacheHttpResponseStatus status = new ApacheHttpResponseStatus(StatusType.OK);
    int statusCode = response.getStatusLine().getStatusCode();
    status.setStatusCode(statusCode);

    HttpUtils.updateStatusType(status, statusCode, errorCodeWhitelist);

    if (status.getType() == StatusType.OK) {
      status.setContent(getEntityAsByteArray(response.getEntity()));
      status.setContentType(response.getEntity().getContentType().getValue());
    } else {
      LOG.info("Receive an unsuccessful response with status code: " + statusCode);
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
