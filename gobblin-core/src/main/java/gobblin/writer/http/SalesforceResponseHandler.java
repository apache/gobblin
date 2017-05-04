package gobblin.writer.http;

import java.io.IOException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.extern.log4j.Log4j;

import gobblin.http.ResponseHandler;


@Log4j
public class SalesforceResponseHandler implements ResponseHandler<CloseableHttpResponse> {
  private static final String DUPLICATE_VALUE_ERR_CODE = "DUPLICATE_VALUE";

  private SalesforceClient client;
  private final SalesforceWriterBuilder.Operation operation;
  private final boolean isBatching;

  SalesforceResponseHandler(SalesforceClient client, SalesforceWriterBuilder.Operation operation, boolean isBatching) {
    this.client = client;
    this.operation = operation;
    this.isBatching = isBatching;
  }

  @Override
  public void handleResponse(CloseableHttpResponse response)
      throws IOException {
    log.debug("Received response " + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE));

    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == 401 || statusCode == 403) {
      client.setAccessToken(null);
      throw new RuntimeException(
          "Access denied. Access token has been reacquired and retry may solve the problem. " + ToStringBuilder
              .reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE));
    }

    if (isBatching) {
      processBatchRequestResponse(response);
    } else {
      processSingleRequestResponse(response);
    }
  }

  private void processSingleRequestResponse(CloseableHttpResponse response)
      throws IOException {
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode < 400) {
      return;
    }
    String entityStr = EntityUtils.toString(response.getEntity());
    if (statusCode == 400 && SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST.equals(operation)
        && entityStr != null) { //Ignore if it's duplicate entry error code

      JsonArray jsonArray = new JsonParser().parse(entityStr).getAsJsonArray();
      JsonObject jsonObject = jsonArray.get(0).getAsJsonObject();
      if (isDuplicate(jsonObject, statusCode)) {
        return;
      }
    }
    throw new IOException("Failed due to " + entityStr + " (Detail: " + ToStringBuilder
        .reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE) + " )");
  }

  /**
   * Check results from batch response, if any of the results is failure throw exception.
   * @param response
   * @throws IOException
   */
  private void processBatchRequestResponse(CloseableHttpResponse response)
      throws IOException {
    String entityStr = EntityUtils.toString(response.getEntity());
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode >= 400) {
      throw new RuntimeException("Failed due to " + entityStr + " (Detail: " + ToStringBuilder
          .reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE) + " )");
    }

    JsonObject jsonBody = new JsonParser().parse(entityStr).getAsJsonObject();
    if (!jsonBody.get("hasErrors").getAsBoolean()) {
      return;
    }

    JsonArray results = jsonBody.get("results").getAsJsonArray();
    for (JsonElement jsonElem : results) {
      JsonObject json = jsonElem.getAsJsonObject();
      int subStatusCode = json.get("statusCode").getAsInt();
      if (subStatusCode < 400) {
        continue;
      } else if (subStatusCode == 400 && SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST.equals(operation)) {
        JsonElement resultJsonElem = json.get("result");
        Preconditions.checkNotNull(resultJsonElem, "Error response should contain result property");
        JsonObject resultJsonObject = resultJsonElem.getAsJsonArray().get(0).getAsJsonObject();
        if (isDuplicate(resultJsonObject, subStatusCode)) {
          continue;
        }
      }
      throw new IOException("Failed due to " + jsonBody + " (Detail: " + ToStringBuilder
          .reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE) + " )");
    }
  }

  private boolean isDuplicate(JsonObject responseJsonObject, int statusCode) {
    return statusCode == 400 && SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST.equals(operation)
        && DUPLICATE_VALUE_ERR_CODE.equals(responseJsonObject.get("errorCode").getAsString());
  }
}
