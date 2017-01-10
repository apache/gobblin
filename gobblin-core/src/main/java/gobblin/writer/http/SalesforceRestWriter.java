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
package gobblin.writer.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.converter.http.RestEntry;
import gobblin.writer.exception.NonTransientException;

/**
 * Writes to Salesforce via RESTful API, supporting INSERT_ONLY_NOT_EXIST, and UPSERT.
 *
 */
public class SalesforceRestWriter extends RestJsonWriter {
  public static enum Operation {
    INSERT_ONLY_NOT_EXIST,
    UPSERT
  }
  static final String DUPLICATE_VALUE_ERR_CODE = "DUPLICATE_VALUE";

  private String accessToken;

  private final URI oauthEndPoint;
  private final String clientId;
  private final String clientSecret;
  private final String userId;
  private final String password;
  private final String securityToken;
  private final Operation operation;

  private final int batchSize;
  private final Optional<String> batchResourcePath;
  private Optional<JsonArray> batchRecords = Optional.absent();
  private long numRecordsWritten = 0L;

  public SalesforceRestWriter(SalesForceRestWriterBuilder builder) {
    super(builder);

    this.oauthEndPoint = builder.getSvcEndpoint().get(); //Set oauth end point
    this.clientId = builder.getClientId();
    this.clientSecret = builder.getClientSecret();
    this.userId = builder.getUserId();
    this.password = builder.getPassword();
    this.securityToken = builder.getSecurityToken();
    this.operation = builder.getOperation();
    this.batchSize = builder.getBatchSize();
    this.batchResourcePath = builder.getBatchResourcePath();
    Preconditions.checkArgument(batchSize == 1 || batchResourcePath.isPresent(), "Batch resource path is missing");
    if (batchSize > 1) {
      getLog().info("Batch api will be used with batch size " + batchSize);
    }
    try {
      onConnect(oauthEndPoint);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  SalesforceRestWriter(SalesForceRestWriterBuilder builder, String accessToken) {
    super(builder);

    this.oauthEndPoint = builder.getSvcEndpoint().get(); //Set oauth end point
    this.clientId = builder.getClientId();
    this.clientSecret = builder.getClientSecret();
    this.userId = builder.getUserId();
    this.password = builder.getPassword();
    this.securityToken = builder.getSecurityToken();
    this.operation = builder.getOperation();
    this.batchSize = builder.getBatchSize();
    this.batchResourcePath = builder.getBatchResourcePath();
    Preconditions.checkArgument(batchSize == 1 || batchResourcePath.isPresent(), "Batch resource path is missing");

    this.accessToken = accessToken;
  }

  /**
   * Retrieve access token, if needed, retrieve instance url, and set server host URL
   * {@inheritDoc}
   * @see gobblin.writer.http.HttpWriter#onConnect(org.apache.http.HttpHost)
   */
  @Override
  public void onConnect(URI serverHost) throws IOException {
    if (!StringUtils.isEmpty(accessToken)) {
      return; //No need to be called if accessToken is active.
    }

    try {
      getLog().info("Getting Oauth2 access token.");
      OAuthClientRequest request = OAuthClientRequest.tokenLocation(serverHost.toString())
          .setGrantType(GrantType.PASSWORD)
          .setClientId(clientId)
          .setClientSecret(clientSecret)
          .setUsername(userId)
          .setPassword(password + securityToken).buildQueryMessage();
      OAuthClient client = new OAuthClient(new URLConnectionClient());
      OAuthJSONAccessTokenResponse response = client.accessToken(request, OAuth.HttpMethod.POST);

      accessToken = response.getAccessToken();
      setCurServerHost(new URI(response.getParam("instance_url")));
    } catch (OAuthProblemException e) {
      throw new NonTransientException("Error while authenticating with Oauth2", e);
    } catch (OAuthSystemException e) {
      throw new RuntimeException("Failed getting access token", e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed due to invalid instance url", e);
    }
  }

  /**
   * For single request, creates HttpUriRequest and decides post/patch operation based on input parameter.
   *
   * For batch request, add the record into JsonArray as a subrequest and only creates HttpUriRequest with POST method if it filled the batch size.
   * {@inheritDoc}
   * @see gobblin.writer.http.RestJsonWriter#onNewRecord(gobblin.converter.rest.RestEntry)
   */
  @Override
  public Optional<HttpUriRequest> onNewRecord(RestEntry<JsonObject> record) {
    Preconditions.checkArgument(!StringUtils.isEmpty(accessToken), "Access token has not been acquired.");
    Preconditions.checkNotNull(record, "Record should not be null");

    RequestBuilder builder = null;
    JsonObject payload = null;

    if (batchSize > 1) {
      if (!batchRecords.isPresent()) {
        batchRecords = Optional.of(new JsonArray());
      }
      batchRecords.get().add(newSubrequest(record));

      if (batchRecords.get().size() < batchSize) { //No need to send. Return absent.
        return Optional.absent();
      }

      payload = newPayloadForBatch();
      builder = RequestBuilder.post().setUri(combineUrl(getCurServerHost(), batchResourcePath));
    } else {
      switch (operation) {
        case INSERT_ONLY_NOT_EXIST:
          builder = RequestBuilder.post();
          break;
        case UPSERT:
          builder = RequestBuilder.patch();
          break;
        default:
          throw new IllegalArgumentException(operation + " is not supported.");
      }
      builder.setUri(combineUrl(getCurServerHost(), record.getResourcePath()));
      payload = record.getRestEntryVal();
    }
    return Optional.of(newRequest(builder, payload));
  }

  /**
   * Create batch subrequest. For more detail @link https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/requests_composite_batch.htm
   *
   * @param record
   * @return
   */
  private JsonObject newSubrequest(RestEntry<JsonObject> record) {
    Preconditions.checkArgument(record.getResourcePath().isPresent(), "Resource path is not defined");
    JsonObject subReq = new JsonObject();
    subReq.addProperty("url", record.getResourcePath().get());
    subReq.add("richInput", record.getRestEntryVal());

    switch (operation) {
      case INSERT_ONLY_NOT_EXIST:
        subReq.addProperty("method", "POST");
        break;
      case UPSERT:
        subReq.addProperty("method", "PATCH");
        break;
      default:
        throw new IllegalArgumentException(operation + " is not supported.");
    }
    return subReq;
  }
  /**
   * @return JsonObject contains batch records
   */
  private JsonObject newPayloadForBatch() {
    JsonObject payload = new JsonObject();
    payload.add("batchRequests", batchRecords.get());
    return payload;
  }

  private HttpUriRequest newRequest(RequestBuilder builder, JsonElement payload) {
    try {
      builder.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
             .addHeader(HttpHeaders.AUTHORIZATION, "OAuth " + accessToken)
             .setEntity(new StringEntity(payload.toString(), ContentType.APPLICATION_JSON));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (getLog().isDebugEnabled()) {
      getLog().debug("Request builder: " + ToStringBuilder.reflectionToString(builder, ToStringStyle.SHORT_PREFIX_STYLE));
    }
    return builder.build();
  }

  @Override
  public void flush() {
    try {
      if (isRetry()) {
        //flushing failed and it should be retried.
        super.writeImpl(null);
        return;
      }

      if (batchRecords.isPresent() && batchRecords.get().size() > 0) {
        getLog().info("Flusing remaining subrequest of batch. # of subrequests: " + batchRecords.get().size());
        curRequest = Optional.of(newRequest(RequestBuilder.post().setUri(combineUrl(getCurServerHost(), batchResourcePath)),
                                                newPayloadForBatch()));
        super.writeImpl(null);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Make it fail (throw exception) if status code is greater or equal to 400 except,
   * the status code is 400 and error code is duplicate value, regard it as success(do not throw exception).
   *
   * If status code is 401 or 403, re-acquire access token before make it fail -- retry will take care of rest.
   *
   * {@inheritDoc}
   * @see gobblin.writer.http.HttpWriter#processResponse(org.apache.http.HttpResponse)
   */
  @Override
  public void processResponse(CloseableHttpResponse response) throws IOException, UnexpectedResponseException {
    if (getLog().isDebugEnabled()) {
      getLog().debug("Received response " + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE));
    }

    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == 401 || statusCode == 403) {
      getLog().info("Reacquiring access token.");
      accessToken = null;
      onConnect(oauthEndPoint);
      throw new RuntimeException("Access denied. Access token has been reacquired and retry may solve the problem. "
                                 + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE));

    }
    if (batchSize > 1) {
      processBatchRequestResponse(response);
      numRecordsWritten += batchRecords.get().size();
      batchRecords = Optional.absent();
    } else {
      processSingleRequestResponse(response);
      numRecordsWritten++;
    }
  }

  private void processSingleRequestResponse(CloseableHttpResponse response) throws IOException,
      UnexpectedResponseException {
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode < 400) {
      return;
    }
    String entityStr = EntityUtils.toString(response.getEntity());
    if (statusCode == 400
        && Operation.INSERT_ONLY_NOT_EXIST.equals(operation)
        && entityStr != null) { //Ignore if it's duplicate entry error code

      JsonArray jsonArray = new JsonParser().parse(entityStr).getAsJsonArray();
      JsonObject jsonObject = jsonArray.get(0).getAsJsonObject();
      if (isDuplicate(jsonObject, statusCode)) {
        return;
      }
    }
    throw new RuntimeException("Failed due to " + entityStr + " (Detail: "
        + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE) + " )");
  }

  /**
   * Check results from batch response, if any of the results is failure throw exception.
   * @param response
   * @throws IOException
   * @throws UnexpectedResponseException
   */
  private void processBatchRequestResponse(CloseableHttpResponse response) throws IOException,
      UnexpectedResponseException {
    String entityStr = EntityUtils.toString(response.getEntity());
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode >= 400) {
      throw new RuntimeException("Failed due to " + entityStr + " (Detail: "
          + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE) + " )");
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
      } else if (subStatusCode == 400
                 && Operation.INSERT_ONLY_NOT_EXIST.equals(operation)) {
        JsonElement resultJsonElem = json.get("result");
        Preconditions.checkNotNull(resultJsonElem, "Error response should contain result property");
        JsonObject resultJsonObject = resultJsonElem.getAsJsonArray().get(0).getAsJsonObject();
        if (isDuplicate(resultJsonObject, subStatusCode)) {
          continue;
        }
      }
      throw new RuntimeException("Failed due to " + jsonBody + " (Detail: "
          + ToStringBuilder.reflectionToString(response, ToStringStyle.SHORT_PREFIX_STYLE) + " )");
    }
  }

  private boolean isDuplicate(JsonObject responseJsonObject, int statusCode) {
    return statusCode == 400
           && Operation.INSERT_ONLY_NOT_EXIST.equals(operation)
           && DUPLICATE_VALUE_ERR_CODE.equals(responseJsonObject.get("errorCode").getAsString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long recordsWritten() {
    return this.numRecordsWritten;
  }
}