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

package org.apache.gobblin.azure.adf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.aad.adal4j.AuthenticationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.task.HttpExecutionTask;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


/**
 * A task that execute Azure Data Factory pipelines through REST calls
 */
@Alpha
@Slf4j
public abstract class AbstractADFPipelineExecutionTask extends HttpExecutionTask {

  public AbstractADFPipelineExecutionTask(TaskContext taskContext) {
    super(taskContext);
  }

  /**
   * Provide the authentication token for calling ADF REST endpoints
   */
  protected abstract AuthenticationResult getAuthenticationToken();

  @Override
  protected HttpUriRequest createHttpRequest() {
    TaskState taskState = this.taskContext.getTaskState();
    WorkUnit wu = taskState.getWorkunit();

    String subscriptionId = wu.getProp(ADFConfKeys.AZURE_SUBSCRIPTION_ID);
    String resourceGroupName = wu.getProp(ADFConfKeys.AZURE_RESOURCE_GROUP_NAME);
    String dataFactoryName = wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_NAME);
    String apiVersion = wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_API_VERSION);
    ADFPipelineExecutionUriBuilder builder = new ADFPipelineExecutionUriBuilder(subscriptionId, resourceGroupName, dataFactoryName, apiVersion);

    String pipelineName = wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_PIPELINE_NAME);
    URI uri;
    try {
      String url = builder.buildPipelineRunUri(pipelineName);
      log.debug("Built Pipeline Execution URL: " + url);
      URIBuilder pipelineExecutionBuilder = new URIBuilder(url);
      uri = pipelineExecutionBuilder.build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    switch (HttpMethod.valueOf(wu.getProp(CONF_HTTPTASK_TYPE).toUpperCase())) {
      case POST:
        return new HttpPost(uri);
      case GET:
        return new HttpGet(uri);
      default:
        throw new UnsupportedOperationException(String.format("Type HttpTask of type %s is not supported", wu.getProp(CONF_HTTPTASK_TYPE)));
    }
  }

  @Override
  protected Map<String, String> provideHeaderSettings() {
    AuthenticationResult token = getAuthenticationToken();

    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json"); //request
    headers.put("Accept", "application/json"); //response
    headers.put("Authorization", "Bearer " + token.getAccessToken());
    return headers;
  }

  /**
   * Override to allow it to be tested
   */
  protected HttpUriRequest createHttpUriRequest() throws JsonProcessingException, UnsupportedEncodingException {
    return super.createHttpUriRequest();
  }
}
