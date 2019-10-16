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

package org.apache.gobblin.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * An abstract task class that handles a Http request
 */
@Alpha
@Slf4j
public abstract class HttpExecutionTask extends BaseAbstractTask {
  public final static String CONF_HTTPTASK_CLASS = "gobblin.httptask.class";
  public final static String CONF_HTTPTASK_TYPE = "gobblin.httptask.type";
  protected final TaskContext taskContext;

  public enum HttpMethod {
    POST,
    GET
  }

  public HttpExecutionTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
  }

  /**
   * Provide the Http Request to be performed
   */
  protected abstract HttpUriRequest createHttpRequest();

  /**
   * Provide the payload for the Http Request created using {@link HttpExecutionTask#createHttpRequest()}
   */
  protected abstract Map<String, String> providePayloads();

  /**
   * Provide the header settings for the Http Request created using {@link HttpExecutionTask#createHttpRequest()}
   */
  protected abstract Map<String, String> provideHeaderSettings();

  /**
   * Construct and execute a Http Request built with
   * - {@link HttpExecutionTask#createHttpRequest()}
   * - {@link HttpExecutionTask#providePayloads()}
   * - {@link HttpExecutionTask#provideHeaderSettings()}
   */
  @Override
  public void run() {
    HttpClient httpclient = HttpClients.createDefault();
    try {
      HttpUriRequest request = createHttpUriRequest();
      HttpResponse response = httpclient.execute(request);
      HttpEntity entity = response.getEntity();

      if (entity != null) {
        log.info(EntityUtils.toString(entity));
      }

      this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
    } catch (Exception e) {
      log.error("ADF pipeline execution failed with error message: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Construct a Http Request with implementation from
   * - {@link HttpExecutionTask#createHttpRequest()}
   * - {@link HttpExecutionTask#providePayloads()}
   * - {@link HttpExecutionTask#provideHeaderSettings()}
   *
   * @return the constructed Http Request
   */
  protected HttpUriRequest createHttpUriRequest() throws JsonProcessingException, UnsupportedEncodingException {
    HttpUriRequest request = createHttpRequest();
    log.debug("Created Http request: " + request.toString());
    Map<String, String> headerSettings = provideHeaderSettings();
    for (Map.Entry<String, String> header : headerSettings.entrySet()) {
      request.setHeader(header.getKey(), header.getValue());
    }
    Map<String, String> body = providePayloads();
    if (!body.isEmpty()) {
      if (!(request instanceof HttpEntityEnclosingRequestBase)) {
        this.workingState = WorkUnitState.WorkingState.FAILED;
        throw new RuntimeException(String.format("The Http request type %s doesn't support payload", request.getClass().getName()));
      }
      String bodyJson = new ObjectMapper().writeValueAsString(body);
      log.debug("Json Payload is: " + bodyJson);
      StringEntity reqEntity = new StringEntity(bodyJson);
      ((HttpEntityEnclosingRequestBase) request).setEntity(reqEntity);
    }
    return request;
  }
}
