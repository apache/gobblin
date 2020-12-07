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

package org.apache.gobblin.zuora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HttpsURLConnection;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommand;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommandOutput;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.jdbc.SqlQueryUtils;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


@Alpha
@Slf4j
class ZuoraClientImpl implements ZuoraClient {
  private static final Gson GSON = new Gson();
  private final WorkUnitState _workUnitState;
  private final String _hostName;
  private final Retryer<CommandOutput<RestApiCommand, String>> _postRetryer;
  private final Retryer<List<String>> _getRetryer;

  ZuoraClientImpl(WorkUnitState workUnitState) {
    _workUnitState = workUnitState;
    _hostName = _workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    _postRetryer =
        RetryerBuilder.<CommandOutput<RestApiCommand, String>>newBuilder().retryIfExceptionOfType(IOException.class)
            .withStopStrategy(StopStrategies
                .stopAfterAttempt(workUnitState.getPropAsInt(ZuoraConfigurationKeys.ZUORA_API_RETRY_POST_COUNT, 20)))
            .withWaitStrategy(WaitStrategies
                .fixedWait(workUnitState.getPropAsInt(ZuoraConfigurationKeys.ZUORA_API_RETRY_POST_WAIT_TIME, 60000),
                    TimeUnit.MILLISECONDS)).build();
    _getRetryer = RetryerBuilder.<List<String>>newBuilder().retryIfExceptionOfType(IOException.class).withStopStrategy(
        StopStrategies
            .stopAfterAttempt(workUnitState.getPropAsInt(ZuoraConfigurationKeys.ZUORA_API_RETRY_GET_FILES_COUNT, 30)))
        .withWaitStrategy(WaitStrategies
            .fixedWait(workUnitState.getPropAsInt(ZuoraConfigurationKeys.ZUORA_API_RETRY_GET_FILES_WAIT_TIME, 30000),
                TimeUnit.MILLISECONDS)).build();
  }

  @Override
  public List<Command> buildPostCommand(List<Predicate> predicateList) {
    String host = getEndPoint("batch-query/");
    List<String> params = Lists.newLinkedList();
    params.add(host);

    String query = _workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY,
        "SELECT * FROM " + _workUnitState.getProp(ConfigurationKeys.SOURCE_ENTITY));

    if (predicateList != null) {
      for (Predicate predicate : predicateList) {
        query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
      }
    }

    String rowLimit = _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_ROW_LIMIT);
    if (StringUtils.isNotBlank(rowLimit)) {
      query += " LIMIT " + rowLimit;
    }

    List<ZuoraQuery> queries = Lists.newArrayList();
    queries.add(new ZuoraQuery(_workUnitState.getProp(ConfigurationKeys.JOB_NAME_KEY), query,
        _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_DELTED_COLUMN, "")));
    ZuoraParams filterPayload = new ZuoraParams(_workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_PARTNER, "sample"),
        _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_PROJECT, "sample"), queries,
        _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_API_NAME, "sample"),
        _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_OUTPUT_FORMAT, "csv"),
        _workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_VERSION, "1.1"));
    params.add(GSON.toJson(filterPayload));
    return Collections.singletonList(new RestApiCommand().build(params, RestApiCommand.RestApiCommandType.POST));
  }

  @Override
  public CommandOutput<RestApiCommand, String> executePostRequest(final Command command)
      throws DataRecordException {
    try {
      return _postRetryer.call(new Callable<CommandOutput<RestApiCommand, String>>() {
        @Override
        public CommandOutput<RestApiCommand, String> call()
            throws Exception {
          return executePostRequestInternal(command);
        }
      });
    } catch (Exception e) {
      throw new DataRecordException("Post request failed for command: " + command.toString(), e);
    }
  }

  public static String getJobId(CommandOutput<?, ?> postResponse)
      throws DataRecordException {
    Iterator<String> itr = (Iterator<String>) postResponse.getResults().values().iterator();
    if (!itr.hasNext()) {
      throw new DataRecordException("Failed to get data from RightNowCloud; REST postResponse has no output");
    }

    String stringResponse = itr.next();
    log.info("Zuora post response: " + stringResponse);
    JsonObject jsonObject = GSON.fromJson(stringResponse, JsonObject.class).getAsJsonObject();
    return jsonObject.get("id").getAsString();
  }

  @Override
  public List<String> getFileIds(final String jobId)
      throws DataRecordException, IOException {
    log.info("Getting files for job " + jobId);
    String url = getEndPoint("batch-query/jobs/" + jobId);
    final Command cmd = new RestApiCommand().build(Collections.singleton(url), RestApiCommand.RestApiCommandType.GET);

    try {
      return _getRetryer.call(new Callable<List<String>>() {
        @Override
        public List<String> call()
            throws Exception {
          return executeGetRequestInternal(cmd, jobId);
        }
      });
    } catch (Exception e) {
      throw new DataRecordException("Get request failed for command: " + cmd.toString(), e);
    }
  }

  private List<String> executeGetRequestInternal(Command cmd, String jobId)
      throws IOException, DataRecordException {
    CommandOutput<RestApiCommand, String> response = executeGetRequest(cmd);
    Iterator<String> itr = response.getResults().values().iterator();
    if (!itr.hasNext()) {
      throw new DataRecordException("Failed to get file Ids based on job id " + jobId);
    }
    String output = itr.next();
    JsonObject jsonResp = GSON.fromJson(output, JsonObject.class).getAsJsonObject();
    String status = jsonResp.get("status").getAsString();
    log.info(String.format("Job %s %s: %s", jobId, status, output));
    if (!status.equals("completed")) {
      throw new IOException("Retrying... This exception will be handled by retryer.");
    }
    List<String> fileIds = Lists.newArrayList();
    for (JsonElement jsonObj : jsonResp.get("batches").getAsJsonArray()) {
      fileIds.add(jsonObj.getAsJsonObject().get("fileId").getAsString());
    }
    log.info("Get Files Response - FileIds: " + fileIds);
    return fileIds;
  }

  @Override
  public CommandOutput<RestApiCommand, String> executeGetRequest(final Command cmd)
      throws IOException {
    HttpsURLConnection connection = null;
    try {
      String urlPath = cmd.getParams().get(0);
      connection = ZuoraUtil.getConnection(urlPath, _workUnitState);
      connection.setRequestProperty("Accept", "application/json");

      String result = ZuoraUtil.getStringFromInputStream(connection.getInputStream());
      CommandOutput<RestApiCommand, String> output = new RestApiCommandOutput();
      output.put((RestApiCommand) cmd, result);
      return output;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private CommandOutput<RestApiCommand, String> executePostRequestInternal(Command command)
      throws IOException {
    List<String> params = command.getParams();
    String payLoad = params.get(1);
    log.info("Executing post request with payLoad:" + payLoad);

    BufferedReader br = null;
    HttpsURLConnection connection = null;
    try {
      connection = ZuoraUtil.getConnection(params.get(0), _workUnitState);
      connection.setDoOutput(true);
      connection.setRequestMethod("POST");

      OutputStream os = connection.getOutputStream();
      os.write(payLoad.getBytes("UTF-8"));
      os.flush();

      br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
      StringBuilder result = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        result.append(line);
      }
      CommandOutput<RestApiCommand, String> output = new RestApiCommandOutput();
      output.put((RestApiCommand) command, result.toString());
      return output;
    } finally {
      if (br != null) {
        br.close();
      }
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Override
  public String getEndPoint(String relativeUrl) {
    return _hostName + relativeUrl;
  }
}
