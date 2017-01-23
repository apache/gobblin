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

package gobblin.service;

import com.google.common.base.Optional;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.DeleteRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.UpdateRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Job Configuration client for REST job configuration server
 */
public class JobConfigClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(JobConfigClient.class);

  private Optional<HttpClientFactory> httpClientFactory;
  private Optional<RestClient> restClient;
  private final JobconfigsRequestBuilders jobconfigsRequestBuilders;

  /**
   * Construct a {@link JobConfigClient} to communicate with http job config server at URI serverUri
   * @param serverUri address and port of the REST server
   */
  public JobConfigClient(String serverUri) {
    LOG.debug("JobConfigClient with serverUri " + serverUri);

    httpClientFactory = Optional.of(new HttpClientFactory());
    Client r2Client = new TransportClientAdapter(httpClientFactory.get().getClient(Collections.<String, String>emptyMap()));
    restClient = Optional.of(new RestClient(r2Client, serverUri));

    jobconfigsRequestBuilders = new JobconfigsRequestBuilders();
  }

  /**
   * Create a job configuration
   * @param jobConfig job configuration attributes
   * @throws RemoteInvocationException
   */
  public void createJobConfig(JobConfig jobConfig)
      throws RemoteInvocationException {
    LOG.debug("createJobConfig with groupName " + jobConfig.getJobGroup() + " jobName " + jobConfig.getJobName());

    CreateIdRequest<ComplexResourceKey<JobConfigId, EmptyRecord>, JobConfig> request =
        jobconfigsRequestBuilders.create().input(jobConfig).build();
    ResponseFuture<IdResponse<ComplexResourceKey<JobConfigId, EmptyRecord>>> jobConfigResponseFuture =
        restClient.get().sendRequest(request);

    jobConfigResponseFuture.getResponse();
  }

  /**
   * Update a job configuration
   * @param jobConfig job configuration attributes
   * @throws RemoteInvocationException
   */
  public void updateJobConfig(JobConfig jobConfig)
      throws RemoteInvocationException {
    LOG.debug("updateJobConfig with groupName " + jobConfig.getJobGroup() + " jobName " + jobConfig.getJobName());

    JobConfigId jobConfigId = new JobConfigId().setJobGroup(jobConfig.getJobGroup()).setJobName(jobConfig.getJobName());

    UpdateRequest<JobConfig> updateRequest =
        jobconfigsRequestBuilders.update().id(new ComplexResourceKey<>(jobConfigId, new EmptyRecord()))
            .input(jobConfig).build();

    ResponseFuture<EmptyRecord> response = restClient.get().sendRequest(updateRequest);

    response.getResponse();
  }

  /**
   * Get a job configuration
   * @param jobConfigId identifier of job configuration to get
   * @return a {@link JobConfig} with the job configuration
   * @throws RemoteInvocationException
   */
  public JobConfig getJobConfig(JobConfigId jobConfigId)
      throws RemoteInvocationException {
    LOG.debug("getJobConfig with groupName " + jobConfigId.getJobGroup() + " jobName " + jobConfigId.getJobName());

    GetRequest<JobConfig> getRequest = jobconfigsRequestBuilders.get()
        .id(new ComplexResourceKey<>(jobConfigId, new EmptyRecord())).build();

    Response<JobConfig> response =
        restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity();
  }

  /**
   * Delete a job configuration
   * @param jobConfigId identifier of job configuration to delete
   * @throws RemoteInvocationException
   */
  public void deleteJobConfig(JobConfigId jobConfigId)
      throws RemoteInvocationException {
    LOG.debug("deleteJobConfig with groupName " + jobConfigId.getJobGroup() + " jobName " + jobConfigId.getJobName());

    DeleteRequest<JobConfig> deleteRequest = jobconfigsRequestBuilders.delete()
        .id(new ComplexResourceKey<>(jobConfigId, new EmptyRecord())).build();
    ResponseFuture<EmptyRecord> response = restClient.get().sendRequest(deleteRequest);

    response.getResponse();
  }

  @Override
  public void close()
      throws IOException {
    if (restClient.isPresent()) {
      restClient.get().shutdown(new FutureCallback<None>());
    }

    if (httpClientFactory.isPresent()) {
      httpClientFactory.get().shutdown(new FutureCallback<None>());
    }
  }
}