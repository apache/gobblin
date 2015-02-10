/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.BatchGetRequest;
import com.linkedin.restli.client.ErrorHandlingBehavior;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.BatchResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;


/**
 * A Rest.li client to work with the Rest.li service for job execution queries.
 *
 * @author ynli
 */
public class JobExecutionInfoClient implements Closeable {

  private final HttpClientFactory httpClientFactory;
  protected final RestClient restClient;

  public JobExecutionInfoClient(String serverUri) {
    this.httpClientFactory = new HttpClientFactory();
    Client r2Client =
        new TransportClientAdapter(this.httpClientFactory.getClient(Collections.<String, String>emptyMap()));
    this.restClient = new RestClient(r2Client, serverUri);
  }

  /**
   * Get a {@link gobblin.rest.JobExecutionQueryResult} for a {@link gobblin.rest.JobExecutionQuery}.
   *
   * @param query a {@link gobblin.rest.JobExecutionQuery}
   * @return a {@link gobblin.rest.JobExecutionQueryResult}
   * @throws RemoteInvocationException
   */
  public JobExecutionQueryResult get(JobExecutionQuery query)
      throws RemoteInvocationException {
    GetRequest<JobExecutionQueryResult> getRequest = new JobExecutionsBuilders().get()
        .id(new ComplexResourceKey<JobExecutionQuery, EmptyRecord>(query, new EmptyRecord())).build();

    Response<JobExecutionQueryResult> response =
        this.restClient.sendRequest(getRequest, ErrorHandlingBehavior.TREAT_SERVER_ERROR_AS_SUCCESS).getResponse();
    return response.getEntity();
  }

  /**
   * Get a collection of {@link JobExecutionQueryResult}s for a collection of {@link JobExecutionQuery}s.
   *
   * <p>
   *     The order of {@link JobExecutionQueryResult}s may not match the order of {@link JobExecutionQuery}s.
   * </p>
   *
   * @param queries a collection of {@link JobExecutionQuery}s
   * @return a collection of {@link JobExecutionQueryResult}s
   * @throws RemoteInvocationException
   */
  public Collection<JobExecutionQueryResult> batchGet(Collection<JobExecutionQuery> queries)
      throws RemoteInvocationException {

    Set<ComplexResourceKey<JobExecutionQuery, EmptyRecord>> ids = Sets.newHashSet();
    for (JobExecutionQuery query : queries) {
      ids.add(new ComplexResourceKey<JobExecutionQuery, EmptyRecord>(query, new EmptyRecord()));
    }

    BatchGetRequest<JobExecutionQueryResult> batchGetRequest = new JobExecutionsBuilders().batchGet().ids(ids).build();

    BatchResponse<JobExecutionQueryResult> response =
        this.restClient.sendRequest(batchGetRequest, ErrorHandlingBehavior.TREAT_SERVER_ERROR_AS_SUCCESS)
            .getResponseEntity();
    return response.getResults().values();
  }

  @Override
  public void close()
      throws IOException {
    this.restClient.shutdown(new FutureCallback<None>());
    this.httpClientFactory.shutdown(new FutureCallback<None>());
  }
}
