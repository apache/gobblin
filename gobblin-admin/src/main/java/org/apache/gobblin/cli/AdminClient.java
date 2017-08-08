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
package org.apache.gobblin.cli;

import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.linkedin.r2.RemoteInvocationException;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.rest.*;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;


/**
 * Simple wrapper around the JobExecutionInfoClient
 */
public class AdminClient {
  private final JobExecutionInfoClient client;
  private Closer closer;

  /**
   * Creates a new client with the host and port specified.
   */
  public AdminClient(String host, int port) {
    this.closer = Closer.create();

    URI serverUri = URI.create(String.format("http://%s:%d/", host, port));
    this.client = new JobExecutionInfoClient(serverUri.toString());
    this.closer.register(this.client);
  }

  /**
   * Close connections to the REST server
   */
  public void close() {
    try {
      this.closer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Retrieve a Gobblin job by its id.
   *
   * @param id                Id of the job to retrieve
   * @return JobExecutionInfo representing the job
   */
  public Optional<JobExecutionInfo> queryByJobId(String id) throws RemoteInvocationException {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.JOB_ID);
    query.setId(JobExecutionQuery.Id.create(id));
    query.setLimit(1);

    List<JobExecutionInfo> results = executeQuery(query);
    return getFirstFromQueryResults(results);
  }

  /**
   * Retrieve all jobs
   *
   * @param lookupType Query type
   * @return List of all jobs (limited by results limit)
   */
  public List<JobExecutionInfo> queryAllJobs(QueryListType lookupType, int resultsLimit)
      throws RemoteInvocationException {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.LIST_TYPE);
    query.setId(JobExecutionQuery.Id.create(lookupType));

    // Disable properties and task executions (prevents response size from ballooning)
    query.setJobProperties(ConfigurationKeys.JOB_RUN_ONCE_KEY + "," + ConfigurationKeys.JOB_SCHEDULE_KEY);
    query.setIncludeTaskExecutions(false);

    query.setLimit(resultsLimit);

    return executeQuery(query);
  }

  /**
   * Query jobs by name
   *
   * @param name         Name of the job to query for
   * @param resultsLimit Max # of results to return
   * @return List of jobs with the name (empty list if none can be found)
   */
  public List<JobExecutionInfo> queryByJobName(String name, int resultsLimit) throws RemoteInvocationException {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.JOB_NAME);
    query.setId(JobExecutionQuery.Id.create(name));
    query.setIncludeTaskExecutions(false);
    query.setLimit(resultsLimit);

    return executeQuery(query);
  }

  /**
   * Execute a query and coerce the result into a java List
   * @param query Query to execute
   * @return List of jobs that matched the query. (Empty list if none did).
   * @throws RemoteInvocationException If the server throws an error
   */
  private List<JobExecutionInfo> executeQuery(JobExecutionQuery query) throws RemoteInvocationException {
    JobExecutionQueryResult result = this.client.get(query);

    if (result != null && result.hasJobExecutions()) {
      return result.getJobExecutions();
    }
    return Collections.emptyList();
  }

  private static Optional<JobExecutionInfo> getFirstFromQueryResults(List<JobExecutionInfo> results) {
    if (results == null || results.size() == 0) {
      return Optional.absent();
    }

    return Optional.of(results.get(0));
  }

}
