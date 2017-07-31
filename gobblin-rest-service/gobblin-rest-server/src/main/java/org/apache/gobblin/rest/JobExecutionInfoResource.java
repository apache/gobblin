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

package org.apache.gobblin.rest;

import com.linkedin.restli.server.ResourceContext;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import org.apache.gobblin.metastore.JobHistoryStore;


/**
 * A Rest.li resource for serving queries of Gobblin job executions.
 *
 * @author Yinan Li
 */
@RestLiCollection(name = "jobExecutions", namespace = "org.apache.gobblin.rest")
public class JobExecutionInfoResource extends ComplexKeyResourceTemplate<JobExecutionQuery, EmptyRecord, JobExecutionQueryResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionInfoResource.class);

  @Inject
  @Named("jobHistoryStore")
  private JobHistoryStore jobHistoryStore;

  @Override
  public JobExecutionQueryResult get(ComplexResourceKey<JobExecutionQuery, EmptyRecord> key) {
    JobExecutionQuery query = key.getKey();

    JobExecutionInfoArray jobExecutionInfos = new JobExecutionInfoArray();
    try {
      for (JobExecutionInfo jobExecutionInfo : this.jobHistoryStore.get(query)) {
        jobExecutionInfos.add(jobExecutionInfo);
      }
    } catch (Throwable t) {
      LOGGER
          .error(String.format("Failed to execute query [id = %s, type = %s]", query.getId(), query.getIdType().name()),
              t);
      return null;
    }

    JobExecutionQueryResult result = new JobExecutionQueryResult();
    result.setJobExecutions(jobExecutionInfos);
    ResourceContext rc = this.getContext();
    rc.setResponseHeader("Access-Control-Allow-Origin", "*");
    this.setContext(rc);

    return result;
  }

  @Override
  public Map<ComplexResourceKey<JobExecutionQuery, EmptyRecord>, JobExecutionQueryResult> batchGet(
      Set<ComplexResourceKey<JobExecutionQuery, EmptyRecord>> keys) {

    Map<ComplexResourceKey<JobExecutionQuery, EmptyRecord>, JobExecutionQueryResult> results = Maps.newHashMap();
    for (ComplexResourceKey<JobExecutionQuery, EmptyRecord> key : keys) {
      JobExecutionQueryResult result = get(key);
      if (result != null) {
        results.put(key, get(key));
      }
    }
    ResourceContext rc = this.getContext();
    rc.setResponseHeader("Access-Control-Allow-Origin", "*");
    this.setContext(rc);

    return results;
  }
}
