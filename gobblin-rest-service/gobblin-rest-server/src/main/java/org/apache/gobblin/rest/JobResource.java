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

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.ResourceContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;
import org.apache.gobblin.metastore.jobstore.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;


/**
 * A Rest.li Job resource.
 *
 */
@RestLiCollection(name = "job", namespace = "org.apache.gobblin.rest")
public class JobResource extends CollectionResourceTemplate<String, Job> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobResource.class);

  @Inject
  @Named("jobStore")
  private JobStore jobStore;

  @Override
  public Job get(String jobName) {
    try {
      Job job = this.jobStore.get(jobName);
      ResourceContext rc = this.getContext();
      rc.setResponseHeader("Access-Control-Allow-Origin", "*");
      this.setContext(rc);

      return job;
    } catch (Exception e) {
      LOGGER.error(String.format("Failed to get job: %s", jobName), e);
    }
    return null;
  }

  @Override
  public CreateResponse create(Job entity) {
    try{
      this.jobStore.create(entity);
      return new CreateResponse(entity);
    }catch (Exception e){
      String errorMsg = String.format("Failed to create job: %s, Error: %s", entity, e.getMessage());
      LOGGER.error(errorMsg, e);
      return new CreateResponse(new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, errorMsg));
    }
  }

  @Override
  public UpdateResponse update(String jobName, Job entity){
    try{
      this.jobStore.mergedUpdate(entity);
      return new UpdateResponse(HttpStatus.S_200_OK);
    }catch (Exception e){
      String errorMsg = String.format("Failed to update job: %s, Error: %s", entity, e.getMessage());
      LOGGER.error(errorMsg, e);
      return new UpdateResponse(HttpStatus.S_400_BAD_REQUEST);
    }
  }

  @Override
  public UpdateResponse delete(String jobName){
    try{
      this.jobStore.delete(jobName);
      return new UpdateResponse(HttpStatus.S_200_OK);
    }catch (Exception e){
      String errorMsg = String.format("Failed to delete job: %s, Error: %s", jobName, e.getMessage());
      LOGGER.error(errorMsg, e);
      return new UpdateResponse(HttpStatus.S_400_BAD_REQUEST);
    }
  }

}
