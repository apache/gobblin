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

import com.google.common.collect.Maps;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.job_catalog.FSJobCatalog;
import gobblin.runtime.job_catalog.ImmutableFSJobCatalog;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RestLiCollection(name = "jobconfigs", namespace = "gobblin.service")
/**
 * Rest.li resource for handling job configuration requests
 */
public class JobConfigsResource extends ComplexKeyResourceTemplate<JobConfigId, EmptyRecord, JobConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(JobConfigsResource.class);

  private MutableJobCatalog jobCatalog;

  JobConfigsResource(FSJobCatalog jobCatalog) {
    this.jobCatalog = jobCatalog;
  }

  /**
   * Logs message and throws Rest.li exception
   * @param status HTTP status code
   * @param msg error message
   * @param e exception
   */
  public void logAndThrowRestLiServiceException(HttpStatus status, String msg, Exception e) {
    if (e != null) {
      LOG.error(msg, e);
      throw new RestLiServiceException(status, msg + " cause = " + e.getMessage());
    } else {
      LOG.error(msg);
      throw new RestLiServiceException(status, msg);
    }
  }

  /**
   * Retrieve the {@link JobConfig} with the given key
   * @param key job config id key containing group name and job name
   * @return {@link JobConfig} with job configuration
   */
  @Override
  public JobConfig get(ComplexResourceKey<JobConfigId, EmptyRecord> key) {
    String jobGroup = key.getKey().getJobGroup();
    String jobName = key.getKey().getJobName();

    LOG.info("Get called with jobGroup " + jobGroup + " jobName " + jobName);

    try {
      JobSpec jobSpec = jobCatalog.getJobSpec(new URI("/" + jobGroup  + "/" + jobName));
      Properties jobProps = jobSpec.getConfigAsProperties();
      String schedule = jobProps.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY);
      String template = jobProps.getProperty(ConfigurationKeys.JOB_TEMPLATE_PATH);
      // remove keys that were injected by jobCatalog or as part of jobSpec creation
      jobProps.remove(ConfigurationKeys.JOB_SCHEDULE_KEY);
      jobProps.remove(ConfigurationKeys.JOB_NAME_KEY);
      jobProps.remove(ConfigurationKeys.JOB_GROUP_KEY);
      jobProps.remove(ConfigurationKeys.JOB_TEMPLATE_PATH);
      jobProps.remove(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY);
      jobProps.remove(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
      jobProps.remove(ImmutableFSJobCatalog.VERSION_KEY_IN_JOBSPEC);
      jobProps.remove(ImmutableFSJobCatalog.DESCRIPTION_KEY_IN_JOBSPEC);

      StringMap jobPropsAsStringMap = new StringMap();
      jobPropsAsStringMap.putAll(Maps.fromProperties(jobProps));

      return new JobConfig().setJobGroup(jobGroup).setJobName(jobName).setSchedule(schedule).setTemplateUri(template)
          .setProperties(jobPropsAsStringMap);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + jobName, e);
    } catch (JobSpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Job requested does not exist: " + jobName, null);
    }

    return null;
  }

  /**
   * Build a {@link JobSpec} from a {@link JobConfig}
   * @param jobConfig job configuration
   * @return {@link JobSpec} created with attributes from jobConfig
   */
  private JobSpec createJobSpecForConfig(JobConfig jobConfig) {
    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, jobConfig.getJobGroup())
        .addPrimitive(ConfigurationKeys.JOB_NAME_KEY, jobConfig.getJobName())
        .addPrimitive(ConfigurationKeys.JOB_SCHEDULE_KEY, jobConfig.getSchedule()).build();
    Config configWithFallback = config.withFallback(ConfigFactory.parseMap(jobConfig.getProperties()));

    URI templateURI = null;
    try {
      templateURI = new URI(jobConfig.getTemplateUri());
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + jobConfig.getTemplateUri(), e);
    }

    return JobSpec.builder().withConfig(configWithFallback).withTemplate(templateURI).build();
  }

  /**
   * Put a new {@link JobSpec} based on jobConfig in the {@link MutableJobCatalog}
   * @param jobConfig job configuration
   * @return {@link CreateResponse}
   */
  @Override
  public CreateResponse create(JobConfig jobConfig) {
    LOG.info("Create called with jobName " + jobConfig.getJobName());

    try {
      URI jobUri = new URI("/" + jobConfig.getJobGroup() + "/" + jobConfig.getJobName());
      if (jobCatalog.getJobSpec(jobUri) != null) {
        logAndThrowRestLiServiceException(HttpStatus.S_409_CONFLICT,
            "Job with the same name already exists: " + jobUri, null);
      }
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + jobConfig.getJobName(), e);
    } catch (JobSpecNotFoundException e) {
      // okay if job does not exist
    }

    jobCatalog.put(createJobSpecForConfig(jobConfig));

    return new CreateResponse(jobConfig.getJobName(), HttpStatus.S_201_CREATED);
  }

  /**
   * Update the {@link JobSpec} in the {@link MutableJobCatalog} based on the specified {@link JobConfig}
   * @param key composite key containing group name and job name that identifies the job to update
   * @param jobConfig new job configuration
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse update(ComplexResourceKey<JobConfigId, EmptyRecord> key, JobConfig jobConfig) {
    String jobGroup = key.getKey().getJobGroup();
    String jobName = key.getKey().getJobName();
    String jobUriString = "/" + jobGroup  + "/" + jobName;

    LOG.info("Update called with jobGroup " + jobGroup + " jobName " + jobName);

    if (!jobGroup.equals(jobConfig.getJobGroup()) || !jobName.equals(jobConfig.getJobName())) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "jobName and jobGroup cannot be changed in update", null);
    }

    try {
      URI jobUri = new URI(jobUriString);
      JobSpec oldJobSpec = jobCatalog.getJobSpec(jobUri);
      JobSpec newJobSpec = createJobSpecForConfig(jobConfig);

      jobCatalog.put(newJobSpec);

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + jobUriString, e);
    } catch (JobSpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Job does not exist: jobGroup " + jobGroup +
          " jobName " + jobName, null);
    }

    return null;
  }

  /** delete a configured job
   * @param key composite key containing job group and job name that identifies the job to remove from the
   * {@link MutableJobCatalog}
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<JobConfigId, EmptyRecord> key) {
    String jobGroup = key.getKey().getJobGroup();
    String jobName = key.getKey().getJobName();
    String jobUriString = "/" + jobGroup  + "/" + jobName;

    LOG.info("Delete called with jobGroup " + jobGroup + " jobName " + jobName);

    try {
      URI jobUri = new URI(jobUriString);
      JobSpec jobSpec = jobCatalog.getJobSpec(jobUri);

      jobCatalog.remove(jobUri);

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (URISyntaxException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "bad URI " + jobUriString, e);
    } catch (JobSpecNotFoundException e) {
      logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND, "Job does not exist: jobGroup " + jobGroup +
          " jobName " + jobName, null);
    }

    return null;
  }
}

