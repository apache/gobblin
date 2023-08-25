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

package org.apache.gobblin.metastore.jobstore;

import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.StringArray;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.JobStoreModule;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.rest.Job;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;


@Test(groups = {"gobblin.jobmgmt"})
public class MySQLJobStoreTest {
    private ITestMetastoreDatabase testMetastoreDatabase;
    private JobStore jobStore;
    private final String CREATE_GET_TEST_JOB_NAME = "test_job_to_create";
    private final String UPDATE_GET_TEST_JOB_NAME = "test_job_to_update";
    private final String MERGED_UPDATE_GET_TEST_JOB_NAME = "test_job_to_merge_update";

    @BeforeClass
    public void setUp() throws Exception {
        ConfigFactory.invalidateCaches();
        testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, testMetastoreDatabase.getJdbcUrl());
        Injector injector = Guice.createInjector(new JobStoreModule(properties));
        this.jobStore = injector.getInstance(JobStore.class);
    }

    @Test
    public void createJobTest() throws Exception {
        Job createdJob = createTestJob(this.CREATE_GET_TEST_JOB_NAME);
        jobStore.create(createdJob);

        // get job from DB and verify values
        Job gotJob = jobStore.get(createdJob.getName());
        Assert.assertTrue(compareJobContent(createdJob, gotJob));
    }

    @Test
    public void updateTest() throws Exception {
        Job jobToUpdate = createTestJob(this.UPDATE_GET_TEST_JOB_NAME);
        jobStore.create(jobToUpdate);

        String newDescription = "new description with update.";
        String newSchedule = "NEW_DUMMY_CRON_EXP";
        String newOwnerEmail = "someone@paypal.com";
        String newSourceSystem = "Jackal";
        String newTargetSystem = "Horton";
        int newPriority = 32000;

        jobToUpdate.setName(this.UPDATE_GET_TEST_JOB_NAME);
        jobToUpdate.setDescription(newDescription);
        jobToUpdate.setSchedule(newSchedule);
        jobToUpdate.setDisabled(!jobToUpdate.isDisabled());
        jobToUpdate.setPriority(newPriority);
        jobToUpdate.getConfigs().remove("property2");
        jobToUpdate.getConfigs().put("property3", "value3");
        jobToUpdate.getConfigs().put("job.group", "NewTestGroup");
        jobToUpdate.setOwnerEmail(newOwnerEmail);
        jobToUpdate.setSourceSystem(newSourceSystem);
        jobToUpdate.setTargetSystem(newTargetSystem);

        jobStore.update(jobToUpdate);

        Job updatedJob = jobStore.get(this.UPDATE_GET_TEST_JOB_NAME);
        Assert.assertTrue(compareJobContent(jobToUpdate, updatedJob));
    }


    @Test
    public void mergedUpdateTest() throws Exception {
        Job createdJob = createTestJob(this.MERGED_UPDATE_GET_TEST_JOB_NAME);
        jobStore.create(createdJob);

        String newDescription = "new description with update.";
        String newSchedule = "NEW_DUMMY_CRON_EXP";
        String newOwnerEmail = "someone@paypal.com";
        String newSourceSystem = "Jackal";
        String newTargetSystem = "Horton";
        int newPriority = 32000;

        Job jobToUpdate = new Job();
        jobToUpdate.setName(this.MERGED_UPDATE_GET_TEST_JOB_NAME);
        jobToUpdate.setDescription(newDescription);
        jobToUpdate.setSchedule(newSchedule);
        jobToUpdate.setDisabled(!createdJob.isDisabled());
        jobToUpdate.setPriority(newPriority);
        jobToUpdate.setConfigs(new StringMap());
        jobToUpdate.getConfigs().put("property3", "value3");
        jobToUpdate.getConfigs().put("job.group", "NewTestGroup");
        StringArray jobPropertiesToRemove = new StringArray();
        jobPropertiesToRemove.add("property2");
        jobToUpdate.setConfigsToRemove(jobPropertiesToRemove);
        jobToUpdate.setOwnerEmail(newOwnerEmail);
        jobToUpdate.setSourceSystem(newSourceSystem);
        jobToUpdate.setTargetSystem(newTargetSystem);

        jobStore.mergedUpdate(jobToUpdate);

        Job updatedJob = jobStore.get(this.MERGED_UPDATE_GET_TEST_JOB_NAME);

        Assert.assertEquals(jobToUpdate.getDescription(),updatedJob.getDescription(), "job description do not match.");
        Assert.assertEquals(jobToUpdate.getSchedule(),updatedJob.getSchedule(), "job schedule do not match.");
        Assert.assertEquals(jobToUpdate.isDisabled(),updatedJob.isDisabled(), "job disable flag do not match.");
        Assert.assertEquals(jobToUpdate.getPriority(),updatedJob.getPriority(), "job priority do not match.");
        Assert.assertNotEquals(updatedJob.getConfigs(), jobToUpdate.getConfigs());
        Assert.assertNull(updatedJob.getConfigs().get("property2"));
        Assert.assertNotNull(updatedJob.getConfigs().get("property3"));
        Assert.assertEquals(jobToUpdate.getOwnerEmail(),updatedJob.getOwnerEmail(), "job owner email do not match.");
        Assert.assertEquals(jobToUpdate.getSourceSystem(),updatedJob.getSourceSystem(), "job source system do not match.");
        Assert.assertEquals(jobToUpdate.getTargetSystem(),updatedJob.getTargetSystem(), "job target system do not match.");
        Assert.assertNotNull(updatedJob.getCreatedDate(), "job dont have created date.");
        Assert.assertNotNull(updatedJob.getUpdatedDate(), "job dont have updated date.");
    }

    Job createTestJob(String jobName){
        Job job = new Job();
        job.setName(jobName);
        job.setDescription("test description");
        job.setSchedule("DUMMY_CRON_EXP");
        job.setDisabled(false);
        job.setPriority(99);
        StringMap jobProperties = new StringMap();
        jobProperties.put("property1", "value1");
        jobProperties.put("property2", "value2");
        jobProperties.put("job.group", "TestGroup");
        jobProperties.put("source.class.name", "org.apache.gobblin.example.wikipedia.WikipediaSource");
        job.setConfigs(jobProperties);
        job.setOwnerEmail("jay@paypal.com");
        job.setSourceSystem("teradata");
        job.setTargetSystem("hadoop");
        return job;
    }

    boolean compareJobContent(Job existingJob, Job newJob){
        Assert.assertEquals(existingJob.getDescription(),newJob.getDescription(), "job description do not match.");
        Assert.assertEquals(existingJob.getSchedule(),newJob.getSchedule(), "job schedule do not match.");
        Assert.assertEquals(existingJob.isDisabled(),newJob.isDisabled(), "job disable flag do not match.");
        Assert.assertEquals(existingJob.getPriority(),newJob.getPriority(), "job priority do not match.");
        Assert.assertTrue(existingJob.getConfigs().equals(newJob.getConfigs()));
        Assert.assertEquals(existingJob.getConfigs(),newJob.getConfigs(), "job configs do not match.");
        Assert.assertEquals(existingJob.getOwnerEmail(),newJob.getOwnerEmail(), "job owner email do not match.");
        Assert.assertEquals(existingJob.getSourceSystem(),newJob.getSourceSystem(), "job source system do not match.");
        Assert.assertEquals(existingJob.getTargetSystem(),newJob.getTargetSystem(), "job target system do not match.");
        Assert.assertNotNull(newJob.getCreatedDate(), "job dont have created date.");
        Assert.assertNotNull(newJob.getUpdatedDate(), "job dont have updated date.");
        return true;
    }

    @Test
    public void remove() throws Exception {
    }
}
