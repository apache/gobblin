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

package org.apache.gobblin.runtime.job_catalog;

import com.google.common.base.Predicates;
import com.typesafe.config.Config;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.GobblinMetricsKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.test.MetricsAssert;
import org.apache.gobblin.runtime.api.JobCatalog;
import org.apache.gobblin.runtime.api.JobCatalogListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/** Verify {@link MysqlJobCatalog} [modeled on {@link TestInMemoryJobCatalog}] */
public class TestMysqlJobCatalog {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "job_catalog";

  private MysqlJobCatalog cat;

  /** create a new DB/`JobCatalog` for each test, so they're completely independent */
  @BeforeMethod
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.METRICS_ENABLED_KEY, "true")
        .addPrimitive(MysqlJobCatalog.DB_CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(MysqlJobCatalog.DB_CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(MysqlJobCatalog.DB_CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(MysqlJobCatalog.DB_CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.cat = new MysqlJobCatalog(config);
  }

  @Test
  public void testCallbacks() throws Exception {
    cat.startAsync();
    cat.awaitRunning(1, TimeUnit.SECONDS);

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);

    JobSpec js1_1 = JobSpec.builder("test:job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test:job1").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builder("test:job1").withVersion("3").build();
    JobSpec js2 = JobSpec.builder("test:job2").withVersion("1").build();

    cat.put(js1_1);
    cat.addListener(l);
    cat.put(js1_2);
    cat.put(js2);
    cat.put(js1_3);

    JobSpec js1_latest_version = cat.getJobSpec(js1_1.getUri());
    Assert.assertEquals(js1_3, js1_latest_version);

    cat.remove(js2.getUri());
    cat.remove(new URI("test:dummy_job")); // doesn't exist: won't be found, so expect no callback
    cat.removeListener(l);
    cat.remove(js1_3.getUri());

    Mockito.verify(l).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_2));
    Mockito.verify(l).onAddJob(Mockito.eq(js2));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_3));
    Mockito.verify(l).onDeleteJob(Mockito.eq(js2.getUri()), Mockito.eq(js2.getVersion()));

    Mockito.verifyNoMoreInteractions(l);

    cat.stopAsync();
    cat.awaitTerminated(1, TimeUnit.SECONDS);
  }

  @Test
  public void testMetrics() throws Exception {
    cat.startAsync();
    cat.awaitRunning(1, TimeUnit.SECONDS);

    MetricsAssert ma = new MetricsAssert(cat.getMetricContext());

    JobSpec js1_1 = JobSpec.builder("test:job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test:job1").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builder("test:job1").withVersion("3").build();
    JobSpec js2 = JobSpec.builder("test:job2").withVersion("1").build();

    cat.put(js1_1);
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 0);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 0);
    ma.assertEvent(Predicates.and(
        MetricsAssert.eqEventNamespace(JobCatalog.class.getName()),
        MetricsAssert.eqEventName(JobCatalog.StandardMetrics.TRACKING_EVENT_NAME),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.OPERATION_TYPE_META,
                                     JobCatalog.StandardMetrics.JOB_ADDED_OPERATION_TYPE),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_URI_META, js1_1.getUri().toString()),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_VERSION_META, js1_1.getVersion())
        ),
        100, TimeUnit.MILLISECONDS);

    cat.put(js1_2);
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 0);
    ma.assertEvent(Predicates.and(
        MetricsAssert.eqEventNamespace(JobCatalog.class.getName()),
        MetricsAssert.eqEventName(JobCatalog.StandardMetrics.TRACKING_EVENT_NAME),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.OPERATION_TYPE_META,
                                     JobCatalog.StandardMetrics.JOB_UPDATED_OPERATION_TYPE),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_URI_META, js1_2.getUri().toString()),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_VERSION_META, js1_2.getVersion())
        ),
        100, TimeUnit.MILLISECONDS);

    cat.put(js2);
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 0);
    ma.assertEvent(Predicates.and(
        MetricsAssert.eqEventNamespace(JobCatalog.class.getName()),
        MetricsAssert.eqEventName(JobCatalog.StandardMetrics.TRACKING_EVENT_NAME),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.OPERATION_TYPE_META,
                                     JobCatalog.StandardMetrics.JOB_ADDED_OPERATION_TYPE),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_URI_META, js2.getUri().toString()),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_VERSION_META, js2.getVersion())
        ),
        100, TimeUnit.MILLISECONDS);

    cat.put(js1_3);
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 0);
    ma.assertEvent(Predicates.and(
        MetricsAssert.eqEventNamespace(JobCatalog.class.getName()),
        MetricsAssert.eqEventName(JobCatalog.StandardMetrics.TRACKING_EVENT_NAME),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.OPERATION_TYPE_META,
                                     JobCatalog.StandardMetrics.JOB_UPDATED_OPERATION_TYPE),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_URI_META, js1_3.getUri().toString()),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_VERSION_META, js1_3.getVersion())
        ),
        100, TimeUnit.MILLISECONDS);

    cat.remove(js2.getUri());
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 1);
    ma.assertEvent(Predicates.and(
        MetricsAssert.eqEventNamespace(JobCatalog.class.getName()),
        MetricsAssert.eqEventName(JobCatalog.StandardMetrics.TRACKING_EVENT_NAME),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.OPERATION_TYPE_META,
                                     JobCatalog.StandardMetrics.JOB_DELETED_OPERATION_TYPE),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_URI_META, js2.getUri().toString()),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_VERSION_META, js2.getVersion())
        ),
        100, TimeUnit.MILLISECONDS);

    cat.remove(new URI("test:dummy_job"));
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 1);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 1);

    cat.remove(js1_3.getUri());
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 0);
    Assert.assertEquals(cat.getMetrics().getTotalAddCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalUpdateCalls().getValue().longValue(), 2);
    Assert.assertEquals(cat.getMetrics().getTotalDeleteCalls().getValue().longValue(), 2);
    ma.assertEvent(Predicates.and(
        MetricsAssert.eqEventNamespace(JobCatalog.class.getName()),
        MetricsAssert.eqEventName(JobCatalog.StandardMetrics.TRACKING_EVENT_NAME),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.OPERATION_TYPE_META,
                                     JobCatalog.StandardMetrics.JOB_DELETED_OPERATION_TYPE),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_URI_META, js1_3.getUri().toString()),
        MetricsAssert.eqEventMetdata(GobblinMetricsKeys.JOB_SPEC_VERSION_META, js1_3.getVersion())
        ),
        100, TimeUnit.MILLISECONDS);

    cat.stopAsync();
    cat.awaitTerminated(1, TimeUnit.SECONDS);
  }
}
