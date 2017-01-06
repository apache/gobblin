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

package gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;

import gobblin.instrumented.GobblinMetricsKeys;
import gobblin.metrics.MetricContext;
import gobblin.metrics.test.MetricsAssert;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;


/** Unit tests for {@link InMemoryJobCatalog} */
public class TestInMemoryJobCatalog {

  @Test
  public void testCallbacks()
      throws Exception {
    InMemoryJobCatalog cat = new InMemoryJobCatalog();
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
    cat.remove(js2.getUri());
    cat.remove(new URI("test:dummy_job"));
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

  @SuppressWarnings("unchecked")
  @Test
  public void testMetrics() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getSimpleName() +".testMetrics");
    InMemoryJobCatalog cat = new InMemoryJobCatalog(Optional.of(log),
        Optional.<MetricContext>absent(), true);
    cat.startAsync();
    cat.awaitRunning(1, TimeUnit.SECONDS);

    MetricsAssert ma = new MetricsAssert(cat.getMetricContext());

    JobSpec js1_1 = JobSpec.builder("test:job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test:job1").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builder("test:job1").withVersion("3").build();
    JobSpec js2 = JobSpec.builder("test:job2").withVersion("1").build();

    cat.put(js1_1);
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 1);
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 1);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 0);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 0);
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
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 1);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 1);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 0);
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
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 1);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 0);
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
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 0);
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
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 1);
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
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 1);

    cat.remove(js1_3.getUri());
    Assert.assertEquals(cat.getMetrics().getNumActiveJobs().getValue().intValue(), 0);
    Assert.assertEquals(cat.getMetrics().getNumAddedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumUpdatedJobs().getCount(), 2);
    Assert.assertEquals(cat.getMetrics().getNumDeletedJobs().getCount(), 2);
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
