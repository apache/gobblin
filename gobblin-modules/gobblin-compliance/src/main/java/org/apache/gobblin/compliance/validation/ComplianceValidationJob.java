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
package org.apache.gobblin.compliance.validation;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compliance.ComplianceEvents;
import org.apache.gobblin.compliance.ComplianceJob;
import org.apache.gobblin.compliance.utils.ProxyUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.compliance.ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS;


/**
 * A compliance job for validating if datasets are compliant with policies.
 */
@Slf4j
public class ComplianceValidationJob extends ComplianceJob {

  public ComplianceValidationJob(Properties properties) {
    super(properties);
    initDatasetFinder(properties);
    try {
      ProxyUtils.cancelTokens(new State(properties));
    } catch (IOException | TException | InterruptedException e) {
      Throwables.propagate(e);
    }
  }

  public void initDatasetFinder(Properties properties) {
    Preconditions.checkArgument(properties.containsKey(GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS),
        "Missing required propety " + GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS);
    String finderClass = properties.getProperty(GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS);
    this.finder = GobblinConstructorUtils.invokeConstructor(DatasetsFinder.class, finderClass, new State(properties));
  }

  public void run()
      throws IOException {
    Preconditions.checkNotNull(this.finder, "Dataset finder class is not set");
    List<Dataset> datasets = this.finder.findDatasets();
    this.finishCleanSignal = Optional.of(new CountDownLatch(datasets.size()));
    for (final Dataset dataset : datasets) {
      ListenableFuture<Void> future = this.service.submit(new Callable<Void>() {
        @Override
        public Void call()
            throws Exception {
          if (dataset instanceof ValidatableDataset) {
            ((ValidatableDataset) dataset).validate();
          } else {
            log.warn(
                "Not an instance of " + ValidatableDataset.class + " Dataset won't be validated " + dataset.datasetURN());
          }
          return null;
        }
      });
      Futures.addCallback(future, new FutureCallback<Void>() {
        @Override
        public void onSuccess(@Nullable Void result) {
          ComplianceValidationJob.this.finishCleanSignal.get().countDown();
          log.info("Successfully validated: " + dataset.datasetURN());
        }

        @Override
        public void onFailure(Throwable t) {
          ComplianceValidationJob.this.finishCleanSignal.get().countDown();
          log.warn("Exception caught when validating " + dataset.datasetURN() + ".", t);
          ComplianceValidationJob.this.throwables.add(t);
          ComplianceValidationJob.this.eventSubmitter.submit(ComplianceEvents.Validation.FAILED_EVENT_NAME, ImmutableMap
              .of(ComplianceEvents.FAILURE_CONTEXT_METADATA_KEY, ExceptionUtils.getFullStackTrace(t),
                  ComplianceEvents.DATASET_URN_METADATA_KEY, dataset.datasetURN()));
        }
      });
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (this.finishCleanSignal.isPresent()) {
        this.finishCleanSignal.get().await();
      }
      if (!this.throwables.isEmpty()) {
        for (Throwable t : this.throwables) {
          log.error("Failed validation due to ", t);
        }
        throw new RuntimeException("Validation job failed for one or more datasets");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Not all datasets finish validation job", e);
    } finally {
      ExecutorsUtils.shutdownExecutorService(this.service, Optional.of(log));
      this.closer.close();
    }
  }

  /**
   * Generates validation metrics for the instrumentation of this class.
   */
  protected void regenerateMetrics() {
    // TODO
  }
}
