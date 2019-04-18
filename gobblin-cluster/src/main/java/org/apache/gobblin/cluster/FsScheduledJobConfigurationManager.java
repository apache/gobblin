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
package org.apache.gobblin.cluster;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;


/**
 * A {@link ScheduledJobConfigurationManager} that reads {@link JobSpec}s from a source path on a
 * {@link org.apache.hadoop.fs.FileSystem} and adds them to a {@link org.apache.gobblin.runtime.api.JobCatalog}.
 * The {@link FsScheduledJobConfigurationManager} has an underlying {@link FsSpecConsumer} that reads the {@link JobSpec}s
 * from the filesystem and once the JobSpecs have been added to the {@link org.apache.gobblin.runtime.api.JobCatalog},
 * the consumer deletes the specs from the source path.
 */
@Slf4j
public class FsScheduledJobConfigurationManager extends ScheduledJobConfigurationManager {
  private final MutableJobCatalog _jobCatalog;

  public FsScheduledJobConfigurationManager(EventBus eventBus, Config config, MutableJobCatalog jobCatalog) {
    super(eventBus, config);
    this._jobCatalog = jobCatalog;
  }

  @Override
  protected void fetchJobSpecs() throws ExecutionException, InterruptedException {
    List<Pair<SpecExecutor.Verb, Spec>> jobSpecs =
        (List<Pair<SpecExecutor.Verb, Spec>>) this._specConsumer.changedSpecs().get();

    for (Pair<SpecExecutor.Verb, Spec> entry : jobSpecs) {
      Spec spec = entry.getValue();
      SpecExecutor.Verb verb = entry.getKey();
      if (verb.equals(SpecExecutor.Verb.ADD) || verb.equals(SpecExecutor.Verb.UPDATE)) {
        // Handle addition
        JobSpec jobSpec = (JobSpec) spec;
        this._jobCatalog.put(jobSpec);
        postNewJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.DELETE)) {
        // Handle delete
        this._jobCatalog.remove(spec.getUri());
        postDeleteJobConfigArrival(spec.getUri().toString(), new Properties());
      }

      try {
        //Acknowledge the successful consumption of the JobSpec back to the SpecConsumer, so that the
        //SpecConsumer can delete the JobSpec.
        this._specConsumer.commit(spec);
      } catch (IOException e) {
        log.error("Error when committing to FsSpecConsumer: ", e);
      }
    }
  }
}
