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

package org.apache.gobblin.publisher;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;

import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.hive.HiveRegProps;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ExecutorsUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link DataPublisher} that registers the already published data with Hive.
 *
 * <p>
 *   This publisher is not responsible for publishing data, and it relies on another publisher
 *   to document the published paths in property {@link ConfigurationKeys#PUBLISHER_DIRS}. Thus this publisher
 *   should generally be used as a job level data publisher, where the task level publisher should be a publisher
 *   that documents the published paths, such as {@link BaseDataPublisher}.
 * </p>
 *
 * @author Ziyang Liu
 */
@Slf4j
@Alias("hivereg")
public class HiveRegistrationPublisher extends DataPublisher {

  private static final String DATA_PUBLISH_TIME = HiveRegistrationPublisher.class.getName() + ".lastDataPublishTime";
  private static final Splitter LIST_SPLITTER_COMMA = Splitter.on(",").trimResults().omitEmptyStrings();
  public static final String HIVE_SPEC_COMPUTATION_TIMER = "hiveSpecComputationTimer";
  private static final String PATH_DEDUPE_ENABLED = "hive.registration.path.dedupe.enabled";
  private static final boolean DEFAULT_PATH_DEDUPE_ENABLED = true;

  private final Closer closer = Closer.create();
  private final HiveRegister hiveRegister;
  private final ExecutorService hivePolicyExecutor;
  private final MetricContext metricContext;

  /**
   * The configuration to determine if path deduplication should be enabled during Hive Registration process.
   * Recall that HiveRegistration iterate thru. each topics' data folder and obtain schema from newest partition,
   * it might be the case that a table corresponding to a registered path has a schema changed.
   * In this case, path-deduplication won't work.
   *
   * e.g. In streaming mode, there could be cases that files(e.g. avro) under single topic folder carry different schema.
   */
  private boolean isPathDedupeEnabled;

  /**
   * Make the deduplication of path to be registered in the Publisher level,
   * So that each invocation of {@link #publishData(Collection)} contribute paths registered to this set.
   */
  private static Set<String> pathsToRegisterFromSingleState = Sets.newHashSet();

  /**
   * @param state This is a Job State
   */
  public HiveRegistrationPublisher(State state) {
    super(state);
    this.hiveRegister = this.closer.register(HiveRegister.get(state));
    this.hivePolicyExecutor = ExecutorsUtils.loggingDecorator(Executors.newFixedThreadPool(new HiveRegProps(state).getNumThreads(),
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("HivePolicyExecutor-%d"))));
    this.metricContext = Instrumented.getMetricContext(state, HiveRegistrationPublisher.class);

    isPathDedupeEnabled = state.getPropAsBoolean(PATH_DEDUPE_ENABLED, this.DEFAULT_PATH_DEDUPE_ENABLED);
  }

  @Override
  public void close() throws IOException {
    try {
      ExecutorsUtils.shutdownExecutorService(this.hivePolicyExecutor, Optional.of(log));
    } finally {
      this.closer.close();
    }
  }

  @Deprecated
  @Override
  public void initialize() throws IOException {}

  /**
   * @param states This is a collection of TaskState.
   */
  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    CompletionService<Collection<HiveSpec>> completionService =
        new ExecutorCompletionService<>(this.hivePolicyExecutor);

    // Each state in states is task-level State, while superState is the Job-level State.
    // Using both State objects to distinguish each HiveRegistrationPolicy so that
    // they can carry task-level information to pass into Hive Partition and its corresponding Hive Table.

    // Here all runtime task-level props are injected into superstate which installed in each Policy Object.
    // runtime.props are comma-separated props collected in runtime.
    int toRegisterPathCount = 0 ;
    for (State state:states) {
      State taskSpecificState = state;
      if (state.contains(ConfigurationKeys.PUBLISHER_DIRS)) {

        // Upstream data attribute is specified, need to inject these info into superState as runtimeTableProps.
        if (this.hiveRegister.getProps().getUpstreamDataAttrName().isPresent()) {
          for (String attrName:
              LIST_SPLITTER_COMMA.splitToList(this.hiveRegister.getProps().getUpstreamDataAttrName().get())){
            if (state.contains(attrName)) {
              taskSpecificState.appendToListProp(HiveMetaStoreUtils.RUNTIME_PROPS,
                    attrName + ":" + state.getProp(attrName));
            }
          }
        }

        final HiveRegistrationPolicy policy = HiveRegistrationPolicyBase.getPolicy(taskSpecificState);
        for ( final String path : state.getPropAsList(ConfigurationKeys.PUBLISHER_DIRS) ) {
          if (isPathDedupeEnabled && pathsToRegisterFromSingleState.contains(path)){
            continue;
          }
          pathsToRegisterFromSingleState.add(path);
          toRegisterPathCount += 1;
          completionService.submit(new Callable<Collection<HiveSpec>>() {
            @Override
            public Collection<HiveSpec> call() throws Exception {
              try (Timer.Context context = metricContext.timer(HIVE_SPEC_COMPUTATION_TIMER).time()) {
                return policy.getHiveSpecs(new Path(path));
              }
            }
          });
        }
      }
      else continue;
    }
    for (int i = 0; i < toRegisterPathCount; i++) {
      try {
        for (HiveSpec spec : completionService.take().get()) {
          this.hiveRegister.register(spec);
        }
      } catch (InterruptedException | ExecutionException e) {
        log.info("Failed to generate HiveSpec", e);
        throw new IOException(e);
      }
    }
    log.info("Finished registering all HiveSpecs");
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    // Nothing to do
  }

  private static void addRuntimeHiveRegistrationProperties(State state) {
    // Use seconds instead of milliseconds to be consistent with other times stored in hive
    state.appendToListProp(HiveRegProps.HIVE_TABLE_PARTITION_PROPS,
        String.format("%s:%d", DATA_PUBLISH_TIME, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)));
  }
}
