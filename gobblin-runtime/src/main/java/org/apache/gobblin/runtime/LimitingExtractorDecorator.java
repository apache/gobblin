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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.util.TaskMetrics;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.limiter.LimiterConfigurationKeys;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.Decorator;
import org.apache.gobblin.util.limiter.Limiter;


/**
 * A decorator class for {@link Extractor} that uses a {@link Limiter} on data record extraction.
 *
 * <p>
 *   The fact that the {@link Limiter} is passed in as a parameter to the constructor
 *   {@link LimitingExtractorDecorator#LimitingExtractorDecorator(Extractor, Limiter, TaskState)} (Extractor, Limiter, State)} (Extractor, Limiter)}
 *   means multiple {@link LimitingExtractorDecorator}s can share a single {@link Limiter}
 *   or each individual {@link LimitingExtractorDecorator} has its own {@link Limiter}.
 *   The first case is useful for throttling at above the task level, e.g., at the job level.
 * </p>
 *
 * @param <S> output schema type
 * @param <D> output record type
 *
 * @author Yinan Li
 */
public class LimitingExtractorDecorator<S, D> implements Extractor<S, D>, Decorator {

  private final Extractor<S, D> extractor;
  private final Limiter limiter;
  private final TaskState taskState;
  public static final String LIMITER_STOP_EVENT_NAME = "PrematureExtractorStop";
  public static final String LIMITER_STOP_CAUSE_KEY = "limiterStopCause";
  public static final String LIMITER_STOP_CAUSE_VALUE = "LimiterPermitAcquireFailure";
  private EventSubmitter eventSubmitter;
  public LimitingExtractorDecorator(Extractor<S, D> extractor, Limiter limiter, TaskState state) {
    this.extractor = extractor;
    this.limiter = limiter;
    this.taskState = state;
    this.limiter.start();
    this.eventSubmitter = new EventSubmitter.Builder(TaskMetrics.get(taskState).getMetricContext(), "gobblin.runtime.task").build();
  }

  @Override
  public Object getDecoratedObject() {
    return this.extractor;
  }

  @Override
  public S getSchema() throws IOException {
    return this.extractor.getSchema();
  }


  /**
   * Compose meta data when limiter fails to acquire permit
   * The meta data key list is passed from source layer
   * A prefix matching is used because some work unit {@link org.apache.gobblin.source.workunit.MultiWorkUnit} have packing strategy, which
   * can append additional string after the key name
   *
   * @return String map representing all the meta data need to report. Return null if no meta data was found.
   */
  private ImmutableMap<String, String> getLimiterStopMetadata() {
    WorkUnit workUnit = this.taskState.getWorkunit();
    Properties properties = workUnit.getProperties();

    String metadataKeyList = properties.getProperty(LimiterConfigurationKeys.LIMITER_REPORT_KEY_LIST, LimiterConfigurationKeys.DEFAULT_LIMITER_REPORT_KEY_LIST);
    List<String> keyList = Splitter.on(',').omitEmptyStrings().trimResults()
            .splitToList(metadataKeyList);

    if (keyList.isEmpty())
      return ImmutableMap.of();

      Set<String> names = properties.stringPropertyNames();
      TreeMap<String, String> orderedProperties = new TreeMap<>();

      for (String name : names) {
        orderedProperties.put(name, properties.getProperty(name));
      }

      ImmutableMap.Builder builder = ImmutableMap.<String, String>builder();
      for (String oldKey : keyList) {
        builder.putAll(orderedProperties.subMap(oldKey, oldKey + Character.MAX_VALUE));
      }
      builder.put(LIMITER_STOP_CAUSE_KEY, LIMITER_STOP_CAUSE_VALUE);
      return builder.build();
  }

  private void submitLimiterStopMetadataEvents (){
    ImmutableMap<String, String> metaData = this.getLimiterStopMetadata();
    if (!metaData.isEmpty()) {
      this.eventSubmitter.submit(LIMITER_STOP_EVENT_NAME, metaData);
    }
  }

  @Override
  public D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    try (Closer closer = Closer.create()) {
      if (closer.register(this.limiter.acquirePermits(1)) != null) {
        return this.extractor.readRecord(reuse);
      }
      submitLimiterStopMetadataEvents();
      return null;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while trying to acquire the next permit", ie);
    }
  }

  @Override
  public long getExpectedRecordCount() {
    return this.extractor.getExpectedRecordCount();
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return this.extractor.getHighWatermark();
  }

  @Override
  public void close() throws IOException {
    try {
      this.extractor.close();
    } finally {
      this.limiter.stop();
    }
  }
}
