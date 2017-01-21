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

package gobblin.runtime;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.fork.ForkOperator;
import gobblin.instrumented.Instrumented;
import gobblin.instrumented.converter.InstrumentedConverterDecorator;
import gobblin.instrumented.fork.InstrumentedForkOperatorDecorator;
import gobblin.publisher.TaskPublisher;
import gobblin.publisher.TaskPublisherBuilderFactory;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.qualitychecker.row.RowLevelPolicyCheckerBuilderFactory;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckerBuilderFactory;
import gobblin.runtime.util.TaskMetrics;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.limiter.DefaultLimiterFactory;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.NonRefillableLimiter;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.Destination;
import gobblin.writer.WatermarkStorage;
import gobblin.writer.WriterOutputFormat;


/**
 * A class containing all necessary information to construct and run a {@link Task}.
 *
 * @author Yinan Li
 */
public class TaskContext {

  private final TaskState taskState;
  private final TaskMetrics taskMetrics;
  private Extractor rawSourceExtractor;

  public TaskContext(WorkUnitState workUnitState) {
    this.taskState = new TaskState(workUnitState);
    this.taskMetrics = TaskMetrics.get(this.taskState);
    this.taskState.setProp(Instrumented.METRIC_CONTEXT_NAME_KEY, this.taskMetrics.getName());
  }

  /**
   * Get a {@link TaskState} instance for the task.
   *
   * @return a {@link TaskState} instance
   */
  public TaskState getTaskState() {
    return this.taskState;
  }

  /**
   * Get a {@link TaskMetrics} instance for the task.
   *
   * @return a {@link TaskMetrics} instance
   */
  public TaskMetrics getTaskMetrics() {
    return this.taskMetrics;
  }

  /**
   * Get a {@link Source} instance used to get a list of {@link WorkUnit}s.
   *
   * @return the {@link Source} used to get the {@link WorkUnit}, <em>null</em>
   *         if it fails to instantiate a {@link Source} object of the given class.
   */
  public Source getSource() {
    try {
      return Source.class.cast(Class.forName(this.taskState.getProp(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance());
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException(ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }
  }

  /**
   * Get a {@link Extractor} instance.
   *
   * @return a {@link Extractor} instance
   */
  public Extractor getExtractor() {
    try {
      this.rawSourceExtractor = getSource().getExtractor(this.taskState);
      boolean throttlingEnabled = this.taskState.getPropAsBoolean(ConfigurationKeys.EXTRACT_LIMIT_ENABLED_KEY,
          ConfigurationKeys.DEFAULT_EXTRACT_LIMIT_ENABLED);
      if (throttlingEnabled) {
        Limiter limiter = DefaultLimiterFactory.newLimiter(this.taskState);
        if (!(limiter instanceof NonRefillableLimiter)) {
          throw new IllegalArgumentException("The Limiter used with an Extractor should be an instance of "
              + NonRefillableLimiter.class.getSimpleName());
        }
        return new LimitingExtractorDecorator<>(this.rawSourceExtractor, limiter);
      }
      return this.rawSourceExtractor;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }


  public Extractor getRawSourceExtractor() {
    return this.rawSourceExtractor;
  }



  /**
   * Get the interval for status reporting.
   *
   * @return interval for status reporting
   */
  public long getStatusReportingInterval() {
    return this.taskState.getPropAsLong(ConfigurationKeys.TASK_STATUS_REPORT_INTERVAL_IN_MS_KEY,
        ConfigurationKeys.DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS);
  }

  /**
   * Get the writer {@link Destination.DestinationType}.
   *
   * @param branches number of forked branches
   * @param index branch index
   * @return writer {@link Destination.DestinationType}
   */
  public Destination.DestinationType getDestinationType(int branches, int index) {
    return Destination.DestinationType.valueOf(this.taskState.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, branches, index),
        Destination.DestinationType.HDFS.name()));
  }

  /**
   * Get the output format of the writer of type {@link WriterOutputFormat}.
   *
   * @param branches number of forked branches
   * @param index branch index
   * @return output format of the writer
   */
  public WriterOutputFormat getWriterOutputFormat(int branches, int index) {
    String writerOutputFormatValue = this.taskState.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, branches, index),
        WriterOutputFormat.OTHER.name());

    return Enums.getIfPresent(WriterOutputFormat.class, writerOutputFormatValue.toUpperCase())
        .or(WriterOutputFormat.OTHER);
  }

  /**
   * Get the list of pre-fork {@link Converter}s.
   *
   * @return list (possibly empty) of {@link Converter}s
   */
  public List<Converter<?, ?, ?, ?>> getConverters() {
    return getConverters(-1, this.taskState);
  }

  /**
   * Get the list of post-fork {@link Converter}s for a given branch.
   *
   * @param index branch index
   * @param forkTaskState a {@link TaskState} instance specific to the fork identified by the branch index
   * @return list (possibly empty) of {@link Converter}s
   */
  @SuppressWarnings("unchecked")
  public List<Converter<?, ?, ?, ?>> getConverters(int index, TaskState forkTaskState) {
    String converterClassKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.CONVERTER_CLASSES_KEY, index);

    if (!this.taskState.contains(converterClassKey)) {
      return Collections.emptyList();
    }

    if (index >= 0) {
      forkTaskState.setProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, index);
    }

    List<Converter<?, ?, ?, ?>> converters = Lists.newArrayList();
    for (String converterClass : Splitter.on(",").omitEmptyStrings().trimResults()
        .split(this.taskState.getProp(converterClassKey))) {
      try {
        Converter<?, ?, ?, ?> converter = Converter.class.cast(Class.forName(converterClass).newInstance());
        InstrumentedConverterDecorator instrumentedConverter = new InstrumentedConverterDecorator<>(converter);
        instrumentedConverter.init(forkTaskState);
        converters.add(instrumentedConverter);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(cnfe);
      } catch (InstantiationException ie) {
        throw new RuntimeException(ie);
      } catch (IllegalAccessException iae) {
        throw new RuntimeException(iae);
      }
    }

    return converters;
  }

  /**
   * Get the {@link ForkOperator} to be applied to converted input schema and data record.
   *
   * @return {@link ForkOperator} to be used or <code>null</code> if none is specified
   */
  @SuppressWarnings("unchecked")
  public ForkOperator getForkOperator() {
    try {
      ForkOperator fork =
          ForkOperator.class.cast(Class.forName(this.taskState.getProp(ConfigurationKeys.FORK_OPERATOR_CLASS_KEY,
              ConfigurationKeys.DEFAULT_FORK_OPERATOR_CLASS)).newInstance());
      return new InstrumentedForkOperatorDecorator<>(fork);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException(ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }
  }

  /**
   * Get a pre-fork {@link RowLevelPolicyChecker} for executing row-level
   * {@link gobblin.qualitychecker.row.RowLevelPolicy}.
   *
   * @return a {@link RowLevelPolicyChecker}
   */
  public RowLevelPolicyChecker getRowLevelPolicyChecker() throws Exception {
    return getRowLevelPolicyChecker(-1);
  }

  /**
   * Get a post-fork {@link RowLevelPolicyChecker} for executing row-level
   * {@link gobblin.qualitychecker.row.RowLevelPolicy} in the given branch.
   *
   * @param index branch index
   * @return a {@link RowLevelPolicyChecker}
   */
  public RowLevelPolicyChecker getRowLevelPolicyChecker(int index) throws Exception {
    return RowLevelPolicyCheckerBuilderFactory.newPolicyCheckerBuilder(this.taskState, index).build();
  }

  /**
   * Get a post-fork {@link TaskLevelPolicyChecker} for executing task-level
   * {@link gobblin.qualitychecker.task.TaskLevelPolicy} in the given branch.
   *
   * @param taskState {@link TaskState} of a {@link Task}
   * @param index branch index
   * @return a {@link TaskLevelPolicyChecker}
   * @throws Exception
   */
  public TaskLevelPolicyChecker getTaskLevelPolicyChecker(TaskState taskState, int index) throws Exception {
    return TaskLevelPolicyCheckerBuilderFactory.newPolicyCheckerBuilder(taskState, index).build();
  }

  /**
   * Get a post-fork {@link TaskPublisher} for publishing data in the given branch.
   *
   * @param taskState {@link TaskState} of a {@link Task}
   * @param results Task-level policy checking results
   * @return a {@link TaskPublisher}
   */
  public TaskPublisher getTaskPublisher(TaskState taskState, TaskLevelPolicyCheckResults results) throws Exception {
    return TaskPublisherBuilderFactory.newTaskPublisherBuilder(taskState, results).build();
  }

  /**
   * Get a {@link DataWriterBuilder} for building a {@link gobblin.writer.DataWriter}.
   *
   * @param branches number of forked branches
   * @param index branch index
   * @return a {@link DataWriterBuilder}
   */
  public DataWriterBuilder getDataWriterBuilder(int branches, int index) {
    String dataWriterBuilderClassName = this.taskState.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUILDER_CLASS, branches, index),
        ConfigurationKeys.DEFAULT_WRITER_BUILDER_CLASS);
    try {
      return DataWriterBuilder.class.cast(Class.forName(dataWriterBuilderClassName).newInstance());
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException(ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }
  }

  public WatermarkStorage getWatermarkStorage() {
    return new StateStoreBasedWatermarkStorage(taskState);
  }
}
