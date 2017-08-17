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

package org.apache.gobblin.data.management.source;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.dataset.DatasetUtils;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.PartitionableDataset;
import org.apache.gobblin.source.WorkUnitStreamSource;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.util.HadoopUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * An abstract source that uses a {@link org.apache.gobblin.dataset.DatasetsFinder} to find {@link Dataset}s and creates a
 * work unit for each one.
 */
@Slf4j
public abstract class DatasetFinderSource<S, D> implements WorkUnitStreamSource<S, D> {

  protected final boolean drilldownIntoPartitions;

  /**
   * @param drilldownIntoPartitions if set to true, will process each partition of a {@link PartitionableDataset} as a
   *                                separate work unit.
   */
  public DatasetFinderSource(boolean drilldownIntoPartitions) {
    this.drilldownIntoPartitions = drilldownIntoPartitions;
  }

  /**
   * @return the {@link WorkUnit} for the input dataset.
   */
  protected abstract WorkUnit workUnitForDataset(Dataset dataset);

  /**
   * @return the {@link WorkUnit} for the input partition.
   */
  protected abstract WorkUnit workUnitForDatasetPartition(PartitionableDataset.DatasetPartition partition);

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    try {
      return createWorkUnitStream(state).collect(Collectors.toList());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public WorkUnitStream getWorkunitStream(SourceState state) {
    try {
      return new BasicWorkUnitStream.Builder(createWorkUnitStream(state).iterator()).build();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Can be overriden to specify a non-pluggable {@link org.apache.gobblin.dataset.DatasetsFinder}.
   * @throws IOException
   */
  protected IterableDatasetFinder createDatasetsFinder(SourceState state) throws IOException {
    return DatasetUtils.instantiateIterableDatasetFinder(state.getProperties(),
        HadoopUtils.getSourceFileSystem(state), null);
  }

  private Stream<WorkUnit> createWorkUnitStream(SourceState state) throws IOException {
    IterableDatasetFinder datasetsFinder = createDatasetsFinder(state);

    Stream<Dataset> datasetStream = datasetsFinder.getDatasetsStream(0, null);

    if (this.drilldownIntoPartitions) {
      return datasetStream.flatMap(dataset -> {
        if (dataset instanceof PartitionableDataset) {
          try {
            return (Stream<PartitionableDataset.DatasetPartition>) ((PartitionableDataset) dataset).getPartitions(0,
                null);
          } catch (IOException ioe) {
            log.error("Failed to get partitions for dataset " + dataset.getUrn());
            return Stream.empty();
          }
        } else {
          return Stream.of(new DatasetWrapper(dataset));
        }
      }).map(this::workUnitForPartitionInternal);
    } else {
      return datasetStream.map(this::workUnitForDataset);
    }
  }

  private WorkUnit workUnitForPartitionInternal(PartitionableDataset.DatasetPartition partition) {
    if (partition instanceof DatasetWrapper) {
      return workUnitForDataset(((DatasetWrapper) partition).dataset);
    } else {
      return workUnitForDatasetPartition(partition);
    }
  }

  /**
   * A wrapper around a {@link org.apache.gobblin.dataset.PartitionableDataset.DatasetPartition} that makes it look
   * like a {@link Dataset} for slightly easier to understand code.
   */
  @AllArgsConstructor
  protected static class DatasetWrapper implements PartitionableDataset.DatasetPartition {
    @Getter
    private final Dataset dataset;

    @Override
    public String getUrn() {
      return this.dataset.datasetURN();
    }
  }
}
