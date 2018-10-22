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

package org.apache.gobblin.data.management.copy.splitter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.IdentityConverter;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter;
import org.apache.gobblin.data.management.copy.writer.FileAwareInputStreamDataWriterBuilder;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.guid.Guid;


/**
 * Helper class for splitting files for distcp. The property flag gobblin.copy.split.enabled should be used to enable
 * splitting of files (which is disabled by default). Splitting should only be used if the distcp job uses only the
 * IdentityConverter and should not be used for distcp jobs that require decryption/ungzipping.
 */
@Slf4j
public class DistcpFileSplitter {

  public static final String SPLIT_ENABLED = CopyConfiguration.COPY_PREFIX + ".split.enabled";
  public static final String MAX_SPLIT_SIZE_KEY = CopyConfiguration.COPY_PREFIX + ".file.max.split.size";

  public static final long DEFAULT_MAX_SPLIT_SIZE = Long.MAX_VALUE;
  public static final Set<String> KNOWN_SCHEMES_SUPPORTING_CONCAT = Sets.newHashSet("hdfs", "adl");

  /**
   * A split for a distcp file. Represents a section of a file; split should be aligned to block boundaries.
   */
  @Data
  public static class Split {
    private final long lowPosition;
    private final long highPosition;
    private final int splitNumber;
    private final int totalSplits;
    private final String partName;

    public final boolean isLastSplit() {
      return this.splitNumber == this.totalSplits - 1;
    }
  }

  private static final String SPLIT_KEY = CopyConfiguration.COPY_PREFIX + ".file.splitter.split";
  private static final Gson GSON = new Gson();

  /**
   * Split an input {@link CopyableFile} into multiple splits aligned with block boundaries.
   *
   * @param file {@link CopyableFile} to split.
   * @param workUnit {@link WorkUnit} generated for this file.
   * @param targetFs destination {@link FileSystem} where file is to be copied.
   * @return a list of {@link WorkUnit}, each for a split of this file.
   * @throws IOException
   */
  public static Collection<WorkUnit> splitFile(CopyableFile file, WorkUnit workUnit, FileSystem targetFs)
      throws IOException {
    long len = file.getFileStatus().getLen();
    // get lcm of source and target block size so that split aligns with block boundaries for both extract and write
    long blockSize = ArithmeticUtils.lcm(file.getFileStatus().getBlockSize(), file.getBlockSize(targetFs));
    long maxSplitSize = workUnit.getPropAsLong(MAX_SPLIT_SIZE_KEY, DEFAULT_MAX_SPLIT_SIZE);

    if (maxSplitSize < blockSize) {
      log.warn(String.format("Max split size must be at least block size. Adjusting to %d.", blockSize));
      maxSplitSize = blockSize;
    }
    if (len < maxSplitSize) {
      return Lists.newArrayList(workUnit);
    }

    Collection<WorkUnit> newWorkUnits = Lists.newArrayList();

    long lengthPerSplit = (maxSplitSize / blockSize) * blockSize;
    int splits = (int) (len / lengthPerSplit + 1);

    for (int i = 0; i < splits; i++) {
      WorkUnit newWorkUnit = WorkUnit.copyOf(workUnit);

      long lowPos = lengthPerSplit * i;
      long highPos = Math.min(lengthPerSplit * (i + 1), len);

      Split split = new Split(lowPos, highPos, i, splits,
          String.format("%s.__PART%d__", file.getDestination().getName(), i));
      String serializedSplit = GSON.toJson(split);

      newWorkUnit.setProp(SPLIT_KEY, serializedSplit);

      Guid oldGuid = CopySource.getWorkUnitGuid(newWorkUnit).get();
      Guid newGuid = oldGuid.append(Guid.fromStrings(serializedSplit));

      CopySource.setWorkUnitGuid(workUnit, newGuid);
      newWorkUnits.add(newWorkUnit);
    }
    return newWorkUnits;
  }

  /**
   * Finds all split work units in the input collection and merges the file parts into the expected output files.
   * @param fs {@link FileSystem} where file parts exist.
   * @param workUnits Collection of {@link WorkUnitState}s possibly containing split work units.
   * @return The collection of {@link WorkUnitState}s where split work units for each file have been merged.
   * @throws IOException
   */
  public static Collection<WorkUnitState> mergeAllSplitWorkUnits(FileSystem fs, Collection<WorkUnitState> workUnits)
      throws IOException {
    ListMultimap<CopyableFile, WorkUnitState> splitWorkUnitsMap = ArrayListMultimap.create();
    for (WorkUnitState workUnit : workUnits) {
      if (isSplitWorkUnit(workUnit)) {
        CopyableFile copyableFile = (CopyableFile) CopySource.deserializeCopyEntity(workUnit);
        splitWorkUnitsMap.put(copyableFile, workUnit);
      }
    }

    for (CopyableFile file : splitWorkUnitsMap.keySet()) {
      log.info(String.format("Merging split file %s.", file.getDestination()));

      WorkUnitState oldWorkUnit = splitWorkUnitsMap.get(file).get(0);
      Path outputDir = FileAwareInputStreamDataWriter.getOutputDir(oldWorkUnit);
      CopyEntity.DatasetAndPartition datasetAndPartition =
          file.getDatasetAndPartition(CopySource.deserializeCopyableDataset(oldWorkUnit));
      Path parentPath = FileAwareInputStreamDataWriter.getOutputFilePath(file, outputDir, datasetAndPartition)
          .getParent();

      WorkUnitState newWorkUnit = mergeSplits(fs, file, splitWorkUnitsMap.get(file), parentPath);

      for (WorkUnitState wu : splitWorkUnitsMap.get(file)) {
        // Set to committed so that task states will not fail
        wu.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        workUnits.remove(wu);
      }
      workUnits.add(newWorkUnit);
    }
    return workUnits;
  }

  /**
   * Merges all the splits for a given file.
   * Should be called on the target/destination file system (after blocks have been copied to targetFs).
   * @param fs {@link FileSystem} where file parts exist.
   * @param file {@link CopyableFile} to merge.
   * @param workUnits {@link WorkUnitState}s for all parts of this file.
   * @param parentPath {@link Path} where the parts of the file are located.
   * @return a {@link WorkUnit} equivalent to the distcp work unit if the file had not been split.
   * @throws IOException
   */
  private static WorkUnitState mergeSplits(FileSystem fs, CopyableFile file, Collection<WorkUnitState> workUnits,
      Path parentPath) throws IOException {

    log.info(String.format("File %s was written in %d parts. Merging.", file.getDestination(), workUnits.size()));
    Path[] parts = new Path[workUnits.size()];
    for (WorkUnitState workUnit : workUnits) {
      if (!isSplitWorkUnit(workUnit)) {
        throw new IOException("Not a split work unit.");
      }
      Split split = getSplit(workUnit).get();
      parts[split.getSplitNumber()] = new Path(parentPath, split.getPartName());
    }

    Path target = new Path(parentPath, file.getDestination().getName());

    fs.rename(parts[0], target);
    fs.concat(target, Arrays.copyOfRange(parts, 1, parts.length));

    WorkUnitState finalWorkUnit = workUnits.iterator().next();
    finalWorkUnit.removeProp(SPLIT_KEY);
    return finalWorkUnit;
  }

  /**
   * @return whether the {@link WorkUnit} is a split work unit.
   */
  public static boolean isSplitWorkUnit(State workUnit) {
    return workUnit.contains(SPLIT_KEY);
  }

  /**
   * @return the {@link Split} object contained in the {@link WorkUnit}.
   */
  public static Optional<Split> getSplit(State workUnit) {
    return workUnit.contains(SPLIT_KEY) ? Optional.of(GSON.fromJson(workUnit.getProp(SPLIT_KEY), Split.class))
        : Optional.<Split>absent();
  }

  /**
   * @param state {@link State} containing properties for a job.
   * @param targetFs destination {@link FileSystem} where file is to be copied
   * @return whether to allow for splitting of work units based on the filesystem, state, converter/writer config.
   */
  public static boolean allowSplit(State state, FileSystem targetFs) {
    // Don't allow distcp jobs that use decrypt/ungzip converters or tararchive/encrypt writers to split work units
    Collection<String> converterClassNames = Collections.emptyList();
    if (state.contains(ConfigurationKeys.CONVERTER_CLASSES_KEY)) {
      converterClassNames = state.getPropAsList(ConfigurationKeys.CONVERTER_CLASSES_KEY);
    }

    return state.getPropAsBoolean(SPLIT_ENABLED, false) &&
        KNOWN_SCHEMES_SUPPORTING_CONCAT.contains(targetFs.getUri().getScheme()) &&
        state.getProp(ConfigurationKeys.WRITER_BUILDER_CLASS, "")
            .equals(FileAwareInputStreamDataWriterBuilder.class.getName()) &&
        converterClassNames.stream().noneMatch(s -> !s.equals(IdentityConverter.class.getName()));
  }

}
