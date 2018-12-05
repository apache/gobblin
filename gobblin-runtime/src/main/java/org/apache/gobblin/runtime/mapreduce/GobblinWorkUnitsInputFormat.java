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

package org.apache.gobblin.runtime.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.SerializationUtils;
import org.apache.gobblin.util.binpacking.WorstFitDecreasingBinPacking;


/**
 * An input format for reading Gobblin inputs (work unit and multi work unit files).
 */
@Slf4j
public class GobblinWorkUnitsInputFormat extends InputFormat<LongWritable, Text> {

  private static final String MAX_MAPPERS = GobblinWorkUnitsInputFormat.class.getName() + ".maxMappers";

  public static final String WORK_UNIT_WEIGHT = "gobblin.workunit.weight";
  private static final String GOBBLIN_SPLIT_PREFIX = "gobblin.inputsplit";
  public static final String GOBBLIN_SPLIT_FILE_PATH = GOBBLIN_SPLIT_PREFIX + ".source.file.path";
  public static final String GOBBLIN_SPLIT_FILE_LOW_POSITION = GOBBLIN_SPLIT_PREFIX + ".source.file.low.position";
  public static final String GOBBLIN_SPLIT_FILE_HIGH_POSITION = GOBBLIN_SPLIT_PREFIX + ".source.file.high.position";

  private static final double LOCALITY_THRESHOLD = 0.8;

  Set<String> workUnitPaths = Sets.newHashSet();
  Map<String, Long> workUnitLengths = Maps.newHashMap();
  Map<String, Map<String, Long>> workUnitBlkLocationsMap = Maps.newHashMap();

  /**
   * Set max mappers used in MR job.
   */
  public static void setMaxMappers(Job job, int maxMappers) {
    job.getConfiguration().setInt(MAX_MAPPERS, maxMappers);
  }

  public static int getMaxMapper(Configuration conf) {
    return conf.getInt(MAX_MAPPERS, Integer.MAX_VALUE);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {

    Path[] inputPaths = FileInputFormat.getInputPaths(context);
    if (inputPaths == null || inputPaths.length == 0) {
      throw new IOException("No input found!");
    }

    for (Path path : inputPaths) {
      // path is a single work unit / multi work unit
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      FileStatus[] inputs = fs.listStatus(path);

      if (inputs == null) {
        throw new IOException(String.format("Path %s does not exist.", path));
      }
      log.info(String.format("Found %d input files at %s: %s", inputs.length, path, Arrays.toString(inputs)));

      for (FileStatus input : inputs) {
        addWorkUnitPathInfo(input.getPath(), fs);
      }
    }

    int maxMappers = getMaxMapper(context.getConfiguration());
    int numTasksPerMapper = workUnitPaths.size() / maxMappers + (workUnitPaths.size() % maxMappers == 0 ? 0 : 1);

    List<InputSplit> splits = Lists.newArrayList();
    createSplits(splits, numTasksPerMapper);
    clearWorkUnitPathsInfo();
    return splits;

  }

  protected void clearWorkUnitPathsInfo() {
    workUnitPaths.clear();
    workUnitLengths.clear();
    workUnitBlkLocationsMap.clear();
  }

  protected void addWorkUnitPathInfo(Path workUnitPath, FileSystem fs) throws IOException {
    WorkUnit workUnit = deserializeWorkUnitWrapper(workUnitPath, fs);
    workUnitPaths.add(workUnitPath.toString());
    workUnitLengths.put(workUnitPath.toString(), getBlockHostsAndLengths(fs, workUnit, workUnitPath));
  }

  @VisibleForTesting
  WorkUnit deserializeWorkUnitWrapper(Path workUnitPath, FileSystem fs) throws IOException {
    if (workUnitPath == null || fs == null) {
      return null;
    }
    WorkUnit tmpWorkUnit = (workUnitPath.toString().endsWith(AbstractJobLauncher.MULTI_WORK_UNIT_FILE_EXTENSION) ?
        MultiWorkUnit.createEmpty() : WorkUnit.createEmpty());
    SerializationUtils.deserializeState(fs, workUnitPath, tmpWorkUnit);
    return tmpWorkUnit;
  }

  /**
   * Hook to actually create the splits based on the work unit path info we've populated for this instance.
   * This implementation iterates through the work unit paths map using a limited iterator to group work units
   * into groups with numTasksPerMapper number of work units. For each group, the top node locations are chosen
   * based on the amount of data on the node, to be used to create the split.
   * @param splits List of splits to add newly created splits to
   * @param numTasksPerMapper
   */
  protected void createSplits(List<InputSplit> splits, int numTasksPerMapper) {
    Iterator<String> workUnitPathsIt = workUnitPaths.iterator();
    while (workUnitPathsIt.hasNext()) {
      Iterator<String> limitedIterator = Iterators.limit(workUnitPathsIt, numTasksPerMapper);

      List<String> splitPaths = Lists.newArrayList();
      Long splitLength = 0L;
      Map<String, Long> splitBlkLocationNamesAndLengths = Maps.newHashMap();

      while (limitedIterator.hasNext()) {
        String workUnitPath = limitedIterator.next();
        splitPaths.add(workUnitPath);
        splitLength += workUnitLengths.get(workUnitPath);

        for (Map.Entry<String, Long> entry : workUnitBlkLocationsMap.get(workUnitPath).entrySet()) {
          splitBlkLocationNamesAndLengths.put(entry.getKey(),
              splitBlkLocationNamesAndLengths.getOrDefault(entry.getKey(), 0L) + entry.getValue());
        }
      }

      // Sort list of block location names for split, then choose top locations to use for split, based on amount of
      // relevant data in the node
      List<Map.Entry<String, Long>> splitBlkLocationNamesOrderedByLength =
          Lists.newArrayList(splitBlkLocationNamesAndLengths.entrySet());
      splitBlkLocationNamesOrderedByLength.sort((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()));

      int endIdx = 0;
      while (endIdx < splitBlkLocationNamesOrderedByLength.size() &&
          (double) splitBlkLocationNamesOrderedByLength.get(endIdx).getValue() / splitLength >= LOCALITY_THRESHOLD) {
        endIdx += 1;
      }

      // Should add at least one node name, if available, even if no nodes contain amount greater than threshold
      if (endIdx == 0 && splitBlkLocationNamesOrderedByLength.size() > 0) {
        endIdx = 1;
        long maxLen = splitBlkLocationNamesOrderedByLength.get(0).getValue();
        while (endIdx < splitBlkLocationNamesOrderedByLength.size() &&
            splitBlkLocationNamesOrderedByLength.get(endIdx).getValue() == maxLen) {
          endIdx += 1;
        }
      }

      splits.add(new GobblinSplit(splitPaths, splitLength, splitBlkLocationNamesOrderedByLength.subList(0, endIdx)
          .stream().map(Map.Entry::getKey).toArray(String[]::new)));
    }
  }

  private static List<WorkUnit> getFlattenedWorkUnitList(WorkUnit workUnit) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    if (workUnit != null) {
      if (workUnit instanceof MultiWorkUnit) {
        workUnits.addAll(((MultiWorkUnit) workUnit).getWorkUnits());
      } else {
        workUnits.add(workUnit);
      }
    }
    return workUnits;
  }

  /**
   * Get all the node location names for the data represented by the work unit and the amount of the data at each
   * node location, and update the workUnitBlkLocationsMap with this information.
   * @param fs
   * @param workUnit
   * @param workUnitPath
   * @return total length of the data represented by the work unit.
   * @throws IOException
   */
  private long getBlockHostsAndLengths(FileSystem fs, WorkUnit workUnit, Path workUnitPath) throws IOException {
    long totalWorkUnitLength = 0L;
    Map<String, Long> lengthsForBlkLocations = Maps.newHashMap();

    for (WorkUnit singleWorkUnit : getFlattenedWorkUnitList(workUnit)) {
      if (!singleWorkUnit.contains(GOBBLIN_SPLIT_FILE_PATH)) {
        totalWorkUnitLength += singleWorkUnit.getPropAsLong(WORK_UNIT_WEIGHT, 0L);
        continue;
      }

      Path filePath = new Path(singleWorkUnit.getProp(GOBBLIN_SPLIT_FILE_PATH));
      long splitOffset = singleWorkUnit.getPropAsLong(GOBBLIN_SPLIT_FILE_LOW_POSITION, 0);
      long splitLength = (singleWorkUnit.contains(GOBBLIN_SPLIT_FILE_HIGH_POSITION) ?
          singleWorkUnit.getPropAsLong(GOBBLIN_SPLIT_FILE_HIGH_POSITION) :
          fs.getFileStatus(filePath).getLen()) - splitOffset;
      totalWorkUnitLength += splitLength;

      parseToBlkLocationLengthsMap(fs.getFileBlockLocations(filePath, splitOffset, splitLength),
          splitOffset, splitLength, lengthsForBlkLocations);
    }

    workUnitBlkLocationsMap.put(workUnitPath.toString(), lengthsForBlkLocations);
    return workUnit.getPropAsLong(WorstFitDecreasingBinPacking.TOTAL_MULTI_WORK_UNIT_WEIGHT, totalWorkUnitLength);
  }

  void parseToBlkLocationLengthsMap(BlockLocation[] blkLocations, long initialOffset, long totalLength,
      Map<String, Long> lengthsForBlkLocations) throws IOException {
    if (blkLocations != null) {
      for (int i = 0; i < blkLocations.length; ++i) {
        long bytesInThisBlock = blkLocations[i].getLength();
        if (i == 0) {
          bytesInThisBlock += blkLocations[i].getOffset() - initialOffset;
        } else if (i == blkLocations.length - 1) {
          bytesInThisBlock = initialOffset + totalLength - blkLocations[i].getOffset();
        }

        for (String blkLocationName : blkLocations[i].getHosts()) {
          lengthsForBlkLocations.put(blkLocationName,
              lengthsForBlkLocations.getOrDefault(blkLocationName, 0L) + bytesInThisBlock);
        }
      }
    }
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new GobblinRecordReader((GobblinSplit) split);
  }

  /**
   * {@link InputSplit} that just contain the work unit / multi work unit files that each mapper should process.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Builder
  @EqualsAndHashCode
  public static class GobblinSplit extends InputSplit implements Writable {

    /**
     * A list of {@link Path}s containing work unit / multi work unit.
     */
    @Getter
    @Singular
    private List<String> paths;

    /**
     * Total length of the data to be processed represented by the work units
     */
    private long totalLength;

    /**
     * Locations (names of nodes) of blocks of files specified in the split's work unit files defined by paths
     */
    private String[] blockLocationNames;

    @Override
    public void write(DataOutput out)
        throws IOException {
      out.writeLong(this.totalLength);
      out.writeInt(this.paths.size());
      for (String path : this.paths) {
        out.writeUTF(path);
      }
      out.writeInt(this.blockLocationNames == null ? 0 : this.blockLocationNames.length);
      if (this.blockLocationNames != null) {
        for (String blockLocationName : this.blockLocationNames) {
          out.writeUTF(blockLocationName);
        }
      }
    }

    @Override
    public void readFields(DataInput in)
        throws IOException {
      this.totalLength = in.readLong();
      int numPaths = in.readInt();
      this.paths = Lists.newArrayListWithExpectedSize(numPaths);
      for (int i = 0; i < numPaths; i++) {
        this.paths.add(in.readUTF());
      }
      int numBlockLocationNames = in.readInt();
      this.blockLocationNames = new String[numBlockLocationNames];
      for (int i = 0; i < numBlockLocationNames; ++i) {
        this.blockLocationNames[i] = in.readUTF();
      }
    }

    @Override
    public long getLength()
        throws IOException, InterruptedException {
      return totalLength;
    }

    @Override
    public String[] getLocations()
        throws IOException, InterruptedException {
      if (blockLocationNames == null) {
        return new String[0];
      } else {
        return blockLocationNames;
      }
    }
  }

  /**
   * Returns records containing the name of the work unit / multi work unit files to process.
   */
  public static class GobblinRecordReader extends RecordReader<LongWritable, Text> {
    private int currentIdx = -1;
    private final List<String> paths;
    private final int totalPaths;

    public GobblinRecordReader(GobblinSplit split) {
      this.paths = split.getPaths();
      this.totalPaths = this.paths.size();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      this.currentIdx++;
      return this.currentIdx < this.totalPaths;
    }

    @Override
    public LongWritable getCurrentKey()
        throws IOException, InterruptedException {
      return new LongWritable(this.currentIdx);
    }

    @Override
    public Text getCurrentValue()
        throws IOException, InterruptedException {
      return new Text(this.paths.get(this.currentIdx));
    }

    @Override
    public float getProgress()
        throws IOException, InterruptedException {
      return (float) this.currentIdx / (float) this.totalPaths;
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
